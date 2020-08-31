import pytest

import sys
from textwrap import dedent

from bionic.exception import CodeVersioningError, UndefinedEntityError


@pytest.fixture
def tmp_module_path(tmp_path):
    tmp_dir_name = str(tmp_path)
    sys.path.insert(0, tmp_dir_name)

    yield tmp_path

    # Ensure that the temporary module is not loaded by a future test.
    sys.path.remove(tmp_dir_name)


@pytest.fixture
def write_module(tmp_module_path):
    # Pytest rewrites modules and caches the compiled pyc file. The cache can
    # interfere with the tests below. Hence, we disable the cache here.
    dont_write_bytecode = sys.dont_write_bytecode
    sys.dont_write_bytecode = True

    modules = set()

    def _write_module(name, contents):
        (tmp_module_path / f"{name}.py").write_text(dedent(contents))
        modules.add(name)

    yield _write_module

    # Remove the written modules from sys.modules cache so that they will not be
    # accidentally used in subsequent tests.
    for m in modules:
        sys.modules.pop(m, None)

    sys.dont_write_bytecode = dont_write_bytecode


@pytest.fixture
def write_flow_module(write_module):
    def _write_flow_module(x):
        write_module(
            "flow_module",
            f"""
                import bionic as bn

                builder = bn.FlowBuilder('flow')
                builder.assign('x', {x})

                @builder
                def x_doubled(x):
                    return x*2

                flow = builder.build()
            """,
        )

    return _write_flow_module


@pytest.fixture(params=["reload", "reloading"])
def reload_method(request):
    return request.param


class FlowTester:
    """
    A helper class for testing dynamically written flows.

    If the reload method is reloading(), whenever the method is called, we get a
    new copy of the flow with reloaded modules. We need to verify that the
    entities of the original flow instance give the same values when computed as
    before the reload.
    """

    def __init__(self, reload_method):
        from flow_module import flow

        self.flow = flow
        self.expected_results_by_flow = {}
        self.reload_method = reload_method

    def expect_values(self, **kwargs):
        expected = dict(kwargs)
        self.expected_results_by_flow[self.flow] = expected
        for f, r in self.expected_results_by_flow.items():
            self._verify_flow_contents(f, r)

    def reload_flow(self):
        if self.reload_method == "reload":
            self.flow.reload()
        else:
            self.flow = self.flow.reloading()

    def get(self, name):
        return self.flow.get(name)

    @classmethod
    def _verify_flow_contents(cls, flow, results_map):
        assert {k: flow.get(k) for k in results_map.keys()} == results_map
        assert {k: getattr(flow.get, k)() for k in results_map.keys()} == results_map


def test_no_change(write_flow_module, reload_method):
    write_flow_module(1)
    tester = FlowTester(reload_method)
    tester.expect_values(x=1, x_doubled=2)

    tester.reload_flow()
    tester.expect_values(x=1, x_doubled=2)


def test_change_entity_value(write_flow_module, reload_method):
    write_flow_module(1)
    tester = FlowTester(reload_method)
    tester.expect_values(x=1, x_doubled=2)

    write_flow_module(2)
    tester.reload_flow()
    tester.expect_values(x=2, x_doubled=4)


def test_change_entity_name(write_module, reload_method):
    def write_flow_module(z: int):
        write_module(
            "flow_module",
            f"""
                import bionic as bn

                builder = bn.FlowBuilder('flow')
                builder.assign('x', 1)

                @builder
                def x_plus_{z}(x):
                    return x + {z}

                flow = builder.build()
            """,
        )

    write_flow_module(1)
    tester = FlowTester(reload_method)
    tester.expect_values(x=1, x_plus_1=2)

    write_flow_module(2)
    tester.reload_flow()
    tester.expect_values(x=1, x_plus_2=3)
    with pytest.raises(UndefinedEntityError):
        tester.get("x_plus_1")


def test_change_version(write_module, reload_method):
    def write_flow_module(x: int, y: int):
        write_module(
            "flow_module",
            f"""
                import bionic as bn

                builder = bn.FlowBuilder('flow')

                @builder
                @bn.version(major={x}, minor={y})
                def total():
                    return {x} + {y}

                flow = builder.build()
            """,
        )

    write_flow_module(10, 1)
    tester = FlowTester(reload_method)
    tester.expect_values(total=11)

    # Major version remains unchanged, cached value is returned
    write_flow_module(10, 2)
    tester.reload_flow()
    tester.expect_values(total=11)

    # Major version changed, a new result is computed
    write_flow_module(20, 2)
    tester.reload_flow()
    tester.expect_values(total=22)


def test_assist_versioning_mode(write_module, reload_method):
    def write_flow_module(x: int):
        write_module(
            "flow_module",
            f"""
                import bionic as bn

                builder = bn.FlowBuilder('flow')
                builder.set('core__versioning_mode', 'assist')

                @builder
                def x():
                    return {x}

                @builder
                def one():
                    return 1

                flow = builder.build()
            """,
        )

    write_flow_module(1)
    tester = FlowTester(reload_method)
    tester.expect_values(one=1, x=1)

    write_flow_module(1)
    tester.reload_flow()
    tester.expect_values(one=1, x=1)

    write_flow_module(2)
    tester.reload_flow()
    tester.expect_values(one=1)
    with pytest.raises(CodeVersioningError):
        tester.get("x")


# Test nondeterministic function marked with the changes_per_run decorator
def test_changes_per_run_decorator(write_module, reload_method):
    def write_flow_module():
        write_module(
            "flow_module",
            """
                import bionic as bn
                from random import random

                builder = bn.FlowBuilder('flow')
                builder.assign('x', 1)

                @builder
                @bn.changes_per_run
                def r():
                    return random()

                flow = builder.build()
            """,
        )

    write_flow_module()
    tester = FlowTester(reload_method)
    tester.expect_values(x=1)
    r = tester.get("r")

    write_flow_module()
    tester.reload_flow()
    tester.expect_values(x=1)
    assert tester.get("r") != r
