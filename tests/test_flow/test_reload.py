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


@pytest.fixture(params=["reload", "reloading"])
def reload_method_name(request):
    return request.param


class FlowTester:
    """
    A helper class for testing dynamically written flows.

    If the reload method is reloading(), whenever the method is called, we get a
    new copy of the flow with reloaded modules. We need to verify that the
    entities of the original flow instance give the same values when computed as
    before the reload.
    """

    def __init__(self, reload_method_name, tmp_path, write_module):
        self.flow = None
        self.expected_results_by_flow = {}
        self.reload_method_name = reload_method_name
        self.cache_dir = str(tmp_path / "BNTESTDATA")
        self.write_module = write_module

    def write_flow_module(self, body):
        header = f"""
            import bionic as bn

            builder = bn.FlowBuilder('flow')
            builder.set('core__persistent_cache__flow_dir', '{self.cache_dir}')
        """
        footer = """
            flow = builder.build()
        """
        self.write_module(
            "flow_module",
            dedent(header) + "\n" + dedent(body) + "\n" + dedent(footer),
        )

        if self.flow is None:
            from flow_module import flow

            self.flow = flow

    def expect_values(self, **kwargs):
        expected = dict(kwargs)
        self.expected_results_by_flow[self.flow] = expected
        for f, r in self.expected_results_by_flow.items():
            self._verify_flow_contents(f, r)

    def reload_flow(self):
        self.flow = getattr(self.flow, self.reload_method_name)()

    def get(self, name):
        return self.flow.get(name)

    @classmethod
    def _verify_flow_contents(cls, flow, results_map):
        assert {k: flow.get(k) for k in results_map.keys()} == results_map
        assert {k: getattr(flow.get, k)() for k in results_map.keys()} == results_map


@pytest.fixture
def flow_tester(reload_method_name, tmp_path, write_module):
    return FlowTester(reload_method_name, tmp_path, write_module)


def test_no_change(flow_tester):
    flow_tester.write_flow_module(
        """
            builder.assign('x', 1)

            @builder
            def x_doubled(x):
                return x * 2
        """
    )

    flow_tester.expect_values(x=1, x_doubled=2)

    flow_tester.reload_flow()
    flow_tester.expect_values(x=1, x_doubled=2)


def test_change_assignment(flow_tester):
    flow_tester.write_flow_module(
        """
            builder.assign('x', 1)

            @builder
            def x_doubled(x):
                return x * 2
        """
    )
    flow_tester.expect_values(x=1, x_doubled=2)

    flow_tester.write_flow_module(
        """
            builder.assign('x', 2)

            @builder
            def x_doubled(x):
                return x * 2
        """
    )
    flow_tester.reload_flow()
    flow_tester.expect_values(x=2, x_doubled=4)


def test_change_entity_name(flow_tester):
    def write_flow_module(z: int):
        flow_tester.write_flow_module(
            f"""
                builder.assign('x', 1)

                @builder
                def x_plus_{z}(x):
                    return x + {z}
            """,
        )

    write_flow_module(1)
    flow_tester.expect_values(x=1, x_plus_1=2)

    write_flow_module(2)
    flow_tester.reload_flow()
    flow_tester.expect_values(x=1, x_plus_2=3)
    with pytest.raises(UndefinedEntityError):
        flow_tester.get("x_plus_1")


def test_change_version(flow_tester):
    def write_flow_module(x: int, y: int):
        flow_tester.write_flow_module(
            f"""
                @builder
                @bn.version(major={x}, minor={y})
                def total():
                    return {x} + {y}
            """,
        )

    write_flow_module(10, 1)
    flow_tester.expect_values(total=11)

    # Major version remains unchanged, cached value is returned
    write_flow_module(10, 2)
    flow_tester.reload_flow()
    flow_tester.expect_values(total=11)

    # Major version changed, a new result is computed
    write_flow_module(20, 2)
    flow_tester.reload_flow()
    flow_tester.expect_values(total=22)


def test_assist_versioning_mode(flow_tester):
    def write_flow_module(x: int):
        flow_tester.write_flow_module(
            f"""
                builder.set('core__versioning_mode', 'assist')

                @builder
                def one():
                    return 1

                @builder
                def x():
                    return {x}
            """,
        )

    write_flow_module(1)
    flow_tester.expect_values(one=1, x=1)

    write_flow_module(1)
    flow_tester.reload_flow()
    flow_tester.expect_values(one=1, x=1)

    write_flow_module(2)
    flow_tester.reload_flow()
    flow_tester.expect_values(one=1)
    with pytest.raises(CodeVersioningError):
        flow_tester.get("x")


def test_auto_versioning_mode(flow_tester):
    def write_flow_module(x: int):
        flow_tester.write_flow_module(
            f"""
                builder.set('core__versioning_mode', 'auto')

                @builder
                def one():
                    return 1

                @builder
                def x():
                    return {x}

                @builder
                def x_doubled(x):
                    return x * 2
            """,
        )

    write_flow_module(1)
    flow_tester.expect_values(one=1, x=1, x_doubled=2)

    write_flow_module(2)
    flow_tester.reload_flow()
    flow_tester.expect_values(one=1, x=2, x_doubled=4)

    write_flow_module(3)
    flow_tester.reload_flow()
    flow_tester.expect_values(one=1, x=3, x_doubled=6)


# Test nondeterministic function marked with the changes_per_run decorator
def test_changes_per_run_decorator(flow_tester):
    def write_flow_module():
        flow_tester.write_flow_module(
            """
                from random import random

                builder.assign('x', 1)

                @builder
                @bn.changes_per_run
                def r():
                    return random()
            """,
        )

    write_flow_module()
    flow_tester.expect_values(x=1)
    r = flow_tester.get("r")

    write_flow_module()
    flow_tester.reload_flow()
    flow_tester.expect_values(x=1)
    assert flow_tester.get("r") != r
