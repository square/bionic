import pytest

import sys
import importlib
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


# Test the scenario where we have a chain of flow merges.
def test_change_chain_merged_flow(write_module, flow_tester):
    def write_flow_x_module(x: int):
        write_module(
            "flow_x_module",
            f"""
                import bionic as bn

                builder = bn.FlowBuilder('flow_x')

                @builder
                def x():
                    return {x}

                flow_x = builder.build()
            """,
        )

    def write_flow_y_module(y: int):
        write_module(
            "flow_y_module",
            f"""
                import bionic as bn
                from flow_x_module import flow_x

                builder = bn.FlowBuilder('flow_y')
                builder.merge(flow_x)

                @builder
                def y():
                    return {y}

                flow_y = builder.build()
            """,
        )

    def write_flow_z_module(z: int):
        write_module(
            "flow_z_module",
            f"""
                import bionic as bn
                from flow_y_module import flow_y

                builder = bn.FlowBuilder('flow_z')
                builder.merge(flow_y)

                @builder
                def z():
                    return {z}

                flow_z = builder.build()
        """,
        )

    write_flow_x_module(1)
    write_flow_y_module(10)
    write_flow_z_module(100)
    flow_tester.write_flow_module(
        """
            from flow_z_module import flow_z

            builder = bn.FlowBuilder('flow')
            builder.set('core__versioning_mode', 'auto')
            builder.merge(flow_z)

            @builder
            def total(x, y, z):
                return x + y + z
        """,
    )
    flow_tester.expect_values(x=1, y=10, z=100, total=111)

    write_flow_x_module(2)
    flow_tester.reload_flow()
    flow_tester.expect_values(x=2, y=10, z=100, total=112)

    write_flow_y_module(20)
    flow_tester.reload_flow()
    flow_tester.expect_values(x=2, y=20, z=100, total=122)

    write_flow_z_module(200)
    flow_tester.reload_flow()
    flow_tester.expect_values(x=2, y=20, z=200, total=222)

    write_flow_x_module(3)
    write_flow_y_module(30)
    write_flow_z_module(300)
    flow_tester.reload_flow()
    flow_tester.expect_values(x=3, y=30, z=300, total=333)


# Test the scenario where we have one flow merging another, but both flows have
# one entity with the same name. We test the merged flow with one keep
# parameter. Then, we change the keep parameter and re-test the merged flow
# after reloading.
def test_merge_change_keep_parameter(write_module, flow_tester):
    write_module(
        "base_flow_module",
        """
            import bionic as bn

            builder = bn.FlowBuilder('base_flow')

            @builder
            def x():
                return 1

            @builder
            def x_plus_1(x):
                return x + 1

            @builder
            def total(x):
                return x

            base_flow = builder.build()
        """,
    )

    def write_flow_module(keep):
        flow_tester.write_flow_module(
            f"""
                from base_flow_module import base_flow

                builder = bn.FlowBuilder('flow')
                builder.set('core__versioning_mode', 'auto')

                @builder
                def x():
                    return 2

                @builder
                def y():
                    return 20

                @builder
                def total(x, y):
                    return x + y

                builder.merge(base_flow, keep='{keep}')
            """,
        )

    write_flow_module("new")
    flow_tester.expect_values(x=1, x_plus_1=2, y=20, total=1)

    write_flow_module("old")
    flow_tester.reload_flow()
    flow_tester.expect_values(x=2, x_plus_1=3, y=20, total=22)

    write_flow_module("new")
    flow_tester.reload_flow()
    flow_tester.expect_values(x=1, x_plus_1=2, y=20, total=1)


def test_change_import_builder(write_module, flow_tester):
    def write_builder_module(x: int):
        write_module(
            "base_builder_module",
            f"""
                import bionic as bn

                builder = bn.FlowBuilder('flow')

                @builder
                def x():
                    return {x}

                @builder
                def x_plus_1(x):
                    return x + 1

                @builder
                def total(x):
                    return x
            """,
        )

    write_builder_module(1)
    flow_tester.write_flow_module(
        """
            from base_builder_module import builder

            builder.set('core__versioning_mode', 'auto')

            @builder
            def y():
                return 10

            @builder
            def total(x, y):
                return x + y
        """,
    )
    flow_tester.expect_values(x=1, x_plus_1=2, y=10, total=11)

    write_builder_module(2)
    flow_tester.reload_flow()
    flow_tester.expect_values(x=2, x_plus_1=3, y=10, total=12)

    write_builder_module(3)
    flow_tester.reload_flow()
    flow_tester.expect_values(x=3, x_plus_1=4, y=10, total=13)


def test_internal_modules_not_reloaded(write_module, flow_tester):
    write_module("test_module", "")
    flow_tester.write_flow_module(
        """
            import sys
            import pandas
            import numpy
            import test_module

            @builder
            def x():
                return 1
        """
    )

    flow_tester.expect_values(x=1)

    reloaded_module_names = set()
    original_reload = importlib.reload

    def tracking_reload(module):
        reloaded_module_names.add(module.__name__)
        return original_reload(module)

    try:
        importlib.reload = tracking_reload
        flow_tester.reload_flow()
    finally:
        importlib.reload = original_reload

    # Module 'sys' is internal and does not get reloaded.
    # Module 'bionic' is reloaded since the module running in tests is not
    # installed through pip.
    assert reloaded_module_names == {"flow_module", "bionic", "test_module"}
