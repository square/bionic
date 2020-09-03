import pytest

from textwrap import dedent

import attr
import numpy as np
import re

from bionic.descriptors import ast
from bionic.descriptors.parsing import dnode_from_descriptor
from bionic.utils.misc import single_element, single_unique_element
import bionic as bn


class ModelFlowHarness:
    """
    Manages a Bionic flow and tests its behavior against a "model" implementation.

    This class wraps both a Bionic flow and a simplified model implementation of the
    same flow. The flows are kept in sync, allowing their behavior to be checked against
    each other. This allows large flows to be constructed and validated
    programmatically.

    The model flow implementation supports the following functionality:

    - creating string-valued entities derived from a variable number of other entities
    - making functional changes to the definition of any entity
    - testing the value of an entity and the set of (user-provided) functions computed
      in the process
    - persisted and non-persisted entities
    - functions with multiple entity outputs

    This class is similar to the ``SimpleFlowModel`` ("SFM") in ``test_persisted_fuzz``,
    but with the following differences:

    - SFM does not support non-persisted entities or multiple entity outputs
    - this class does not support the following:
      - non-deterministic (``changes_per_run``) entities
      - non-functional updates
      - alternative versioning modes
      - cloud caching

    This class has a more sophisticated implementation than SFM, so it should be
    easier to port SFM's features to this class than vice versa. If we can port all of
    the above features, we can remove SFM altogether.
    """

    def __init__(self, builder, make_list):
        self._builder = builder

        self._entities_by_name = {}

        self._descriptors_computed_by_flow = make_list()
        self._descriptors_computed_by_model = make_list()

    def get_all_entity_names(self):
        return list(self._entities_by_name.keys())

    def create_entity(self, should_persist):
        name = f"e{len(self._entities_by_name) + 1}"
        entity = ModelEntity(name=name, should_persist=should_persist)
        assert name not in self._entities_by_name
        self._entities_by_name[name] = entity
        return name

    def add_binding(self, out_descriptor, dep_entity_names, use_tuples_for_output):
        out_dnode = dnode_from_descriptor(out_descriptor)
        out_entities = list(
            map(self._entities_by_name.get, out_dnode.all_entity_names())
        )

        should_persists = set(entity.should_persist for entity in out_entities)
        (should_persist,) = should_persists

        binding = ModelBinding(
            out_dnode=out_dnode,
            out_entities=out_entities,
            dep_entity_names=dep_entity_names,
            use_tuples_for_output=use_tuples_for_output,
        )
        for out_entity in out_entities:
            assert out_entity.binding is None
            out_entity.binding = binding
            for dep_entity_name in dep_entity_names:
                dep_entity = self._entities_by_name[dep_entity_name]
                dep_entity.dependent_entities.append(out_entity)

        self._add_binding_to_flow(binding)

    def update_binding(self, entity_name):
        entity = self._entities_by_name[entity_name]
        entity.binding.update_func_version()

        self._add_binding_to_flow(entity.binding)

    def query_and_check_entity(self, entity_name):
        flow = self._builder.build()
        flow_value = flow.get(entity_name)

        model_value = self._compute_model_value(entity_name)

        assert flow_value == model_value
        assert set(self._descriptors_computed_by_flow) == set(
            self._descriptors_computed_by_model
        )

        self._clear_called_descriptors()

    def _add_binding_to_flow(self, binding):
        out_entity_names = [entity.name for entity in binding.out_entities]

        joined_dep_entity_names = ", ".join(binding.dep_entity_names)

        # TODO Could this logic live inside ModelBinding instead?
        if binding.use_tuples_for_output:
            compute_func = binding.compute_value_for_returns
            output_decorator_fragment = f"""
            @bn.returns({binding.out_descriptor!r})
            """.strip()
        else:
            compute_func = binding.compute_value_for_outputs
            output_decorator_fragment = f"""
            @bn.outputs({', '.join(repr(name) for name in out_entity_names)})
            """.strip()

        vars_dict = {
            "bn": bn,
            "builder": self._builder,
            "record_call": self._descriptors_computed_by_flow.append,
            "compute_value": compute_func,
        }

        raw_func_code = f"""
        @builder
        {output_decorator_fragment}
        @bn.version({binding.func_version})
        @bn.persist({binding.should_persist})
        def _({joined_dep_entity_names}):
            record_call({binding.out_descriptor!r})
            return compute_value([{joined_dep_entity_names}])
        """
        exec(dedent(raw_func_code), vars_dict)

    def _compute_model_value(self, entity_name, memoized_values_by_entity_name=None):
        if memoized_values_by_entity_name is None:
            memoized_values_by_entity_name = {}
        if entity_name in memoized_values_by_entity_name:
            return memoized_values_by_entity_name[entity_name]

        entity = self._entities_by_name[entity_name]
        binding = entity.binding

        if not binding.has_persisted_values:
            dep_values = [
                self._compute_model_value(dep_entity_name)
                for dep_entity_name in binding.dep_entity_names
            ]
            values_by_entity_name = binding.compute_and_save_values(dep_values)
            self._descriptors_computed_by_model.append(binding.out_descriptor)

        else:
            values_by_entity_name = binding.persisted_values_by_entity_name

        memoized_values_by_entity_name.update(values_by_entity_name)
        return values_by_entity_name[entity_name]

    def _clear_called_descriptors(self):
        self._descriptors_computed_by_flow[:] = []
        self._descriptors_computed_by_model[:] = []


@attr.s
class ModelEntity:
    """Represents an entity in a Bionic flow."""

    name = attr.ib()
    should_persist = attr.ib()

    binding = attr.ib(default=None)
    dependent_entities = attr.ib(default=attr.Factory(list))

    def compute_value(self, dep_values):
        return f"{self.name}.{self.binding.func_version}({','.join(dep_values)})"


@attr.s
class ModelBinding:
    """
    Represents a user-provided function that computes the value of one or more entities.
    """

    out_dnode = attr.ib()
    out_entities = attr.ib()
    dep_entity_names = attr.ib()
    use_tuples_for_output = attr.ib()
    func_version = attr.ib(default=1)

    has_persisted_values = attr.ib(default=False)
    persisted_values_by_entity_name = attr.ib(default=None)
    some_descendant_might_have_persisted_values = attr.ib(default=False)

    def update_func_version(self):
        self.func_version += 1
        self.invalidate_persisted_values()

    def compute_value_for_outputs(self, dep_values):
        return tuple(
            out_entity.compute_value(dep_values) for out_entity in self.out_entities
        )

    def compute_value_for_returns(self, dep_values, out_dnode=None):
        if out_dnode is None:
            out_dnode = self.out_dnode

        if isinstance(out_dnode, ast.EntityNode):
            entity_name = out_dnode.to_entity_name()
            entity = single_element(
                entity for entity in self.out_entities if entity.name == entity_name
            )
            return entity.compute_value(dep_values)

        elif isinstance(out_dnode, ast.TupleNode):
            return tuple(
                self.compute_value_for_returns(dep_values, out_dnode=child_dnode)
                for child_dnode in out_dnode.children
            )

        else:
            assert False

    def compute_and_save_values(self, dep_values):
        values_by_entity_name = {
            entity.name: entity.compute_value(dep_values)
            for entity in self.out_entities
        }
        if self.should_persist:
            self.persisted_values_by_entity_name = values_by_entity_name
            self.has_persisted_values = True
        self.some_descendant_might_have_persisted_values = True
        return values_by_entity_name

    def invalidate_persisted_values(self):
        if not self.some_descendant_might_have_persisted_values:
            return
        self.has_persisted_values = False
        self.persisted_values_by_entity_name = None
        for out_entity in self.out_entities:
            for dependent_entity in out_entity.dependent_entities:
                dependent_entity.binding.invalidate_persisted_values()
        self.some_descendant_might_have_persisted_values = False

    @property
    def out_descriptor(self):
        return self.out_dnode.to_descriptor()

    @property
    def should_persist(self):
        return single_unique_element(
            entity.should_persist for entity in self.out_entities
        )


@attr.s
class RandomFlowTestConfig:
    """
    Holds configuration settings for a ``RandomFlowTester``.

    Attributes
    ----------

    n_bindings: int
        The number of bindings (definitions) the flow should contain.
    n_iterations: int
        The number of update-and-query operations to perform on the flow.
    p_persist: float between 0 and 1
        The probability that any entity should be persistable.
    p_multi_out: float between 0 and 1
        The probability that any binding should output multiple entities.
    p_three_out_given_multi: float between 0 and 1
        The probability that any multi-output binding should output three entities
        instead of two.
    """

    n_bindings = attr.ib()
    n_iterations = attr.ib()
    p_persist = attr.ib(default=0.5)
    p_multi_out = attr.ib(default=0.3)
    p_three_out_given_multi = attr.ib(default=0.3)
    use_tuples_for_output = attr.ib(default=True)


class RandomFlowTester:
    """
    Randomly constructs and tests a Bionic flow.

    Given a ``ModelFlowHarness``, randomly initializes a flow, then alternates
    between updating a random entity and checking the value of a random entity.

    This is analogous to the ``Fuzzer`` class in ``test_persistence_fuzz``, but (aside
    from using a different model implementation), this tester creates a different
    distribution of random flow graphs. The ``Fuzzer`` class creates random graphs
    in which each possible edge exists with an independent probability, resulting in
    dense graphs where the number of dependencies per entity grows linearly. This class
    has (nearly) keeps the same expected number of dependencies per entity as the graph
    grows, which is more realistic and cheaper to compute. This allows us to test
    larger random graphs.
    """

    def __init__(self, harness, config, seed=0):
        self._config = config
        self._harness = harness
        self._rng = np.random.default_rng(seed=seed)

        self._child_counts_by_entity_name = {}

    def run(self):
        for _ in range(self._config.n_bindings):
            self.add_random_binding()
        for _ in range(self._config.n_iterations):
            self.update_random_entity()
            self.query_and_check_random_entity()

    def add_random_binding(self):
        dep_entity_names = self._random_dep_entity_names()
        for dep_entity_name in dep_entity_names:
            self._child_counts_by_entity_name[dep_entity_name] += 1
        should_persist = self._random_bool(p_true=self._config.p_persist)
        descriptor = self._random_descriptor_of_new_entities(should_persist)
        self._harness.add_binding(
            out_descriptor=descriptor,
            dep_entity_names=dep_entity_names,
            use_tuples_for_output=self._config.use_tuples_for_output,
        )

    def update_random_entity(self):
        entity_name = self._random_existing_entity_name()
        self._harness.update_binding(entity_name)

    def query_and_check_random_entity(self):
        entity_name = self._random_existing_entity_name()
        self._harness.query_and_check_entity(entity_name)

    def _random_bool(self, p_true=0.5):
        assert 0.0 <= p_true <= 1.0
        return self._rng.random() < p_true

    def _random_dep_entity_names(self):
        # We'll generate a random list of dependencies for a new binding, with the
        # goal of creating realistic flow graphs that aren't too expensive to compute.

        # If no entities exist yet, our list will have to be empty.
        n_existing_entities = len(self._child_counts_by_entity_name)
        if n_existing_entities == 0:
            return []

        # We don't want the number of top-level entities (those with no dependencies)
        # to grow linearly with the size of the graph, so we'll decrease the
        # probability of these as we go.
        if self._random_bool(p_true=1.0 / n_existing_entities):
            return []

        # We want to favor deep graphs where each node has similar numbers of parents
        # and children, regardless of whether it's added early or late. So, we choose
        # the number of parents (dependencies) with an expected value of 2, and we favor
        # parents that don't have as many children yet.
        entity_names = list(self._child_counts_by_entity_name.keys())
        child_counts = np.array(list(self._child_counts_by_entity_name.values()))
        raw_weights = np.exp2(-child_counts)
        weights = raw_weights / raw_weights.sum()

        # We use the geometric distribution, which will usually be between 1 and 3, but
        # can sometimes give arbitrarily large values.
        expected_n_samples = 2
        n_samples = min(len(entity_names), self._rng.geometric(1 / expected_n_samples))

        return list(
            self._rng.choice(entity_names, size=n_samples, p=weights, replace=False)
        )

    def _random_existing_entity_name(self):
        return self._rng.choice(self._harness.get_all_entity_names())

    def _random_descriptor_of_new_entities(self, should_persist):
        def new_entity_name():
            entity_name = self._harness.create_entity(should_persist)
            self._child_counts_by_entity_name[entity_name] = 0
            return entity_name

        include_multiple_entities = self._random_bool(p_true=self._config.p_multi_out)
        if not include_multiple_entities:
            return new_entity_name()

        include_three_entities = self._random_bool(
            p_true=self._config.p_three_out_given_multi
        )
        if not include_three_entities:
            template = "X, X"

        else:
            # We'll randomly choose one of the following three-entity descriptors:
            template = self._rng.choice(
                [
                    "X, X, X",
                    "(X, X), X",
                    "X, (X, X)",
                    "(X,), (X,), (X,)",
                    "(X, X, X),",
                ],
            )

        return re.sub("X", lambda x: new_entity_name(), template)


@pytest.fixture
def harness(builder, make_list):
    return ModelFlowHarness(builder, make_list)


def test_random_flow_small(harness):
    config = RandomFlowTestConfig(n_bindings=10, n_iterations=20)
    RandomFlowTester(harness, config).run()


@pytest.mark.slow
@pytest.mark.parametrize("p_persist", [0.2, 0.5, 0.8])
@pytest.mark.parametrize("p_multi_out", [0.2, 0.6])
@pytest.mark.parametrize("n_bindings, n_iterations", [(10, 20), (50, 50)])
def test_random_flow_varied(harness, n_bindings, n_iterations, p_persist, p_multi_out):
    config = RandomFlowTestConfig(
        n_bindings=n_bindings,
        n_iterations=n_iterations,
        p_persist=p_persist,
        p_multi_out=p_multi_out,
    )
    RandomFlowTester(harness, config).run()


# TODO Once we've unified implementation of @outputs and @returns, we should rethink
# this test. Maybe we should support more ways of specifying outputs:
# - @returns
# - @outputs (only when the output is a non-nested tuple or single entity)
# - @output (only when the output is a single entity)
# - no decorator (only when the output is a single entity)
# Or maybe that should be beyond the scope of these tests, and we should assume that
# other tests are checking that they're equivalent?
@pytest.mark.slow
def test_random_flow_no_tuples(harness):
    config = RandomFlowTestConfig(
        n_bindings=50,
        n_iterations=50,
        use_tuples_for_output=False,
    )
    RandomFlowTester(harness, config).run()
