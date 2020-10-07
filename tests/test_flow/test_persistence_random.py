import pytest

from textwrap import dedent

import attr
import numpy as np
import re

from bionic.descriptors.parsing import dnode_from_descriptor
from bionic.exception import CodeVersioningError
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
        self._versioning_mode = "manual"

        self._entities_by_name = {}

        self._descriptors_computed_by_flow = make_list()
        self._descriptors_computed_by_model = make_list()

    def set_versioning_mode(self, mode):
        assert mode in ("manual", "assist", "auto")
        self._versioning_mode = mode
        self._builder.set("core__versioning_mode", mode)

    def get_all_entity_names(self):
        return list(self._entities_by_name.keys())

    def create_entity(self, should_persist=True):
        name = f"e{len(self._entities_by_name) + 1}"
        entity = ModelEntity(name=name, should_persist=should_persist)
        assert name not in self._entities_by_name
        self._entities_by_name[name] = entity
        return name

    def add_binding(self, out_descriptor, dep_entity_names, use_tuples_for_output=True):
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

    def update_binding(
        self,
        entity_name,
        change_func_version=True,
        update_version_annotation=True,
    ):
        entity = self._entities_by_name[entity_name]
        entity.binding.update(
            change_func=change_func_version,
            update_annotation=update_version_annotation,
            versioning_mode=self._versioning_mode,
        )

        self._add_binding_to_flow(entity.binding)

    def query_and_check_entity(self, entity_name):
        flow = self._builder.build()

        flow_exception = None
        try:
            flow_value = flow.get(entity_name)
        except CodeVersioningError as e:
            flow_exception = e

        model_exception = None
        try:
            model_value = self._compute_model_value(entity_name)
        except CodeVersioningError as e:
            model_exception = e

        assert (flow_exception is None) == (model_exception is None)

        if model_exception is None:
            assert flow_value == model_value

        else:
            assert flow_exception.__class__ == model_exception.__class__

            # Now we know that a CodeVersioningError has been thrown, we'll try to get
            # the flow back into a good state by fixing the versioning error. We can
            # pick any of the entities mentioned in the error, since they're all set
            # by the same binding. We'll keep recursively retrying until all the errors
            # are fixed and the query succeeds. At that point both the real flow and the
            # model flow should have computed and persisted the same set of descriptors,
            # so we'll be back in a known-good state.
            # (That's why we do the fixing inside this function instead of leaving it
            # to the caller: it's a little awkward, but it lets us guarantee that both
            # flows will be in sync by the time we return.)

            # It would be nice if we could also assert that both exceptions have the
            # same `bad_descriptor` field, but if there are multiple bad descriptors,
            # which one we get first is not defined. In theory we could, after fixing
            # everything, check that the sets of bad descriptors matched, but I'm not
            # sure it's worth the extra bookkeeping.
            bad_entity_name = dnode_from_descriptor(
                model_exception.bad_descriptor
            ).all_entity_names()[0]
            self.update_binding(
                entity_name=bad_entity_name,
                change_func_version=False,
                update_version_annotation=True,
            )

            self.query_and_check_entity(entity_name)

        assert set(self._descriptors_computed_by_flow) == set(
            self._descriptors_computed_by_model
        )
        self._clear_called_descriptors()

    # This is just used for debugging.
    def save_dag(self, filename):
        self._builder.build().render_dag().save(filename)

    def _add_binding_to_flow(self, binding):
        out_entity_names = [entity.name for entity in binding.out_entities]

        joined_dep_entity_names = ", ".join(binding.dep_entity_names)

        # TODO Could this logic live inside ModelBinding instead?
        if binding.use_tuples_for_output:
            output_decorator_fragment = f"""
            @bn.returns({binding.out_descriptor!r})
            """.strip()
            output_value_fragment = binding.value_code_fragment_for_returns()
        else:
            output_decorator_fragment = f"""
            @bn.outputs({', '.join(repr(name) for name in out_entity_names)})
            """.strip()
            output_value_fragment = binding.value_code_fragment_for_outputs()

        vars_dict = {
            "bn": bn,
            "builder": self._builder,
            "record_call": self._descriptors_computed_by_flow.append,
        }

        raw_func_code = f"""
        @builder
        {output_decorator_fragment}
        @bn.version({binding.annotated_func_version})
        @bn.persist({binding.should_persist})
        def _({joined_dep_entity_names}):
            record_call({binding.out_descriptor!r})
            return {output_value_fragment}
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
            if binding.persisted_values_stale_ancestor is not None:
                assert self._versioning_mode == "assist"
                stale_ancestor = binding.persisted_values_stale_ancestor
                raise CodeVersioningError("Binding is stale", stale_ancestor)
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

    def value_code_fragment(self):
        return (
            f"'{self.name}.{self.binding.func_version}('"
            + " + ',' ".join(f"+ {name}" for name in self.binding.dep_entity_names)
            + " + ')'"
        )


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
    annotated_func_version = attr.ib(default=1)

    has_persisted_values = attr.ib(default=False)
    persisted_values_by_entity_name = attr.ib(default=None)
    persisted_values_stale_ancestor = attr.ib(default=None)

    no_descendant_can_have_persisted_values = attr.ib(default=True)
    no_descendant_can_have_stale_persisted_values = attr.ib(default=True)

    def update(self, change_func, update_annotation, versioning_mode):
        func_changed = False
        annotation_changed = False
        if change_func:
            self.func_version += 1
            func_changed = True
        if update_annotation and self.annotated_func_version != self.func_version:
            self.annotated_func_version = self.func_version
            annotation_changed = True

        if versioning_mode == "manual":
            if annotation_changed:
                self.clear_persisted_values()

        elif versioning_mode == "assist":
            if annotation_changed:
                self.clear_persisted_values()
            elif func_changed:
                stale_ancestor = self.out_entities[0].name
                self.mark_persisted_values_stale(stale_ancestor)

        elif versioning_mode == "auto":
            if func_changed or annotation_changed:
                self.clear_persisted_values()

        else:
            assert False

    def clear_persisted_values(self):
        if self.no_descendant_can_have_persisted_values:
            return

        if self.has_persisted_values:
            self.has_persisted_values = False
            self.persisted_values_by_entity_name = None

        for out_entity in self.out_entities:
            for dependent_entity in out_entity.dependent_entities:
                dependent_entity.binding.clear_persisted_values()

        self.no_descendant_can_have_persisted_values = True
        self.no_descendant_can_have_stale_persisted_values = True

    def mark_persisted_values_stale(self, stale_ancestor):
        if self.no_descendant_can_have_stale_persisted_values:
            return

        if self.has_persisted_values and self.persisted_values_stale_ancestor is None:
            self.persisted_values_stale_ancestor = stale_ancestor

        for out_entity in self.out_entities:
            for dependent_entity in out_entity.dependent_entities:
                dependent_entity.binding.mark_persisted_values_stale(stale_ancestor)

        self.no_descendant_can_have_stale_persisted_values = True

    def value_code_fragment_for_outputs(self):
        return " ".join(
            f"({entity.value_code_fragment()})," for entity in self.out_entities
        )

    def value_code_fragment_for_returns(self, out_dnode=None):
        if out_dnode is None:
            out_dnode = self.out_dnode

        if out_dnode.is_entity():
            entity_name = out_dnode.assume_entity().name
            entity = single_element(
                entity for entity in self.out_entities if entity.name == entity_name
            )
            return entity.value_code_fragment()

        elif out_dnode.is_tuple():
            return " ".join(
                f"({self.value_code_fragment_for_returns(child_dnode)}),"
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
            self.persisted_values_stale_ancestor = None
            self.has_persisted_values = True
        self.no_descendant_can_have_persisted_values = False
        self.no_descendant_can_have_stale_persisted_values = False
        return values_by_entity_name

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
    p_update_version_annotation: float between 0 and 1
        The probability that a binding's version annotation will be updated when its
        definition is changed.
    use_tuples_for_output: bool
        If True, all definitions use ``@returns`` and may include nested tuples; if
        False, all definitions use ``@outputs`` and nested tuples are flattened into
        single-level tuples.
    versioning_mode: "manual", "assist", or "auto"
        Determines the value of ``core__versioning_mode`` in the tested flow.
    """

    n_bindings = attr.ib()
    n_iterations = attr.ib()
    p_persist = attr.ib(default=0.5)
    p_multi_out = attr.ib(default=0.3)
    p_three_out_given_multi = attr.ib(default=0.3)
    p_update_version_annotation = attr.ib(default=1.0)
    use_tuples_for_output = attr.ib(default=True)
    versioning_mode = attr.ib(default="manual")


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
        self._harness.set_versioning_mode(self._config.versioning_mode)

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
        update_annotation = self._random_bool(self._config.p_update_version_annotation)
        self._harness.update_binding(
            entity_name=entity_name,
            update_version_annotation=update_annotation,
        )

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


# This test is mostly here as an example of how to use the harness in a deterministic
# setting. It's a useful starting point for creating simple tests cases for debugging.
def test_harness(harness):
    harness.set_versioning_mode("assist")

    e1 = harness.create_entity()
    harness.add_binding(e1, [])

    e2 = harness.create_entity()
    harness.add_binding(e2, [e1])

    e3 = harness.create_entity()
    harness.add_binding(e3, [e2])

    harness.query_and_check_entity(e3)

    harness.update_binding(e1, update_version_annotation=False)
    harness.query_and_check_entity(e3)


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


@pytest.mark.slow
@pytest.mark.parametrize("n_bindings, n_iterations", [(10, 20), (50, 50)])
@pytest.mark.parametrize("versioning_mode", ["manual", "assist", "auto"])
def test_random_flow_versioning(harness, n_bindings, n_iterations, versioning_mode):
    config = RandomFlowTestConfig(
        n_bindings=n_bindings,
        n_iterations=n_iterations,
        p_persist=0.7,
        p_update_version_annotation=0.5,
        p_multi_out=0.3,
        versioning_mode=versioning_mode,
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
