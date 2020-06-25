import pytest

from numpy.random import choice
from textwrap import dedent
from random import Random

from bionic.exception import CodeVersioningError
from bionic.persistence import FakeCloudStore
from bionic import interpret
import bionic as bn


class SimpleFlowModel:
    """
    Manages a simple Bionic flow where every entity value is an integer
    computed from the sum of other entity values.  This wrapper class maintains
    both the Bionic flow and a parallel model that can predict what the Bionic
    flow should be doing.  This can be used to generate, manipulate, and
    validate large flows.
    """

    def __init__(self, builder, make_list):
        self._builder = builder
        self._current_flow = None
        self._update_flow()

        self._entities_by_name = {}
        self._last_called_names = make_list()

    def add_entity(self, dep_names, nondeterministic=False):
        name = f"e{len(self._entities_by_name) + 1}"

        self._create_entity(name, dep_names, nondeterministic)
        self._define_entity(name)

        return name

    def update_entity(
        self,
        name,
        make_func_change=False,
        make_nonfunc_change=False,
        update_major=False,
        update_minor=False,
    ):

        entity = self._entities_by_name[name]

        if make_func_change:
            entity.func_value += 1
        if make_nonfunc_change:
            entity.nonfunc_value += 1
        if update_major:
            entity.major_version += 1
        if update_minor:
            entity.minor_version += 1

        self._define_entity(name)

    def get_flow(self):
        return self._current_flow

    def expected_entity_value(self, name):
        entity = self._entities_by_name[name]
        dep_values_total = sum(
            self.expected_entity_value(dep_name) for dep_name in entity.dep_names
        )
        return entity.func_value + dep_values_total

    def entity_names(self, downstream_of=None, upstream_of=None, with_children=None):
        names = set(self._entities_by_name.keys())

        if upstream_of is not None:
            bot_names = interpret.str_or_seq_as_list(upstream_of)

            upstream_names = set()
            for bot_name in bot_names:
                bot_entity = self._entities_by_name[bot_name]
                upstream_names |= bot_entity.all_upstream_names

            names = names & upstream_names

        if downstream_of is not None:
            top_names = interpret.str_or_seq_as_list(downstream_of)

            downstream_names = set()
            for top_name in top_names:
                top_entity = self._entities_by_name[top_name]
                downstream_names |= top_entity.all_downstream_names

            names = names & downstream_names

        if with_children is not None:
            names_with_children = set(
                name
                for name in names
                if len(self._entities_by_name[name].child_names) > 0
            )
            if with_children:
                names = names_with_children
            else:
                names -= names_with_children

        return list(sorted(names))

    def nondeterministic_upstream_entity_names(self, upstream_of):
        bot_names = interpret.str_or_seq_as_list(upstream_of)

        upstream_names = set()
        for bot_name in bot_names:
            bot_entity = self._entities_by_name[bot_name]
            upstream_names |= bot_entity.all_nondeterministic_upstream_names

        return upstream_names

    def entity_has_nondeterministic_ancestor(self, entity_name):
        entity = self._entities_by_name[entity_name]
        return len(entity.all_nondeterministic_upstream_names) > 0

    def entity_is_nondeterministic(self, entity_name):
        return self._entities_by_name[entity_name].is_nondeterministic

    def called_entity_names(self):
        names = list(self._last_called_names)
        self.reset_called_entity_names()
        return names

    def peek_called_entity_names(self):
        names = list(self._last_called_names)
        return names

    def reset_called_entity_names(self):
        self._last_called_names[:] = []

    def _create_entity(self, name, dep_names, is_nondeterministic):
        entity = ModelEntity(
            name=name, dep_names=dep_names, is_nondeterministic=is_nondeterministic
        )

        entity.all_upstream_names = {name}
        if is_nondeterministic:
            entity.all_nondeterministic_upstream_names = {name}
        for dep_name in dep_names:
            dep_entity = self._entities_by_name[dep_name]
            entity.all_upstream_names.update(dep_entity.all_upstream_names)
            entity.all_nondeterministic_upstream_names.update(
                dep_entity.all_nondeterministic_upstream_names
            )
            dep_entity.child_names.append(name)
        entity.all_downstream_names = {name}

        self._entities_by_name[name] = entity
        for anc_name in entity.all_upstream_names:
            anc_entity = self._entities_by_name[anc_name]
            anc_entity.all_downstream_names.add(name)

    def _define_entity(self, name):
        entity = self._entities_by_name[name]

        vars_dict = {
            "bn": bn,
            "builder": self._builder,
            "record_call": self._last_called_names.append,
            "noop_func": lambda x: None,
        }

        e = entity
        exec(
            dedent(
                f"""
            @builder
            @bn.changes_per_run({e.is_nondeterministic})
            @bn.version(major={e.major_version}, minor={e.minor_version})
            def {name}({', '.join(e.dep_names)}):
                noop_func({e.nonfunc_value})
                record_call("{name}")
                return {' + '.join([str(e.func_value)] + e.dep_names)}
            """
            ),
            vars_dict,
        )

        self._update_flow()

    def _update_flow(self):
        self._current_flow = self._builder.build()


class ModelEntity:
    """
    Represents one entity in the SimpleFlowModel.
    """

    def __init__(self, name, dep_names, is_nondeterministic):
        self.name = name
        self.dep_names = dep_names
        self.is_nondeterministic = is_nondeterministic

        self.major_version = 0
        self.minor_version = 0
        self.func_value = 0
        self.nonfunc_value = 0

        self.child_names = []
        self.all_upstream_names = set()
        self.all_nondeterministic_upstream_names = set()
        self.all_downstream_names = set()


class Fuzzer:
    """
    Randomly constructs and updates a SimpleModelFlow while checking that its
    behavior is as expected.
    """

    def __init__(self, builder, make_list, random_seed=0):
        self.model = SimpleFlowModel(builder, make_list)
        self._versioning_mode = "manual"
        self._builder = builder
        self._random = Random(random_seed)

    def set_versioning_mode(self, mode):
        self._builder.set("core__versioning_mode", mode)
        self._versioning_mode = mode

    def add_entities(self, n_entities):
        for i in range(n_entities):
            all_names = self.model.entity_names()

            dep_names = []
            is_nondeterministic = self._random_bool_with_weighted_probability(0.05)
            for name in all_names:
                if self._random_bool():
                    dep_names.append(name)
            new_name = self.model.add_entity(dep_names, is_nondeterministic)

            self.model.get_flow().get(new_name)
            expected_called_names = self.model.nondeterministic_upstream_entity_names(
                new_name
            )
            expected_called_names.add(new_name)
            assert sorted(self.model.called_entity_names()) == sorted(
                list(expected_called_names)
            )

    def run(self, n_iterations):
        for i in range(n_iterations):
            updated_name = self._random.choice(self.model.entity_names())
            affected_names = self.model.entity_names(
                downstream_of=updated_name, with_children=False
            )
            make_func_change = self._random_bool()
            make_nonfunc_change = not make_func_change
            update_version = self._random_bool()

            is_updated_entity_nondeterministic = self.model.entity_is_nondeterministic(
                updated_name
            )
            is_updated_entity_deterministic = not is_updated_entity_nondeterministic

            self.model.update_entity(
                updated_name,
                make_func_change=make_func_change,
                make_nonfunc_change=make_nonfunc_change,
                update_major=update_version and make_func_change,
                update_minor=update_version and make_nonfunc_change,
            )

            if not update_version:
                # If we didn't update the version, there's a potential mismatch
                # between the entity code and Bionic's understanding.  How this
                # plays out will depend on the versioning mode.

                if self._versioning_mode == "manual":
                    # Bionic doesn't know the code has changed, so this entity
                    # should still be returning the old value.
                    affected_name = self._random.choice(affected_names)
                    returned_value = self.model.get_flow().get(affected_name)
                    expected_value = self.model.expected_entity_value(affected_name)

                    # When the change is functional and bionic doesn't recompute the
                    # entity, the expected and returned values will be different.
                    if make_func_change and is_updated_entity_deterministic:
                        assert returned_value != expected_value
                    else:
                        assert returned_value == expected_value

                    # We peek at the called entity names to avoid changing the results of
                    # called_entity_names() later.
                    actual_called_names = self.model.peek_called_entity_names()
                    expects_updated_entity_value_change = (
                        make_func_change and is_updated_entity_nondeterministic
                    )
                    expected_called_names = self._expected_called_names(
                        updated_name, affected_name, expects_updated_entity_value_change
                    )
                    assert set(actual_called_names) == expected_called_names

                    # Now update the version to get the flow back into a
                    # "correct" state.
                    self.model.update_entity(
                        updated_name,
                        update_major=make_func_change,
                        update_minor=make_nonfunc_change,
                    )

                elif self._versioning_mode == "assist":
                    affected_name = self._random.choice(affected_names)

                    if is_updated_entity_deterministic:
                        # Bionic should detect that we forgot to update the
                        # version.
                        with pytest.raises(CodeVersioningError):
                            self.model.get_flow().get(affected_name)
                    else:
                        # Version does not matter for nondeterministic entities.
                        assert self.model.get_flow().get(
                            affected_name
                        ) == self.model.expected_entity_value(affected_name)

                    # Now update the version to get the flow back into a
                    # "correct" state.
                    self.model.update_entity(
                        updated_name,
                        update_major=make_func_change,
                        update_minor=make_nonfunc_change,
                    )

                elif self._versioning_mode == "auto":
                    # Even if we didn't update the version, Bionic should
                    # do it for us and the flow should already be in a correct
                    # state.
                    pass

                else:
                    raise AssertionError(
                        "Unexpected versioning mode: " f"{self._versioning_mode!r}"
                    )

            for affected_name in affected_names:
                assert self.model.get_flow().get(
                    affected_name
                ) == self.model.expected_entity_value(affected_name)

            actual_called_names = self.model.called_entity_names()
            expected_called_names = self._expected_called_names(
                updated_name, affected_names, make_func_change
            )
            assert set(actual_called_names) == expected_called_names

    def _random_bool(self):
        return self._random.choice([True, False])

    def _random_bool_with_weighted_probability(self, prob_true):
        assert 0.0 <= prob_true <= 1.0
        return choice([True, False], p=[prob_true, 1 - prob_true])

    def _expected_called_names(
        self, updated_name, affected_name_or_names, expects_updated_entity_value_change
    ):
        if expects_updated_entity_value_change:
            entity_names_in_between = set(
                self.model.entity_names(
                    downstream_of=updated_name, upstream_of=affected_name_or_names,
                )
            )
            nondeterministic_ancestors = self.model.nondeterministic_upstream_entity_names(
                entity_names_in_between
            )
            expected_called_names = entity_names_in_between | nondeterministic_ancestors

        # When the updated entity value doesn't change, we don't call anything between the
        # updated and affected entities since all the entities are persisted and use their
        # parents' value (hash) which did not change.
        else:
            expected_called_names = self.model.nondeterministic_upstream_entity_names(
                affected_name_or_names
            )
            if self._versioning_mode == "auto":
                expected_called_names.add(updated_name)

        return expected_called_names


@pytest.fixture(scope="function")
def fuzzer(builder, make_list, tmp_path):
    fake_cloud_store = FakeCloudStore(str(tmp_path / "BNTESTDATA_FAKE_CLOUD"))
    builder.set("core__persistent_cache__cloud_store", fake_cloud_store)
    return Fuzzer(builder, make_list)


foreach_mode = pytest.mark.parametrize("versioning_mode", ["manual", "assist", "auto"])


@foreach_mode
def test_small_fixed_flow_short_fuzz(fuzzer, versioning_mode):
    fuzzer.set_versioning_mode(versioning_mode)

    e1 = fuzzer.model.add_entity([])
    e2 = fuzzer.model.add_entity([])
    e3 = fuzzer.model.add_entity([e1])
    e4 = fuzzer.model.add_entity([e1, e2])
    e5 = fuzzer.model.add_entity([e3, e4])

    assert fuzzer.model.get_flow().get(e5) == fuzzer.model.expected_entity_value(e5)
    assert list(sorted(fuzzer.model.called_entity_names())) == list(
        sorted(fuzzer.model.entity_names())
    )

    fuzzer.run(n_iterations=30)


@pytest.mark.slow
@foreach_mode
def test_medium_random_flow_long_fuzz(fuzzer, versioning_mode):
    fuzzer.set_versioning_mode(versioning_mode)
    fuzzer.add_entities(10)
    fuzzer.run(n_iterations=100)


@pytest.mark.slow
@foreach_mode
def test_big_random_flow_medium_fuzz(fuzzer, versioning_mode):
    fuzzer.set_versioning_mode(versioning_mode)
    fuzzer.add_entities(20)
    fuzzer.run(n_iterations=50)
