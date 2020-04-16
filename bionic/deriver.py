"""
Contains the core logic for resolving Entities by executing Tasks.
"""

from collections import defaultdict

import attr

from .datatypes import ProvenanceDigest, Query, Result, ResultGroup
from .datatypes import ProvenanceDigest, Query, ResultGroup
from .cache import Provenance
from .descriptors import DescriptorNode
from .exception import UndefinedEntityError, CodeVersioningError
from .execution import complete_task_state, get_results_for_complete_task_state
from .optdep import import_optional_dependency
from .util import oneline

import logging

# TODO At some point it might be good to have the option of Bionic handling its
# own logging.  Probably it would manage its own logger instances and inject
# them into tasks, while providing the option of either handling the output
# itself or routing it back to the global logging system.
logger = logging.getLogger(__name__)


class EntityDeriver:
    """
    Derives the values of Entities.

    This is the class that constructs the entity graph and computes the value
    or values of each entity.
    """

    # --- Public API.

    def __init__(self, flow_state, flow_instance_uuid):
        self._flow_state = flow_state
        self._flow_instance_uuid = flow_instance_uuid

        # These are used to cache DescriptorInfo and TaskState objects, respectively.
        self._saved_dinfos_by_dnode = {}
        self._saved_task_states_by_key = {}

        # Tracks whether we've pre-validated the base descriptors in this flow.
        self._base_prevalidation_is_complete = False

        # The "bootstrap state" needs to be complete before we can compute user-defined
        # entities.
        self._bootstrap_is_complete = False
        self._persistent_cache = None
        self._versioning_policy = None
        self._executor = None
        self._process_manager = None

    def get_ready(self):
        """
        Make sure this Deriver is ready to derive().  Calling this is not
        necessary but allows errors to surface earlier.
        """
        self._prevalidate_base_dnodes()
        self._set_up_bootstrap()

    def derive(self, dnode):
        """
        Given a descriptor node, computes and returns a ResultGroup containing
        all values for that descriptor.
        """
        self.get_ready()
        return self._compute_result_group_for_dnode(dnode)

    def export_dag(self, include_core=False):
        """
        Constructs a NetworkX graph corresponding to the DAG of tasks.  There
        is one node per task key -- i.e., for each artifact that can be created
        (uniquely defined by an entity name and a case key); and one edge from
        each task key to each key that depends on it.  Each node is represented
        by a TaskKey, and also has the following attributes:

            name: a short, unique, human-readable identifier
            entity_name: the name of the entity for this task key
            case_key: the case key for this task key
            task_ix: the task key's index in the ordered series of case keys
                     for its entity
        """
        nx = import_optional_dependency("networkx", purpose="constructing the flow DAG")

        def should_include_entity_name(name):
            return include_core or not self.entity_is_internal(entity_name)

        self.get_ready()

        graph = nx.DiGraph()

        for dnode in self._get_base_dnodes():
            tasks = self._get_or_create_dinfo_for_dnode(dnode).tasks

            entity_name = dnode.to_descriptor()
            if not should_include_entity_name(entity_name):
                continue

            if len(tasks) == 1:
                name_template = "{entity_name}"
            else:
                name_template = "{entity_name}[{task_ix}]"

            for task_ix, task in enumerate(
                sorted(tasks, key=lambda task: task.keys[0].case_key)
            ):
                task_key = task.key_for_entity_name(entity_name)
                state = self._get_or_create_task_state_for_key(task_key)

                node_name = name_template.format(
                    entity_name=entity_name, task_ix=task_ix
                )

                graph.add_node(
                    task_key,
                    name=node_name,
                    entity_name=entity_name,
                    case_key=task_key.case_key,
                    task_ix=task_ix,
                    doc=self._flow_state.get_provider(entity_name).doc_for_name(
                        entity_name
                    ),
                )

                for dep_state in state.dep_states:
                    for dep_task_key in dep_state.task.keys:
                        graph.add_edge(dep_task_key, task_key)

        return graph

    def entity_is_internal(self, entity_name):
        "Indicates if an entity is built-in to Bionic rather than user-defined."
        return entity_name.startswith("core__")

    # --- Private helpers.

    def _set_up_bootstrap(self):
        """
        Initializes some key objects needed to compute user-defined entities.
        """

        if self._bootstrap_is_complete:
            return

        self._persistent_cache = self._bootstrap_singleton_entity(
            "core__persistent_cache"
        )

        self._versioning_policy = self._bootstrap_singleton_entity(
            "core__versioning_policy"
        )

        self._executor = self._bootstrap_singleton_entity("core__process_executor")

        self._process_manager = self._bootstrap_singleton_entity(
            "core__process_manager"
        )

        self._bootstrap_is_complete = True

    def _prevalidate_base_dnodes(self):
        """
        Checks that all 'base' descriptors can be computed.

        (This precomputes and caches all the metadata required for each of these
        descriptors. If you don't call this, the same work will happen lazily later, so
        the only effect of this function is to cause any errors to be surfaced earlier.)
        """

        # Avoid doing pre-validation multiple times. (It's not that expensive since all
        # the state is cached, but it's still O(number of descriptors), so we'll avoid
        # it on principle.)
        if self._base_prevalidation_is_complete:
            return

        self._prevalidate_dnodes(self._get_base_dnodes())

        self._base_prevalidation_is_complete = True

    def _prevalidate_dnodes(self, dnodes):
        """
        Identifies (and caches) all tasks required to compute a collection of dnodes.
        Useful for surfacing any dependency errors ahead of time.
        """

        for dnode in dnodes:
            dinfo = self._get_or_create_dinfo_for_dnode(dnode)
            for task_key, task in dinfo.tasks_by_key.items():
                self._get_or_create_task_state_for_key(task_key)

    def _get_base_dnodes(self):
        """
        Returns the list of descriptor nodes needed to compute all user-defined entities
        and internal entities. (At the time of writing, this includes every
        valid descriptor, but in the future there will be an infinite number
        of valid descriptors.)
        """

        return [
            DescriptorNode.from_descriptor(entity_name)
            for entity_name in self._flow_state.providers_by_name.keys()
        ]

    def _get_or_create_dinfo_for_dnode(self, dnode):
        "Computes (and memoizes) a DescriptorInfo object for a descriptor node."

        if dnode in self._saved_dinfos_by_dnode:
            return self._saved_dinfos_by_dnode[dnode]

        entity_name = dnode.to_entity_name()
        provider = self._flow_state.get_provider(entity_name)

        dep_dnodes = provider.get_dependency_dnodes()
        dep_dinfos = [
            self._get_or_create_dinfo_for_dnode(dep_dnode) for dep_dnode in dep_dnodes
        ]
        dep_key_spaces_by_dnode = {
            dep_dinfo.dnode: dep_dinfo.key_space for dep_dinfo in dep_dinfos
        }
        dep_task_key_lists_by_dnode = {
            dep_dinfo.dnode: [
                task.key_for_entity_name(dep_dinfo.dnode.to_entity_name())
                for task in dep_dinfo.tasks
            ]
            for dep_dinfo in dep_dinfos
        }

        key_space = provider.get_key_space(dep_key_spaces_by_dnode)
        tasks = provider.get_tasks(dep_key_spaces_by_dnode, dep_task_key_lists_by_dnode)
        tasks_by_key = {
            task_key: task
            for task in tasks
            for task_key in task.keys
            if task_key.dnode == dnode
        }

        dinfo = DescriptorInfo(
            dnode=dnode, key_space=key_space, tasks_by_key=tasks_by_key,
        )

        self._saved_dinfos_by_dnode[dnode] = dinfo
        return dinfo

    def _get_or_create_task_state_for_key(self, task_key):
        "Computes (and memoizes) a TaskState for a task key."

        if task_key in self._saved_task_states_by_key:
            return self._saved_task_states_by_key[task_key]

        dnode = task_key.dnode
        dinfo = self._get_or_create_dinfo_for_dnode(dnode)
        task = dinfo.tasks_by_key[task_key]

        dep_states = [
            self._get_or_create_task_state_for_key(dep_key) for dep_key in task.dep_keys
        ]
        # All keys in this task should point to the same provider, so the set below
        # should have exactly one element.
        (provider,) = set(
            self._flow_state.get_provider(task_key.dnode.to_entity_name())
            for task_key in task.keys
        )
        # And all the task keys should have the same case key.
        (case_key,) = set(task_key.case_key for task_key in task.keys)

        task_state = TaskState(
            task=task, dep_states=dep_states, provider=provider, case_key=case_key,
        )

        for task_key in task.keys:
            self._saved_task_states_by_key[task_key] = task_state
        return task_state

    def _bootstrap_singleton_entity(self, entity_name):
        """
        Computes the value of a 'bootstrap' entity -- i.e., a fundamental
        internal entity needed to compute user-defined entities. Assumes the entity
        has a single value.
        """

        dnode = DescriptorNode.from_descriptor(entity_name)
        result_group = self._compute_result_group_for_dnode(dnode)
        if len(result_group) == 0:
            raise ValueError(
                oneline(
                    f"""
                No values were defined for internal bootstrap entity
                {entity_name!r}"""
                )
            )
        if len(result_group) > 1:
            values = [result.value for result in result_group]
            raise ValueError(
                oneline(
                    f"""
                Bootstrap entity {entity_name!r} must have exactly one
                value; got {len(values)} ({values!r})"""
                )
            )
        return result_group[0].value

    def _compute_result_group_for_dnode(self, dnode):
        """
        Computes all results for a descriptor node. Will recursively compute any
        dependencies for that node as well.
        """

        dinfo = self._get_or_create_dinfo_for_dnode(dnode)
        requested_task_states = [
            self._get_or_create_task_state_for_key(task.keys[0]) for task in dinfo.tasks
        ]

        ready_task_states = list(requested_task_states)

        blockage_tracker = TaskBlockageTracker()

        log_level = logging.INFO if self._bootstrap_is_complete else logging.DEBUG
        task_key_logger = TaskKeyLogger(log_level)

        while ready_task_states:
            state = ready_task_states.pop()

            # If this task is already complete, we don't need to do any work.
            if state.is_complete:
                continue

            # If blocked, let's mark it and try to derive its dependencies.
            incomplete_dep_states = state.incomplete_dep_states()
            if incomplete_dep_states:
                blockage_tracker.add_blockage(
                    blocked_state=state, blocking_states=incomplete_dep_states,
                )
                ready_task_states.extend(incomplete_dep_states)
                continue

            # Initialize the task state before attempting to complete it.
            self._initialize_task_state(state)

            # If the task isn't complete or blocked, we can complete the task.
            complete_task_state(state, task_key_logger)

            # See if we can unblock any other states now that we've completed this one.
            unblocked_states = blockage_tracker.get_unblocked_by(state)
            ready_task_states.extend(unblocked_states)

        blocked_states = blockage_tracker.get_all_blocked_states()
        assert not blocked_states, blocked_states

        for state in requested_task_states:
            assert state.is_complete, state

        entity_name = dnode.to_entity_name()
        return ResultGroup(
            results=[
                get_results_for_complete_task_state(state, task_key_logger)[
                    entity_name
                ]
                for state in requested_task_states
            ],
            key_space=dinfo.key_space,
        )

    def _initialize_task_state(self, task_state):
        "Initializes the task state to get it ready for completion."

        if task_state.is_initialized:
            return
        
        # First, set up provenance.
        if not self._bootstrap_is_complete:
            # If we're still in the bootstrap resolution phase, we don't have
            # any versioning policy, so we don't attempt anything fancy.
            treat_bytecode_as_functional = False
        else:
            treat_bytecode_as_functional = (
                self._versioning_policy.treat_bytecode_as_functional
            )

        dep_provenance_digests_by_task_key = {}
        for dep_key, dep_state in zip(task_state.task.dep_keys, task_state.dep_states):
            # Use value hash of persistable values.
            if dep_state.provider.attrs.should_persist():
                value_hash = dep_state.result_value_hashes_by_name[
                    dep_key.dnode.to_entity_name()
                ]
                dep_provenance_digests_by_task_key[
                    dep_key
                ] = ProvenanceDigest.from_value_hash(value_hash)
            # Otherwise, use the provenance.
            else:
                dep_provenance_digests_by_task_key[
                    dep_key
                ] = ProvenanceDigest.from_provenance(dep_state.provenance)

        task_state.provenance = Provenance.from_computation(
            code_fingerprint=task_state.provider.get_code_fingerprint(
                task_state.case_key
            ),
            case_key=task_state.case_key,
            dep_provenance_digests_by_task_key=dep_provenance_digests_by_task_key,
            treat_bytecode_as_functional=treat_bytecode_as_functional,
            can_functionally_change_per_run=task_state.provider.attrs.changes_per_run,
            flow_instance_uuid=self._flow_instance_uuid,
        )

        # Then set up queries.
        task_state.queries = [
            Query(
                task_key=task_key,
                protocol=task_state.provider.protocol_for_name(
                    task_key.dnode.to_entity_name()
                ),
                provenance=task_state.provenance,
            )
            for task_key in task_state.task.keys
        ]

        # Lastly, set up cache accessors.
        if task_state.provider.attrs.should_persist():
            if not self._bootstrap_is_complete:
                name = task_state.task.keys[0].entity_name
                raise AssertionError(
                    oneline(
                        f"""
                    Attempting to load cached state for entity {name!r},
                    but the cache is not available yet because core bootstrap
                    entities depend on this one;
                    you should decorate entity {name!r} with `@persist(False)`
                    or `@immediate` to indicate that it can't be cached."""
                    )
                )

            task_state.cache_accessors = [
                self._persistent_cache.get_accessor(query)
                for query in task_state.queries
            ]

            if self._versioning_policy.check_for_bytecode_errors:
                self._check_accessors_for_version_problems(task_state)

        task_state.is_initialized = True

    def _check_accessors_for_version_problems(self, task_state):
        """
        Checks a task state for any versioning errors -- i.e., any cases where a task's
        function code was updated but its version annotation was not.
        """

        accessors_needing_saving = []
        for accessor in task_state.cache_accessors:
            old_prov = accessor.load_provenance()

            if old_prov is None:
                continue

            new_prov = accessor.query.provenance

            if old_prov.exactly_matches(new_prov):
                continue
            accessors_needing_saving.append(accessor)

            if old_prov.code_version_minor == new_prov.code_version_minor:
                if old_prov.bytecode_hash != new_prov.bytecode_hash:
                    raise CodeVersioningError(
                        oneline(
                            f"""
                        Found a cached artifact with the same
                        descriptor ({accessor.query.dnode.to_descriptor()!r}) and
                        version (major={old_prov.code_version_major!r},
                        minor={old_prov.code_version_minor!r}),
                        But created by different code
                        (old hash {old_prov.bytecode_hash!r},
                        new hash {new_prov.bytecode_hash!r}).
                        Did you change your code but not update the
                        version number?
                        Change @version(major=) to indicate that your
                        function's behavior has changed, or @version(minor=)
                        to indicate that it has *not* changed."""
                        )
                    )

        for accessor in accessors_needing_saving:
            accessor.update_provenance()


class TaskKeyLogger:
    """
    Logs how we derived each task key. The purpose of this class is to make sure that
    each task key used in a derivation (i.e., a call to `Flow.get()`) is logged exactly
    once. (One exception: a task key can be logged twice to indicate the start and end
    of a computation.)
    """

    def __init__(self, level):
        self._level = level
        self._already_logged_task_keys = set()

    def _log(self, template, task_key, is_resolved=True):
        if task_key in self._already_logged_task_keys:
            return
        logger.log(self._level, template, task_key)
        if is_resolved:
            self._already_logged_task_keys.add(task_key)

    def log_accessed_from_memory(self, task_key):
        self._log("Accessed   %s from in-memory cache", task_key)

    def log_accessed_from_definition(self, task_key):
        self._log("Accessed   %s from definition", task_key)

    def log_loaded_from_disk(self, task_key):
        self._log("Loaded     %s from disk cache", task_key)

    def log_computing(self, task_key):
        self._log("Computing  %s ...", task_key, is_resolved=False)

    def log_computed(self, task_key):
        self._log("Computed   %s", task_key)


@attr.s(frozen=True)
class DescriptorInfo:
    """
    Holds useful metadata about a descriptor.
    """

    dnode = attr.ib()
    key_space = attr.ib()
    tasks_by_key = attr.ib()

    @property
    def tasks(self):
        return self.tasks_by_key.values()


class TaskState:
    """
    Represents the state of a task computation.  Keeps track of its position in
    the task graph, whether its values have been computed yet, and additional
    intermediate state.
    """

    def __init__(self, task, dep_states, case_key, provider):
        self.task = task
        self.dep_states = dep_states
        self.case_key = case_key
        self.provider = provider

        # These are set by complete_task_state(), just
        # before the task state becomes eligible for cache lookup / computation.
        #
        # They will be present if and only if is_complete is True.
        self.is_initialized = False
        self.provenance = None
        self.queries = None
        self.cache_accessors = None

        # This can be set by complete_task_state() or _compute_task_state().
        #
        # This will be present if and only if both is_complete and
        # provider.attrs.should_persist() are True.
        self.result_value_hashes_by_name = None

        # This can be set by
        # get_results_for_complete_task_state() or _compute_task_state().
        #
        # This should never be accessed directly, instead use
        # get_results_for_complete_task_state().
        self._results_by_name = None

        self.is_complete = False

    def incomplete_dep_states(self):
        return [dep_state for dep_state in self.dep_states if not dep_state.is_complete]

    @property
    def is_blocked(self):
        return len(self.incomplete_dep_states()) > 0

    def __repr__(self):
        return f"TaskState({self.task!r})"


class TaskBlockage:
    """
    Represents a blocking relationship between a task state and a collection of
    not-yet-completed task keys it depends on.
    """

    def __init__(self, blocked_state, blocking_tks):
        self.blocked_state = blocked_state
        self._blocking_tks = set(blocking_tks)

    def mark_task_key_complete(self, blocking_tk):
        self._blocking_tks.discard(blocking_tk)

    def is_resolved(self):
        return not self._blocking_tks


class TaskBlockageTracker:
    """
    A helper class that keeps track of which task states are blocked by others.

    A task state X is "blocked" by another task state Y if X depends on Y and Y is
    not complete.
    """

    def __init__(self):
        self._blockage_lists_by_blocking_tk = defaultdict(list)

    def add_blockage(self, blocked_state, blocking_states):
        """Records the fact that one task state is blocked by certain others."""

        blocking_tks = [
            blocking_tk
            for blocking_state in blocking_states
            for blocking_tk in blocking_state.task.keys
        ]
        blockage = TaskBlockage(blocked_state, blocking_tks)
        for blocking_tk in blocking_tks:
            self._blockage_lists_by_blocking_tk[blocking_tk].append(blockage)

    def get_unblocked_by(self, completed_state):
        """
        Records the fact that a task state is complete, and yields all task states
        that are newly unblocked.
        """

        for completed_tk in completed_state.task.keys:
            affected_blockages = self._blockage_lists_by_blocking_tk[completed_tk]
            for blockage in affected_blockages:
                blockage.mark_task_key_complete(completed_tk)
                if blockage.is_resolved():
                    yield blockage.blocked_state

    def get_all_blocked_states(self):
        return {
            blockage.blocked_state
            for blockages in self._blockage_lists_by_blocking_tk.values()
            for blockage in blockages
            if not blockage.is_resolved()
        }
