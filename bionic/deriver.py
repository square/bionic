"""
Contains the core logic for resolving Entities by executing Tasks.
"""

from collections import defaultdict
from concurrent.futures import wait, FIRST_COMPLETED
from enum import Enum, auto

import attr

from .datatypes import ResultGroup, EntityDefinition
from .descriptors.parsing import entity_dnode_from_descriptor
from .descriptors import ast
from .exception import UndefinedEntityError
from .deps.optdep import import_optional_dependency
from .protocols import TupleProtocol
from .provider import TupleConstructionProvider, TupleDeconstructionProvider
from .task_state import TaskState
from .util import oneline, SynchronizedSet

import logging

# TODO At some point it might be good to have the option of Bionic handling its
# own logging.  Probably it would manage its own logger instances and inject
# them into tasks, while providing the option of either handling the output
# itself or routing it back to the global logging system.
logger = logging.getLogger(__name__)


def entity_is_internal(entity_name):
    "Indicates if an entity is built-in to Bionic rather than user-defined."
    return entity_name.startswith("core__")


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

        # For many dnodes, we can precompute the appropriate provider ahead of time.
        # This is set by _register_static_providers().
        self._static_providers_by_dnode = None

        # These are used to cache DescriptorInfo and TaskState objects, respectively.
        self._saved_dinfos_by_dnode = {}
        self._saved_task_states_by_key = {}

        # Tracks whether we've pre-validated the base descriptors in this flow.
        self._base_prevalidation_is_complete = False

        # The "bootstrap state" needs to be complete before we can compute user-defined
        # entities.
        self._bootstrap = None

    # TODO We should adjust the wording of the docstring below or refactor this a
    # little. It's not necessary for *the user* to call this method, but it is necessary
    # for it to get called before calling _compute_result_group_for_dnode(), which is
    # why derive() calls it.
    def get_ready(self):
        """
        Make sure this Deriver is ready to derive().  Calling this is not
        necessary but allows errors to surface earlier.
        """
        self._register_static_providers()
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

        def should_include_dnode(dnode):
            if include_core:
                return True
            if isinstance(dnode, ast.EntityNode):
                return not entity_is_internal(dnode.to_entity_name())
            return True

        self.get_ready()

        graph = nx.DiGraph()

        dnodes_to_add_to_graph = list(self._get_base_dnodes())
        dnodes_already_added = set()
        while dnodes_to_add_to_graph:
            dnode = dnodes_to_add_to_graph.pop()
            if dnode in dnodes_already_added:
                continue

            if not should_include_dnode(dnode):
                continue

            tasks = self._get_or_create_dinfo_for_dnode(dnode).tasks
            descriptor = dnode.to_descriptor()
            doc = self._obtain_entity_def_for_dnode(dnode).doc

            for task_ix, task in enumerate(
                sorted(tasks, key=lambda task: task.keys[0].case_key)
            ):
                task_key = task.key_for_dnode(dnode)
                state = self._get_or_create_task_state_for_key(task_key)

                if len(tasks) == 1:
                    node_name = descriptor
                else:
                    node_name = f"{dnode.to_descriptor(near_commas=True)}[{task_ix}]"

                graph.add_node(
                    task_key,
                    name=node_name,
                    descriptor=descriptor,
                    case_key=task_key.case_key,
                    task_ix=task_ix,
                    doc=doc,
                )

                for dep_state in state.dep_states:
                    for dep_task_key in dep_state.task.keys:
                        graph.add_edge(dep_task_key, task_key)
                        dnodes_to_add_to_graph.append(dep_task_key.dnode)

            dnodes_already_added.add(dnode)

        return graph

    # --- Private helpers.

    def _register_static_providers(self):
        """
        Precomputes an appropriate provider for each the following descriptor nodes:

        1. Any descriptor explicitly provided by a user-supplied function.
        2. Any descriptor which can be obtained by decomposing one of the first types of
        descriptors. (E.g., if a user function provides the descriptor "x, (y, z)",
        we will also precompute a provider for "x"; "y, z"; "y"; and "z".)

        This includes every entity descriptor that can be computed. The remaining
        descriptors are all tuples, so it's easy to generate providers for them
        just-in-time because they're composed from other descriptors. (For example,
        in the example above, it's easy to make a provider for "x, y" because we know
        that "x" and "y" must be precomputed. However, finding a provider
        just-in-time for "z" would require a backtracking search, which is why we
        precompute it instead.)

        The just-in-time computation happens in _obtain_provider_for_dnode().
        """

        if self._static_providers_by_dnode is not None:
            return

        static_providers_by_dnode = {}

        def register_provider(provider):
            for dnode in provider.attrs.dnodes:
                assert dnode not in static_providers_by_dnode
                static_providers_by_dnode[dnode] = provider

                if isinstance(dnode, ast.TupleNode):
                    for child_dnode in dnode.children:
                        child_provider = TupleDeconstructionProvider(child_dnode, dnode)
                        register_provider(child_provider)

        for provider in set(self._flow_state.providers_by_name.values()):
            register_provider(provider)

        self._static_providers_by_dnode = static_providers_by_dnode

    def _set_up_bootstrap(self):
        """
        Initializes some key objects needed to compute user-defined entities.
        """

        if self._bootstrap is not None:
            return

        self._bootstrap = Bootstrap(
            persistent_cache=self._bootstrap_singleton_entity("core__persistent_cache"),
            versioning_policy=self._bootstrap_singleton_entity(
                "core__versioning_policy"
            ),
            executor=self._bootstrap_singleton_entity("core__executor"),
            should_memoize_default=self._bootstrap_singleton_entity(
                "core__memoize_by_default"
            ),
        )

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
        and internal entities.
        """

        return [
            entity_dnode_from_descriptor(entity_name)
            for entity_name in self._flow_state.entity_defs_by_name.keys()
        ]

    def _obtain_provider_for_dnode(self, dnode):
        """
        Returns a Provider object for a given descriptor node -- either by finding
        a statically precomputed provider, or automatically generating one.
        """

        assert self._static_providers_by_dnode is not None
        if dnode in self._static_providers_by_dnode:
            return self._static_providers_by_dnode[dnode]

        if isinstance(dnode, ast.EntityNode):
            entity_name = dnode.to_entity_name()
            if entity_name in self._flow_state.entity_defs_by_name:
                message = f"""
                Unexpected failed to find a static provider for defined entity
                {entity_name!r};
                this should be impossible!
                """
                raise AssertionError(oneline(message))

            raise UndefinedEntityError.for_name(entity_name)

        elif isinstance(dnode, ast.TupleNode):
            return TupleConstructionProvider(dnode)

        else:
            raise AssertionError(
                f"Unexpected dnode type {type(dnode)!r} for dnode {dnode!r}"
            )

    def _obtain_entity_def_for_dnode(self, dnode):
        """
        Returns an EntityDefinition object for a given descriptor node. If the node is
        an entity, this returns the definition specified by the user; if the node is a
        tuple, we generate a synthetic definition.
        """

        if isinstance(dnode, ast.EntityNode):
            return self._flow_state.get_entity_def(dnode.to_descriptor())

        elif isinstance(dnode, ast.TupleNode):
            # TODO Since we're using this to describe things something that's not an
            # entity, we should rename `EntityDefinition` to something more general.
            return EntityDefinition(
                name=dnode.to_descriptor(),
                protocol=TupleProtocol(len(dnode.children)),
                doc=f"A Python tuple with {len(dnode.children)} values.",
                optional_should_memoize=True,
                can_persist=False,
            )

        else:
            raise AssertionError(
                f"Unexpected dnode type {type(dnode)!r} for dnode {dnode!r}"
            )

    def _get_or_create_dinfo_for_dnode(self, dnode):
        "Computes (and memoizes) a DescriptorInfo object for a descriptor node."

        if dnode in self._saved_dinfos_by_dnode:
            return self._saved_dinfos_by_dnode[dnode]

        provider = self._obtain_provider_for_dnode(dnode)

        dep_dnodes = provider.get_dependency_dnodes()
        dep_dinfos = [
            self._get_or_create_dinfo_for_dnode(dep_dnode) for dep_dnode in dep_dnodes
        ]
        dep_key_spaces_by_dnode = {
            dep_dinfo.dnode: dep_dinfo.key_space for dep_dinfo in dep_dinfos
        }
        dep_task_key_lists_by_dnode = {
            dep_dinfo.dnode: [
                task.key_for_dnode(dep_dinfo.dnode) for task in dep_dinfo.tasks
            ]
            for dep_dinfo in dep_dinfos
        }

        # TODO Maybe of having these two separate variables that we pass around, we
        # should just have a single method:
        #
        #     provider.get_dinfo(dep_dinfos_by_dnode)
        key_space = provider.get_key_space(dep_key_spaces_by_dnode)
        tasks = provider.get_tasks(
            dep_key_spaces_by_dnode, dep_task_key_lists_by_dnode,
        )
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

        # TODO We could have cached this in the DescriptorInfo object, but for now it's
        # not expensive to just recompute it, so we'll just do that. However, we could
        # also remove the need for this altogether: at this point the only thing a
        # TaskState needs the provider for is to look up function metadata
        # (code_fingerprint and changes_per_run), so we could extract that into a
        # separate class and attach it either to the TaskState or to the Task itself.
        provider = self._obtain_provider_for_dnode(dnode)

        # All keys in this task should have the same case key, so the set below
        # should have exactly one element.
        (case_key,) = set(task_key.case_key for task_key in task.keys)

        entity_defs_by_dnode = {
            task_key.dnode: self._obtain_entity_def_for_dnode(task_key.dnode)
            for task_key in task.keys
        }

        task_state = TaskState(
            task=task,
            dep_states=dep_states,
            case_key=case_key,
            provider=provider,
            entity_defs_by_dnode=entity_defs_by_dnode,
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

        dnode = entity_dnode_from_descriptor(entity_name)
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
        (result,) = result_group
        if result.value_is_missing:
            raise ValueError(
                oneline(
                    f"""
                Bootstrap entity {entity_name!r} could not be computed because
                the following entities are declared but not set:
                {", ".join(result.query.case_key.missing_names)}
                """
                )
            )
        return result.value

    def _compute_result_group_for_dnode(self, dnode):
        """
        Computes all results for a descriptor node. Will recursively compute any
        dependencies for that node as well.
        """

        dinfo = self._get_or_create_dinfo_for_dnode(dnode)
        requested_task_states = [
            self._get_or_create_task_state_for_key(task.keys[0]) for task in dinfo.tasks
        ]

        task_runner = TaskCompletionRunner(self._bootstrap, self._flow_instance_uuid)
        results_by_dnode_by_task_key = task_runner.run(requested_task_states)

        for state in requested_task_states:
            assert state.task.keys[0] in results_by_dnode_by_task_key

        return ResultGroup(
            results=[
                results_by_dnode_by_task_key[state.task.keys[0]][dnode]
                for state in requested_task_states
            ],
            key_space=dinfo.key_space,
        )


@attr.s(frozen=True)
class Bootstrap:
    """
    Set of entity values needed to compute / load other entities.
    """

    persistent_cache = attr.ib()
    versioning_policy = attr.ib()
    executor = attr.ib()
    should_memoize_default = attr.ib()


class TaskKeyLogger:
    """
    Logs how we derived each task key. The purpose of this class is to make sure that
    each task key used in a derivation (i.e., a call to `Flow.get()`) is logged exactly
    once. (One exception: a task key can be logged twice to indicate the start and end
    of a computation.)
    """

    def __init__(self, bootstrap):
        self._level = logging.INFO if bootstrap is not None else logging.DEBUG

        executor = bootstrap.executor if bootstrap is not None else None
        if executor is not None:
            self._already_logged_task_key_set = executor.create_synchronized_set()
        else:
            self._already_logged_task_key_set = SynchronizedSet()

    def _log(self, template, task_key, is_resolved=True):
        if not is_resolved:
            should_log = not self._already_logged_task_key_set.contains(task_key)
        else:
            should_log = self._already_logged_task_key_set.add(task_key)

        if should_log:
            logger.log(self._level, template, task_key)

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

    Attributes
    ----------
    dnode: DescriptorNode
        The descriptor this object refers to.
    key_space: CaseKeySpace
        Each of this descriptor's tasks' CaseKeys will have this key space.
    tasks_by_key: dict from TaskKey to Task
        All the tasks for this decriptor, organized by TaskKey.
    """

    dnode = attr.ib()
    key_space = attr.ib()
    tasks_by_key = attr.ib()

    @property
    def tasks(self):
        return self.tasks_by_key.values()


class EntryBlockage:
    """
    Represents a blocking relationship between a task state and a collection of
    not-yet-completed task keys it depends on.
    """

    def __init__(self, blocked_entry, blocking_tks):
        self.blocked_entry = blocked_entry
        self._blocking_tks = set(blocking_tks)

    def mark_task_key_complete(self, blocking_tk):
        self._blocking_tks.discard(blocking_tk)

    def is_resolved(self):
        return not self._blocking_tks


def compute_task_state(task_state, task_key_logger):
    task_state.compute(task_key_logger)
    return task_state.task.keys[0]


class TaskCompletionRunner:
    """
    Runs `TaskState` to completion.

    Using a `Bootstrap` object, this class completes given task states
    using a stack-based state tracking approach.
    """

    def __init__(self, bootstrap, flow_instance_uuid):
        # These are needed to complete entries.
        self._bootstrap = bootstrap
        self._flow_instance_uuid = flow_instance_uuid
        self.task_key_logger = TaskKeyLogger(bootstrap)

        # These are used for caching and tracking.
        self._entries_by_task_key = {}
        self._pending_entries = []
        self._in_progress_entries = {}
        self._blockage_lists_by_blocking_tk = defaultdict(list)

    def run(self, states):
        try:
            if self._bootstrap is not None and self._bootstrap.executor is not None:
                self._bootstrap.executor.start_logging()

            for state in states:
                entry = self._get_or_create_entry_for_state(state, is_requested=True)
                self._mark_entry_pending(entry)

            while self._has_pending_entries():
                entry = self._activate_next_pending_entry()

                # Before we decide if the current entry is blocked, we need to create
                # an entry for each of its dependencies. This has the side effect of
                # refreshing each of their cache states, so if any of them have had
                # their cache entries deleted, their status will be correctly updated
                # to "not complete".
                for dep_state in entry.state.dep_states:
                    self._get_or_create_entry_for_state(dep_state)

                if len(entry.state.blocking_dep_states()) > 0:
                    self._mark_entry_blocked(entry)
                    continue

                self._process_entry(entry)

            assert len(self._pending_entries) == 0
            assert len(self._in_progress_entries) == 0
            assert len(self._get_all_blocked_entries()) == 0

            return {
                task_key: entry.results_by_dnode
                for (task_key, entry) in self._entries_by_task_key.items()
                if entry.is_requested
            }

        finally:
            if self._bootstrap is not None and self._bootstrap.executor is not None:
                self._bootstrap.executor.stop_logging()

    def _process_entry(self, entry):
        assert entry.stage == EntryStage.ACTIVE

        if entry.state.is_cached:
            self._mark_entry_completed(entry)
            return

        state = entry.state
        # Initialize the task state before attempting to complete it.
        state.initialize(self._bootstrap, self._flow_instance_uuid)

        # Attempt to complete the task state from persistence cache.
        state.attempt_to_complete_from_cache()
        if state.is_cached:
            self._mark_entry_completed(entry)

        # If results aren't cached and the results needs to be returned,
        # we compute the results and store them in the runner.
        elif (
            entry.is_requested and not state.should_persist and not state.should_memoize
        ):
            results = state.compute(self.task_key_logger, return_results=True)
            entry.results_by_dnode = results
            self._mark_entry_completed(entry)

        # Compute the results serially.
        elif (
            # This is a bootstrap entity.
            self._bootstrap is None
            # Complete the task state serially.
            or self._bootstrap.executor is None
            # This is a non-serializable entity that needs to be returned.
            or (entry.is_requested and not state.should_persist)
            # This is a simple lookup task that looks up a value in a dictionary.
            # We can't run this in a separate process because the value may not be
            # cloudpicklable.
            or state.task.is_simple_lookup
        ):
            state.compute(self.task_key_logger)
            self._mark_entry_completed(entry)

        # Compute the results for serializable entity in parallel.
        elif state.should_persist:
            new_state_for_subprocess = state.strip_state_for_subprocess()
            future = self._bootstrap.executor.submit(
                compute_task_state, new_state_for_subprocess, self.task_key_logger,
            )
            self._mark_entry_in_progress(entry, future)

        # Do not compute non-serializable entity in parallel. Any entity that
        # depends on this entity will compute it.
        else:
            self._mark_entry_completed(entry)

    def _get_or_create_entry_for_state(self, state, is_requested=False):
        task_key = state.task.keys[0]
        if task_key not in self._entries_by_task_key:
            # Before doing anything with this task state, we should make sure its
            # cache state is up to date.
            state.refresh_all_persistent_cache_state(self._bootstrap)
            self._entries_by_task_key[task_key] = TaskRunnerEntry(
                state=state, is_requested=is_requested
            )
        return self._entries_by_task_key[task_key]

    def _has_pending_entries(self):
        # While there are no entries in the to-process stack but have any in-progress ones,
        # let's wait for in-progress entries to finish till we find an entry to process or
        # exhaust all in-progress entries.
        while len(self._pending_entries) == 0 and len(self._in_progress_entries) != 0:
            self._wait_on_in_progress_entries()
        return len(self._pending_entries) != 0

    def _activate_next_pending_entry(self):
        assert len(self._pending_entries) != 0
        next_entry = self._pending_entries.pop()
        assert next_entry.stage == EntryStage.PENDING

        next_entry.stage = EntryStage.ACTIVE
        return next_entry

    def _wait_on_in_progress_entries(self):
        "Waits on any in-progress entry to finish."
        futures = [entry.future for entry in self._in_progress_entries.values()]
        finished_futures, _ = wait(futures, return_when=FIRST_COMPLETED)
        for finished_future in finished_futures:
            task_key = finished_future.result()
            entry = self._entries_by_task_key[task_key]
            entry.state.sync_after_subprocess_completion()
            self._mark_entry_completed(entry)

    def _mark_entry_pending(self, pending_entry):
        assert pending_entry.stage in (EntryStage.NEW, EntryStage.BLOCKED)

        pending_entry.stage = EntryStage.PENDING
        self._pending_entries.append(pending_entry)

    def _mark_entry_blocked(self, blocked_entry):
        assert blocked_entry.stage == EntryStage.ACTIVE

        blocked_entry.stage = EntryStage.BLOCKED
        blocking_tks = blocked_entry.state.blocking_dep_task_keys()
        blockage = EntryBlockage(blocked_entry, blocking_tks)
        for blocking_tk in blocking_tks:
            self._blockage_lists_by_blocking_tk[blocking_tk].append(blockage)

        self._mark_blocking_entries_pending(blocked_entry)

    def _mark_entry_in_progress(self, in_progress_entry, future):
        assert in_progress_entry.stage == EntryStage.ACTIVE
        assert in_progress_entry.future is None
        assert in_progress_entry not in self._in_progress_entries

        in_progress_entry.stage = EntryStage.IN_PROGRESS
        in_progress_entry.future = future
        self._in_progress_entries[
            in_progress_entry.state.task.keys[0]
        ] = in_progress_entry

    def _mark_entry_completed(self, completed_entry):
        assert completed_entry.stage == EntryStage.ACTIVE or EntryStage.IN_PROGRESS

        if completed_entry.stage == EntryStage.IN_PROGRESS:
            completed_entry.future = None
            del self._in_progress_entries[completed_entry.state.task.keys[0]]

        if completed_entry.is_requested:
            if completed_entry.state.is_cached:
                results = completed_entry.state.get_cached_results(self.task_key_logger)
                completed_entry.results_by_dnode = results
            else:
                assert completed_entry.results_by_dnode is not None

        completed_entry.stage = EntryStage.COMPLETED
        self._unblock_entries(completed_entry)

    def _mark_blocking_entries_pending(self, blocked_entry):
        blocking_dep_states = blocked_entry.state.blocking_dep_states()
        for blocking_dep_state in blocking_dep_states:
            blocking_entry = self._get_or_create_entry_for_state(blocking_dep_state)
            if blocking_entry.stage == EntryStage.NEW:
                self._mark_entry_pending(blocking_entry)

    def _unblock_entries(self, completed_entry):
        for completed_tk in completed_entry.state.task.keys:
            affected_blockages = self._blockage_lists_by_blocking_tk[completed_tk]
            for blockage in affected_blockages:
                blockage.mark_task_key_complete(completed_tk)
                if (
                    blockage.is_resolved()
                    and blockage.blocked_entry.stage == EntryStage.BLOCKED
                ):
                    self._mark_entry_pending(blockage.blocked_entry)

    def _get_all_blocked_entries(self):
        return {
            blockage.blocked_entry
            for blockages in self._blockage_lists_by_blocking_tk.values()
            for blockage in blockages
            if not blockage.is_resolved()
        }


class TaskRunnerEntry:
    """
    Basic unit of `TaskCompletionRunner` that contains the data for
    `TaskState` execution and tracking.
    """

    def __init__(self, state, is_requested):
        self.state = state
        self.stage = EntryStage.NEW
        self.future = None
        self.is_requested = is_requested
        self.results_by_dnode = None


class EntryStage(Enum):
    """
    Represents the stage of a `TaskRunnerEntry`.
    """

    """
    Entry was just created.
    This is the always the first stage for an entry.
    Valid next stages: [PENDING]
    """
    NEW = auto()

    """
    Entry is waiting to be processed.
    Valid next stages: [ACTIVE]
    """
    PENDING = auto()

    """
    We are actively attempting to start running this entry.
    There should only be one such entry at a time. Any active entry
    running concurrently should be moved to IN_PROGRESS.
    Valid next stages: [BLOCKED, IN_PROGRESS, COMPLETED]
    """
    ACTIVE = auto()

    """
    Entry is blocked by another entry(ies).
    Valid next stages: [PENDING]
    """
    BLOCKED = auto()

    """
    Entry is currently running in another process.
    Valid next stages: [COMPLETED]
    """
    IN_PROGRESS = auto()

    """
    Entry has been successfully processed. This is a terminal stage.
    """
    COMPLETED = auto()
