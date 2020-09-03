"""
Contains the core logic for resolving Entities by executing Tasks.
"""

import attr

from .datatypes import ResultGroup, EntityDefinition, TaskKey
from .descriptors.parsing import entity_dnode_from_descriptor
from .descriptors import ast
from .deps.optdep import import_optional_dependency
from .exception import UndefinedEntityError
from .core.flow_execution import TaskCompletionRunner, TaskKeyLogger
from .core.task_execution import TaskState
from .protocols import TupleProtocol
from .provider import TupleConstructionProvider, TupleDeconstructionProvider
from .utils.misc import oneline


def entity_is_internal(entity_name):
    "Indicates if an entity is built-in to Bionic rather than user-defined."
    return entity_name.startswith("core__")


# TODO Move EntityDeriver and related classes to `core` package.
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
            should_persist_default=self._bootstrap_singleton_entity(
                "core__persist_by_default"
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
                optional_should_persist=False,
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
            dep_key_spaces_by_dnode,
            dep_task_key_lists_by_dnode,
        )
        tasks_by_key = {
            task_key: task
            for task in tasks
            for task_key in task.keys
            if task_key.dnode == dnode
        }

        dinfo = DescriptorInfo(
            dnode=dnode,
            key_space=key_space,
            tasks_by_key=tasks_by_key,
        )

        self._saved_dinfos_by_dnode[dnode] = dinfo
        return dinfo

    def _get_or_create_task_state_for_key(
        self, task_key, in_progress_states_by_key=None
    ):
        """
        Constructs a TaskState for a task key. The TaskState is memoized, so subsequent
        requests for the same key are fast and always return the same object.
        """

        # First, check if we've already constructed this TaskState.
        if task_key in self._saved_task_states_by_key:
            return self._saved_task_states_by_key[task_key]

        # We'll also maintain a temporary cache of "in-progress" TaskStates, which have
        # been constructed but don't have all their fields fully populated. We want to
        # cache them to make sure every task key has exactly one task state, but we
        # don't want to put them in the real cache until they're fully initialized.
        if in_progress_states_by_key is None:
            in_progress_states_by_key = {}
        if task_key in in_progress_states_by_key:
            return in_progress_states_by_key[task_key]

        # Now we'll start by constructing several precursor objects.
        dnode = task_key.dnode
        dinfo = self._get_or_create_dinfo_for_dnode(dnode)
        task = dinfo.tasks_by_key[task_key]

        # All keys in this task should have the same case key, so the set below
        # should have exactly one element.
        (case_key,) = set(task_key.case_key for task_key in task.keys)

        # TODO We could have cached this in the DescriptorInfo object, but for now it's
        # not expensive to just recompute it, so we'll just do that. However, we could
        # also remove the need for this altogether: at this point the only thing a
        # TaskState needs the provider for is to look up function metadata
        # (code_fingerprint and changes_per_run), so we could extract that into a
        # separate class and attach it either to the TaskState or to the Task itself.
        provider = self._obtain_provider_for_dnode(dnode)
        func_attrs = provider.get_func_attrs(case_key)

        entity_defs_by_dnode = {
            task_key.dnode: self._obtain_entity_def_for_dnode(task_key.dnode)
            for task_key in task.keys
        }

        # With the precursors out of the way, we're ready to create the TaskState.
        # However, we're leaving the references to its neighboring task states empty
        # for now; the references can form loops, so there's no way to have all of them
        # initialized beforehand. We'll come back and add them later.
        task_state = TaskState(
            task=task,
            # We'll update these two lists later.
            dep_states=[],
            followup_states=[],
            case_key=case_key,
            func_attrs=func_attrs,
            entity_defs_by_dnode=entity_defs_by_dnode,
        )
        # We immediately put this TaskState in the "in-progress" cache so it will be
        # available as we recursively construct its neighbors.
        for state_task_key in task.keys:
            assert state_task_key not in in_progress_states_by_key
            in_progress_states_by_key[state_task_key] = task_state

        # Now we can recursively construct all of its dependency task states and add
        # references to them.
        for dep_key in task.dep_keys:
            dep_state = self._get_or_create_task_state_for_key(
                dep_key, in_progress_states_by_key
            )
            task_state.dep_states.append(dep_state)

        # Optionally, we will also add "follow-up" references:
        # If this task's output is a tuple node, we may want to add followups for each
        # of the tuple's children to ensure that everything in the tuple gets persisted
        # immediately.
        if (
            isinstance(dnode, ast.TupleNode)
            and provider.task_output_may_need_followups_for_persistence
        ):
            for child_dnode in dnode.children:
                followup_key = TaskKey(dnode=child_dnode, case_key=case_key)
                followup_state = self._get_or_create_task_state_for_key(
                    followup_key, in_progress_states_by_key
                )
                task_state.followup_states.append(followup_state)

        # Now the TaskState is fully initialized, so we can put it in the real cache
        # and return it.
        for state_task_key in task.keys:
            assert state_task_key not in self._saved_task_states_by_key
            self._saved_task_states_by_key[state_task_key] = task_state
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

        task_key_logger = TaskKeyLogger(self._bootstrap)
        task_runner = TaskCompletionRunner(
            bootstrap=self._bootstrap,
            flow_instance_uuid=self._flow_instance_uuid,
            task_key_logger=task_key_logger,
        )
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
    should_persist_default = attr.ib()

    def evolve(self, **kwargs):
        return attr.evolve(self, **kwargs)


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
