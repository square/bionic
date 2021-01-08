"""
Contains the core logic for resolving Entities by executing Tasks.
"""

from collections import defaultdict
import logging
import warnings

import attr

from .datatypes import ResultGroup, DescriptorMetadata, TaskKey, VersioningPolicy
from .descriptors.parsing import entity_dnode_from_descriptor
from .descriptors import ast
from .deps.optdep import import_optional_dependency
from .exception import UndefinedEntityError, UnavailableArtifactError
from .core.flow_execution import (
    ExecutionContext,
    MemoryResultCache,
    TaskCompletionRunner,
    TaskKeyLogger,
)
from .core.task_execution import TaskState
from .protocols import NonSerializableObjectProtocol, TupleProtocol
from .provider import (
    LoadingFromDiskProvider,
    TupleConstructionProvider,
    TupleDeconstructionProvider,
    ValidatingAndSavingToDiskProvider,
    ValidatingProvider,
)
from .utils.misc import oneline

logger = logging.getLogger(__name__)


NON_SERIALIZABLE_OBJECT_PROTOCOL = NonSerializableObjectProtocol()


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

    def __init__(self, flow_config, flow_instance_uuid):
        self._flow_config = flow_config
        self._flow_instance_uuid = flow_instance_uuid

        # These are set by _register_static_providers().
        # If we can precompute the appropriate provider for a dnode, we store it here.
        self._static_providers_by_dnode = None
        # For some dnodes (namely, those containing drafts), computing them should
        # cause us to automatically compute some additional dnodes.
        self._followup_dnode_lists_by_dnode = None
        # When computing a static provider for a persistable dnode, if we don't have
        # access to a persistent cache, we'll treat the dnode as non-persistable. We
        # track all such dnodes here so that we can warn the user if we end up using
        # them. (Normally we don't use them; after bootstrapping the execution core,
        # which contains the persistent cache, we re-register all the static providers.
        # The only time we need to warn is if a persistable dnode is needed during
        # this bootstrapping phase for some reason.)
        self._dnodes_forced_to_be_non_persistable = None

        # These are used to cache DescriptorInfo and TaskState objects, respectively.
        self._saved_dinfos_by_dnode = {}
        self._saved_task_states_by_key = {}

        # We need an ExecutionCore object to do any derivation, but creating the core
        # itself requires derivation. We'll use this default core to get us through the
        # initial bootstrapping phase, and then replace it with a properly configured
        # core.
        self._core = BOOTSTRAP_CORE
        # This indicates that everything above is propertly initalized and we're ready
        # for the `derive` method to be called.
        self._is_ready = False

    # TODO We should adjust the wording of the docstring below or refactor this a
    # little. It's not necessary for *the user* to call this method, but it is necessary
    # for it to get called before calling _compute_result_group_for_dnode(), which is
    # why derive() calls it.
    def get_ready(self):
        """
        Make sure this Deriver is ready to derive().  Calling this is not
        necessary but allows errors to surface earlier.
        """
        if self._is_ready:
            return

        # First, we need to set up the execution core. That requires deriving several
        # "core entities", which means we need to precompute our static providers.
        self._register_static_providers()
        self._set_up_final_core()

        # The way we configure our static providers depends on the core, so now that
        # we've set it up we'll need to re-generate our providers.
        self._register_static_providers()

        # Now everything should be set up; to check, we'll validate that all the basic
        # user-defined entities are derivable.
        self._prevalidate_base_dnodes()

        self._is_ready = True

    def derive(self, dnode):
        """
        Given a descriptor node, computes and returns a ResultGroup containing
        all values for that descriptor.
        """
        self.get_ready()
        return self._compute_result_group_for_dnode(dnode)

    def export_dag(self, include_core=False, _include_detail=False):
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

        # We only want to include certain nodes in the visualization: generally the ones
        # corresponding to user-defined functions and/or entities. (These are usually
        # the same, but not when a user function returns a more complex descriptor.) For
        # other nodes, we "collapse" them: we delete the node and connect each of its
        # parents to each of its children.
        def should_collapse_dnode(dnode):
            if not include_core:
                all_entities_are_core = all(
                    entity_is_internal(entity_name)
                    for entity_name in dnode.all_entity_names()
                )
                if all_entities_are_core:
                    return True

            if _include_detail:
                return False

            if dnode.is_entity():
                return False

            if dnode.is_draft() and not dnode.child.is_entity():
                return False

            return True

        self.get_ready()

        graph = nx.DiGraph()

        dnodes_to_add_to_graph = list(self._get_base_dnodes())
        dnodes_already_added = set()
        graph_nodes_to_collapse = set()
        while dnodes_to_add_to_graph:
            dnode = dnodes_to_add_to_graph.pop()
            if dnode in dnodes_already_added:
                continue

            tasks = self._get_or_create_dinfo_for_dnode(dnode).tasks
            descriptor = dnode.to_descriptor()
            doc = self._obtain_metadata_for_dnode(dnode).doc
            should_collapse = should_collapse_dnode(dnode)

            for task_ix, task in enumerate(
                sorted(tasks, key=lambda task: task.key.case_key)
            ):
                state = self._get_or_create_task_state_for_key(task.key)

                if len(tasks) == 1:
                    node_name = descriptor
                else:
                    node_name = f"{dnode.to_descriptor(near_commas=True)}[{task_ix}]"

                graph.add_node(
                    task.key,
                    name=node_name,
                    descriptor=descriptor,
                    case_key=task.key.case_key,
                    task_ix=task_ix,
                    doc=doc,
                )

                for dep_state in state.dep_states:
                    dep_task_key = dep_state.task.key
                    graph.add_edge(dep_task_key, task.key)
                    dnodes_to_add_to_graph.append(dep_task_key.dnode)

                if should_collapse:
                    graph_nodes_to_collapse.add(task.key)

            dnodes_already_added.add(dnode)

        for node in graph_nodes_to_collapse:
            for pred_node in graph.predecessors(node):
                for succ_node in graph.successors(node):
                    graph.add_edge(pred_node, succ_node)
            graph.remove_node(node)

        return graph

    # --- Private helpers.

    def _register_static_providers(self):
        """
        Precomputes an appropriate provider for each the following descriptor nodes:

        1. Any descriptor explicitly provided by a user-supplied function. (Note that
           these are always draft descriptors: if the user provides the descriptor
           "x, y", we will internally provide it as "<x, y>").
        2. Any descriptor which can be obtained by decomposing one of the first types of
           descriptors. (So if a user function provides the descriptor "<x, y>",
           we will also precompute providers for "<x>, <y>"; "<x>"; "x"; "<y>"; and
           "y".)

        These include every entity descriptor that can be computed, and every draft
        descriptor that we might need. Later, if we need any other descriptor, we'll
        generate the provider "just-in-time". Currently the only other providers we
        might need are tuples, which are easy to construct because they're composed
        of other descriptors. (For example, if we later want the descriptor "y, x",
        it's easy to generate the provider because we know that "x" and "y" must be
        precomputed. However, if we want an entity "z" that comes from a provided "x,
        (y, z)", that's hard to figure out without iterating over all known
        providers, which is why we precompute it instead.)

        The just-in-time computation happens in _obtain_provider_for_dnode().

        Currently, all of the providers of type 2 (above) act on descriptors that are
        or contain drafts. We mark these transformations as "followups", which means
        that anytime their input descriptor is computed, we immediately apply the
        transformation to compute the output descriptor as well. This chain reaction
        ends once we've generated all the individual entities in the descriptor,
        which are the only thing that we'll use in the future. (Draft descriptors are
        only used as intermediate values for these automatic transformations; they
        never get requested by any other part of the system.) The result is that
        every entity gets persisted and/or memoized as appropriate, and the
        intermediate draft values can be discarded.
        """

        static_providers_by_dnode = {}
        dnodes_forced_to_be_non_persistable = set()
        followup_dnode_lists_by_dnode = defaultdict(list)

        def register_followup(out_dnode, dep_dnode):
            followup_dnode_lists_by_dnode[dep_dnode].append(out_dnode)

        def register_provider(provider):
            dnode = provider.attrs.out_dnode
            assert dnode not in static_providers_by_dnode
            static_providers_by_dnode[dnode] = provider

            # If this provider generates a draft value ("<D>"), we will add another
            # provider that converts that draft value into the official value ("D").
            if dnode.is_draft():
                child_dnode = dnode.child

                # We don't allow drafts to be nested.
                if child_dnode.is_draft():
                    assert False

                # If we have a draft of an entity (like "<X>"), we want to validate the
                # value and possible convert it to a persistent artifact.
                elif child_dnode.is_entity():
                    entity_def = self._flow_config.entity_defs_by_name[child_dnode.name]
                    should_persist = self._entity_def_should_persist(entity_def)

                    # If we're in the bootstrapping phase, we don't have access to a
                    # persistent cache, so we need to treat all entities as
                    # non-persistable. Hopefully we don't actually need to use any of
                    # these entities until we've constructed the cache (at which point
                    # we'll recompute all our static providers) but if we do, we'll
                    # want to remember this and emit a warning.
                    if should_persist and self._core.persistent_cache is None:
                        dnodes_forced_to_be_non_persistable.add(child_dnode)
                        should_persist = False

                    # If the entity is persistable, we both validate and serialize the
                    # entity, like this:
                    #     <X> -> X/artifact -> X
                    if should_persist:
                        out_dnode = ast.GenericNode(child_dnode, "artifact")
                        provider = ValidatingAndSavingToDiskProvider(
                            out_dnode,
                            dnode,
                            entity_def.protocol,
                            self._core.persistent_cache,
                        )
                    # If the entity is not persistable, we just validate the value and
                    # return the regular entity:
                    #     <X> -> X
                    else:
                        out_dnode = child_dnode
                        provider = ValidatingProvider(
                            out_dnode,
                            dnode,
                            entity_def.protocol,
                        )
                    register_provider(provider)
                    register_followup(out_dnode, dnode)

                # If we have a draft of a tuple (like "<D1, D2>"), we "distribute" the
                # draft-ness over the childern (producing "<D1>, <D2>"). The idea is
                # that "<D1, D2>" is an unnormalized tuple containing unnormalized
                # values, while "<D1>, <D2>" is a normalized tuple containing
                # unnormalized values. (In the case of tuples, the normalization
                # doesn't actually do anything, but in the future it might; for
                # example, we might allow the unnormalized value to be a list, and then
                # normalize it by converting it to an actual tuple. TODO Let's actually
                # do this!)
                elif child_dnode.is_tuple():
                    out_dnode = ast.TupleNode(
                        ast.DraftNode(grandchild_dnode)
                        for grandchild_dnode in child_dnode.children
                    )
                    register_provider(
                        ValidatingProvider(
                            out_dnode,
                            dnode,
                            TupleProtocol(len(child_dnode.children)),
                        ),
                    )
                    register_followup(out_dnode, dnode)

                # We should never have a draft of an artifact, because user-defined
                # functions should never generate artifacts.
                elif child_dnode.is_generic():
                    raise AssertionError(
                        f"""
                        Encountered a draft of an artifact, {dnode.to_descriptor()!r},
                        which should be impossible.
                        """
                    )

                else:
                    dnode.fail_match()

            # We only have one type of generic: an artifact. The next step for an
            # artifact is to deserialize it into a regular in-memory entity.
            elif dnode.is_generic():
                assert dnode.name == "artifact"
                # If we don't have a persistent cache, we shouldn't be generating any
                # providers with artifact descriptors.
                assert self._core.persistent_cache is not None
                out_dnode = dnode.child
                entity_name = out_dnode.assume_entity().name
                entity_def = self._flow_config.entity_defs_by_name[entity_name]
                provider = LoadingFromDiskProvider(
                    out_dnode,
                    dnode,
                    entity_def.protocol,
                    self._core.persistent_cache,
                )
                register_provider(provider)
                # Once the artifact is computed, it will also be persisted to disk, so
                # we don't need a follow-up.

            # If this provider generates a tuple value ("D1, D2"), we will add
            # providers for extracting all the components ("D1" and "D2"). Note that
            # the components must be drafts, because the only situation where we
            # register a tuple provider is the "distribute" step above. (Remember that
            # if a user-provided function outputs a tuple, it will be wrapped in a
            # draft node and get handled by the case above first.)
            elif dnode.is_tuple():
                for child_dnode in dnode.children:
                    assert child_dnode.is_draft()
                    register_provider(TupleDeconstructionProvider(child_dnode, dnode))
                    register_followup(child_dnode, dnode)

            # The only remaining node type is an entity node, and there's no
            # additional work for us to do on those.
            elif dnode.is_entity():
                pass

            else:
                dnode.fail_match()

        for provider in set(self._flow_config.providers_by_name.values()):
            register_provider(provider)

        self._static_providers_by_dnode = static_providers_by_dnode
        self._dnodes_forced_to_be_non_persistable = dnodes_forced_to_be_non_persistable
        # We convert this from a defaultdict to a dict just to rule out any surprising
        # behavior downstream.
        self._followup_dnode_lists_by_dnode = dict(followup_dnode_lists_by_dnode)

    def _set_up_final_core(self):
        """
        Initializes some key objects needed to compute user-defined entities.
        """

        self._core = ExecutionCore(
            persistent_cache=self._compute_core_entity("core__persistent_cache"),
            versioning_policy=self._compute_core_entity("core__versioning_policy"),
            aip_executor=self._compute_core_entity("core__aip_executor"),
            process_executor=self._compute_core_entity("core__process_executor"),
            gcs_fs=self._compute_core_entity("core__persistent_cache__gcs__fs"),
            should_memoize_default=self._compute_core_entity(
                "core__memoize_by_default"
            ),
            should_persist_default=self._compute_core_entity(
                "core__persist_by_default"
            ),
            should_memoize_for_query_if_uncached=self._compute_core_entity(
                "core__temp_memoize_if_uncached"
            ),
            task_key_logging_level=logging.INFO,
        )

    def _prevalidate_base_dnodes(self):
        """
        Checks that all 'base' descriptors can be computed.

        (This precomputes and caches all the metadata required for each of these
        descriptors. If you don't call this, the same work will happen lazily later, so
        the only effect of this function is to cause any errors to be surfaced earlier.)
        """

        self._prevalidate_dnodes(self._get_base_dnodes())

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
            for entity_name in self._flow_config.entity_defs_by_name.keys()
        ]

    def _obtain_provider_for_dnode(self, dnode):
        """
        Returns a Provider object for a given descriptor node -- either by finding
        a statically precomputed provider, or automatically generating one.
        """

        assert self._static_providers_by_dnode is not None
        if dnode in self._static_providers_by_dnode:
            return self._static_providers_by_dnode[dnode]

        if dnode.is_entity():
            entity_name = dnode.assume_entity().name
            if entity_name in self._flow_config.entity_defs_by_name:
                message = f"""
                Unexpected failed to find a static provider for defined entity
                {entity_name!r};
                this should be impossible!
                """
                raise AssertionError(oneline(message))

            raise UndefinedEntityError.for_name(entity_name)

        elif dnode.is_draft():
            message = f"""
            A draft descriptor {dnode.to_descriptor()} was requested but is not
            statically provided; this should be impossible!
            """
            raise AssertionError(oneline(message))

        elif dnode.is_generic():
            assert dnode.name == "artifact"
            # If this provider is not statically registered, then the entity is not
            # persisted and we can't do this.
            raise UnavailableArtifactError(dnode)

        elif dnode.is_tuple():
            return TupleConstructionProvider(dnode)

        else:
            dnode.fail_match()

    def _obtain_metadata_for_dnode(self, dnode):
        """
        Returns metadata for the specified descriptor node.
        """

        if dnode.is_entity():
            entity_def = self._flow_config.get_entity_def(dnode.to_descriptor())

            # TODO It's a little gross that our metadata object depends on self._core,
            # which is different depending on whether we're in the bootstrapping phase.
            # It might be nice if we could explicitly mark each entity with whether
            # it's supposed to be configured before or after the core. However, that's
            # somewhat complicated by the fact that defining an entity with @builder
            # resets its entity configuration (which was probably a bad design choice
            # that we should change at some point).
            if entity_def.optional_should_memoize is not None:
                should_memoize = entity_def.optional_should_memoize
            else:
                should_memoize = self._core.should_memoize_default
            if entity_def.needs_caching and not should_memoize:
                # TODO Here we require that all non-deterministic values be memoized,
                # but it would probably also be okay if they were persisted instead; we
                # could change this check to only trigger if persistence is not
                # enabled.
                descriptor = dnode.to_descriptor()
                if self._core.should_memoize_default:
                    fix_message = (
                        "removing `memoize(False)` from the corresponding function"
                    )
                else:
                    fix_message = (
                        "applying `@memoize(True)` to the corresponding function"
                    )
                message = f"""
                Descriptor {descriptor!r} isn't configured to be memoized but
                is decorated with @changes_per_run. We will memoize it anyway:
                since @changes_per_run implies that this value can have a different
                value each time it’s computed, we need to memoize its value to make
                sure it’s consistent across the entire flow. To avoid this warning,
                enable memoization for the descriptor by {fix_message!r}."""
                warnings.warn(oneline(message))
                should_memoize = True

            should_persist = self._entity_def_should_persist(entity_def)
            should_memoize_for_query = (
                self._core.should_memoize_for_query_if_uncached
                and not (should_memoize or should_persist)
            )

            return DescriptorMetadata(
                doc=entity_def.doc,
                should_memoize=should_memoize,
                should_memoize_for_query=should_memoize_for_query,
            )

        elif dnode.is_draft():
            child_entity_def = self._obtain_metadata_for_dnode(dnode.child)
            # "Intermediate value" is a vague description, but we don't want to go into
            # detail on what a "draft" is, since they're really an implementation
            # detail. Users will generally only see this when they visualize a DAG
            # containing a tuple-generating function.
            doc_prefix = "(Intermediate value)"
            if child_entity_def.doc is None:
                doc = doc_prefix
            else:
                doc = doc_prefix + " " + child_entity_def.doc
            return DescriptorMetadata(
                doc=doc,
            )

        elif dnode.is_generic():
            assert dnode.name == "artifact"
            return DescriptorMetadata(
                doc="A file artifact persisted to disk.",
                is_artifact=True,
                # Artifact objects are small, so it would be nice if we could memoize
                # them, but they can also become invalidated between `get` calls, so
                # we don't.
            )

        elif dnode.is_tuple():
            return DescriptorMetadata(
                doc=f"A Python tuple with {len(dnode.children)} values.",
                is_composite=True,
            )

        else:
            dnode.fail_match()

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
            dep_dinfo.dnode: [task.key for task in dep_dinfo.tasks]
            for dep_dinfo in dep_dinfos
        }

        # TODO Maybe instead of having these two separate variables that we pass
        # around, we should just have a single method:
        #
        #     provider.get_dinfo(dep_dinfos_by_dnode)
        key_space = provider.get_key_space(dep_key_spaces_by_dnode)
        tasks = provider.get_tasks(
            dep_key_spaces_by_dnode,
            dep_task_key_lists_by_dnode,
        )
        tasks_by_key = {task.key: task for task in tasks}

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
        case_key = task.key.case_key

        # TODO We could have cached this in the DescriptorInfo object, but for now it's
        # not expensive to just recompute it, so we'll just do that. However, we could
        # also remove the need for this altogether: at this point the only thing a
        # TaskState needs the provider for is to look up function metadata
        # (code_fingerprint and changes_per_run), so we could extract that into a
        # separate class and attach it either to the TaskState or to the Task itself.
        provider = self._obtain_provider_for_dnode(dnode)
        func_attrs = provider.get_func_attrs(case_key, self._core.versioning_policy)

        metadata = self._obtain_metadata_for_dnode(task.key.dnode)

        # With the precursors out of the way, we're ready to create the TaskState.
        # However, we're leaving the references to its neighboring task states empty
        # for now; the references can form loops, so there's no way to have all of them
        # initialized beforehand. We'll come back and add them later.
        task_state = TaskState(
            task=task,
            # We'll update these two lists later.
            dep_states=[],
            followup_states=[],
            func_attrs=func_attrs,
            desc_metadata=metadata,
        )
        # We immediately put this TaskState in the "in-progress" cache so it will be
        # available as we recursively construct its neighbors.
        assert task.key not in in_progress_states_by_key
        in_progress_states_by_key[task.key] = task_state

        # Now we can recursively construct all of its dependency task states and add
        # references to them.
        for dep_key in task.dep_keys:
            dep_state = self._get_or_create_task_state_for_key(
                dep_key, in_progress_states_by_key
            )
            task_state.dep_states.append(dep_state)

        # Optionally, we will also add "follow-up" references:
        if dnode in self._followup_dnode_lists_by_dnode:
            for followup_dnode in self._followup_dnode_lists_by_dnode[dnode]:
                # Because all follow-ups have exactly one dependency, they always have
                # the same case key as their parent. (The assert below checks both of
                # these assumptions.)
                followup_key = TaskKey(dnode=followup_dnode, case_key=case_key)
                followup_state = self._get_or_create_task_state_for_key(
                    followup_key, in_progress_states_by_key
                )
                (dep_task_key,) = followup_state.task.dep_keys
                assert task_key == dep_task_key
                task_state.followup_states.append(followup_state)

        # We should not have followup tasks for any artifact value; we only have
        # followups for draft values (which are never artifacts).
        if metadata.is_artifact:
            assert len(task_state.followup_states) == 0

        # If we're in the bootstrapping phase, we may not be able to persist all the
        # entities we should.
        if dnode in self._dnodes_forced_to_be_non_persistable:
            descriptor = dnode.to_descriptor()
            message = f"""
            Descriptor {descriptor!r} is set to be persisted, but it can't be
            because core bootstrap entities depend on it.
            The corresponding value will not be serialized and deserialized,
            which may cause that value to be subtly different. To avoid this
            warning, disable persistence by applying `@persist(False)` or
            `@immediate` to the corresponding function, or passing
            `persist=False` when you `declare` or `assign` the entity values.
            """
            # TODO We should choose between `logger.warn` and `warnings.warn` and
            # use one consistently.
            logger.warn(oneline(message))

        # Now the TaskState is fully initialized, so we can put it in the real cache
        # and return it.
        assert task.key not in self._saved_task_states_by_key
        self._saved_task_states_by_key[task.key] = task_state
        return task_state

    def _entity_def_should_persist(self, entity_def):
        """Determines whether an entity should be persisted."""

        if entity_def.optional_should_persist is not None:
            return entity_def.optional_should_persist
        elif self._core.persistent_cache is not None:
            return self._core.should_persist_default
        else:
            return False

    def _compute_core_entity(self, entity_name):
        """
        Computes the value of a 'core' entity -- i.e., a fundamental internal entity
        needed to compute user-defined entities. Assumes the entity has a single
        value.
        """

        dnode = entity_dnode_from_descriptor(entity_name)
        result_group = self._compute_result_group_for_dnode(dnode)
        if len(result_group) == 0:
            raise ValueError(
                oneline(
                    f"""
                No values were defined for core entity {entity_name!r}"""
                )
            )
        if len(result_group) > 1:
            values = [result.value for result in result_group]
            raise ValueError(
                oneline(
                    f"""
                Core entity {entity_name!r} must have exactly one
                value; got {len(values)} ({values!r})"""
                )
            )
        (result,) = result_group
        if result.value_is_missing:
            raise ValueError(
                oneline(
                    f"""
                Core entity {entity_name!r} could not be computed because
                the following entities are declared but not set:
                {", ".join(result.task_key.case_key.missing_names)}
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
            self._get_or_create_task_state_for_key(task.key) for task in dinfo.tasks
        ]

        exec_context = ExecutionContext(
            flow_instance_uuid=self._flow_instance_uuid,
            core=self._core,
            task_key_logger=TaskKeyLogger(self._core),
            temp_result_cache=MemoryResultCache(),
        )
        task_runner = TaskCompletionRunner(exec_context)
        results = task_runner.run(requested_task_states)
        assert len(results) == len(requested_task_states)

        return ResultGroup(results=results, key_space=dinfo.key_space)


@attr.s(frozen=True)
class ExecutionCore:
    """
    A collection of parameters and services used to compute entity values.

    Most of components are themselves defined as Bionic entities; those entities are
    computed during a special "bootstrapping" phase, in which these components are each
    replaced with a default value.
    """

    persistent_cache = attr.ib()
    versioning_policy = attr.ib()
    aip_executor = attr.ib()
    process_executor = attr.ib()
    gcs_fs = attr.ib()
    should_memoize_default = attr.ib(type=bool)
    should_persist_default = attr.ib(type=bool)
    should_memoize_for_query_if_uncached = attr.ib(type=bool)
    task_key_logging_level = attr.ib()

    def evolve(self, **kwargs):
        return attr.evolve(self, **kwargs)


BOOTSTRAP_CORE = ExecutionCore(
    persistent_cache=None,
    versioning_policy=VersioningPolicy(
        check_for_bytecode_errors=False,
        treat_bytecode_as_functional=False,
        ignore_bytecode_exceptions=True,
    ),
    aip_executor=None,
    process_executor=None,
    gcs_fs=None,
    should_memoize_default=True,
    should_persist_default=False,
    should_memoize_for_query_if_uncached=False,
    task_key_logging_level=logging.DEBUG,
)


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
