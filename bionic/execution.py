"""
Contains the execution logic for deriving Entities.
"""

from .datatypes import Result

def complete_task_state(task_state, task_key_logger):
    """
    Ensures that a task state reaches completion -- i.e., that its results are
    available and can be retrieved. This can happen either by computing the task's
    values or by confirming that cached values already exist.
    """
    assert task_state.is_initialized
    assert not task_state.is_blocked
    assert not task_state.is_complete

    # See if we can load it from the cache.
    if task_state.provider.attrs.should_persist() and all(
        axr.can_load() for axr in task_state.cache_accessors
    ):
        # We only load the hashed result while completing task state
        # and lazily load the entire result when needed later.
        value_hashes_by_name = {}
        for accessor in task_state.cache_accessors:
            value_hash = accessor.load_result_value_hash()
            value_hashes_by_name[accessor.query.dnode.to_entity_name()] = value_hash

        task_state.result_value_hashes_by_name = value_hashes_by_name
    # If we cannot load it from cache, we compute the task state.
    else:
        _compute_task_state(task_state, task_key_logger)

    task_state.is_complete = True

def get_results_for_complete_task_state(task_state, task_key_logger):
    "Returns the results of an already-completed task state."

    assert task_state.is_complete

    if task_state._results_by_name:
        for task_key in task_state.task.keys:
            task_key_logger.log_accessed_from_memory(task_key)
        return task_state._results_by_name

    results_by_name = dict()
    for accessor in task_state.cache_accessors:
        result = accessor.load_result()
        task_key_logger.log_loaded_from_disk(result.query.task_key)

        # Make sure the result is saved in all caches under this exact
        # query.
        accessor.save_result(result)

        results_by_name[result.query.dnode.to_entity_name()] = result

    if task_state.provider.attrs.should_memoize():
        task_state._results_by_name = results_by_name

    return results_by_name

def _compute_task_state(task_state, task_key_logger):
    """
    Computes the values of a task state by running its task. Requires that all
    the task's dependencies are already complete.
    """
    task = task_state.task

    dep_results = [
        get_results_for_complete_task_state(
            dep_state, task_key_logger
        )[dep_key.dnode.to_entity_name()]
        for dep_state, dep_key in zip(task_state.dep_states, task.dep_keys)
    ]

    provider = task_state.provider

    if not task.is_simple_lookup:
        for task_key in task.keys:
            task_key_logger.log_computing(task_key)

    dep_values = [dep_result.value for dep_result in dep_results]

    values = task_state.task.compute(dep_values)
    assert len(values) == len(provider.attrs.names)

    for query in task_state.queries:
        if task.is_simple_lookup:
            task_key_logger.log_accessed_from_definition(query.task_key)
        else:
            task_key_logger.log_computed(query.task_key)

    results_by_name = {}
    result_value_hashes_by_name = {}
    for ix, (query, value) in enumerate(zip(task_state.queries, values)):
        query.protocol.validate(value)

        result = Result(query=query, value=value,)

        if provider.attrs.should_persist():
            accessor = task_state.cache_accessors[ix]
            accessor.save_result(result)

            value_hash = accessor.load_result_value_hash()
            result_value_hashes_by_name[query.dnode.to_entity_name()] = value_hash

        results_by_name[query.dnode.to_entity_name()] = result

    # Memoize results at this point only if results should not persist.
    # Otherwise, load it lazily later so that if the serialized/deserialized
    # value is not exactly the same as the original, we still
    # always return the same value.
    if provider.attrs.should_memoize() and not provider.attrs.should_persist():
        task_state._results_by_name = results_by_name

    # But we cache the hashed values eagerly since they are cheap to load.
    if provider.attrs.should_persist():
        task_state.result_value_hashes_by_name = result_value_hashes_by_name
