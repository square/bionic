from .datatypes import (
    Artifact,
    Future,
    Provenance,
    Rule,
)


persisted_artifacts_by_provenance = {}


def persist_artifact(provenance, artifact):
    assert provenance not in persisted_artifacts_by_provenance
    print(f"SAVE {str(provenance):60s} : {artifact}")
    persisted_artifacts_by_provenance[provenance] = artifact
    return artifact


def load_persisted_artifact(provenance):
    artifact = persisted_artifacts_by_provenance.get(provenance)
    print(f"LOAD {str(provenance):60s} : {artifact}")
    return artifact


raw_base_rules_by_desc = {
    "X": Rule([], lambda: 2),
    "Y": Rule([], lambda: 3),
    "Z": Rule(["X", "Y"], lambda x, y: x + y),
}
base_rules_by_desc = {
    desc: rule.evolve(print_on_compute=True, duration=1)
    for desc, rule in raw_base_rules_by_desc.items()
}


def rule_for_desc(desc):
    if desc.startswith('<') and desc.endswith(">"):
        child_desc = desc[1:-1]
        return base_rules_by_desc[child_desc]
    
    elif '/' in desc:
        child_desc, name = desc.rsplit('/', 1)
        if name == 'provenance':
            return Rule(
                [
                    child_dep + "/digest"
                    for child_dep in rule_for_desc(child_desc).dep_descs
                ],
                lambda *dds: Provenance(child_desc, dds),
            )
        elif name == 'digest':
            if child_desc.endswith('/artifact'):
                return Rule([child_desc], lambda a: a.digest)
            else:
                return Rule([child_desc + "/provenance"], lambda p: p.digest)
        elif name == 'artifact':
            return Rule([desc + "/future"], lambda f: f.get())
        elif name == 'cached_artifact':
            return Rule([child_desc + '/derived_artifact/provenance'], lambda p: load_persisted_artifact(p))
        elif name == 'derived_artifact':
            return Rule(
                ['<' + child_desc + '>'],
                lambda v: Artifact(v),
            )
        elif name == 'future':
            if child_desc.endswith('/artifact'):
                grandchild_desc, _ = child_desc.split("/", 1)
                return Rule(
                    [
                        grandchild_desc + '/derived_artifact/provenance',
                        grandchild_desc + '/cached_artifact',
                        grandchild_desc + '/derived_artifact/future/function',
                    ],
                    lambda p, ca, daff: Future(ca) if ca is not None else daff().map(lambda a: persist_artifact(p, a)),
                )
            else:
                return Rule([child_desc], lambda v: v, present_as_future=True)
        elif name == 'function':
            return Rule([child_desc], lambda v: v, present_as_function=True)
        else:
            assert False, desc
    else:    
        return Rule([desc + '/artifact'], lambda a: a.value)


    
def future_for_desc(desc, memoized_values_by_desc=None, indent=""):
    if memoized_values_by_desc is None:
        memoized_values_by_desc = {}
    if desc in memoized_values_by_desc:
        return Future(memoized_values_by_desc[desc])
    
#     print(f"{indent} \\ {desc}")
    
    rule = rule_for_desc(desc)
    dep_futures = [
        future_for_desc(dep_desc)
        for dep_desc in rule.dep_descs
    ]
    deps_future = Future.join(dep_futures)
    value_future = deps_future.map(
        func=lambda dep_values: rule.func(*dep_values),
        duration=rule.duration,
    )

    def get_value():
        FIXME


    if rule.present_as_function:
        FIXME
    elif rule.present_as_future:
        
    else:
        value = rule.func()
        FIXME




    def get_value():
        if rule.print_on_compute:
            print("COMPUTE", desc)
        return rule.func(*(
            future_for_desc(dep_desc, memoized_values_by_desc, indent+" ").get()
            for dep_desc in rule.dep_descs
        ))
    
    if rule.present_as_function:
        assert not rule.present_as_future
        value = get_value
    elif rule.present_as_future:
        # FIXME This is not the correct duration, since it doesn't include
        # dependency durations.
        value = Future(get_value(), duration=rule.duration)
    else:
        value = get_value()
    
#     print(f"{indent} / {desc}")
    
    memoized_values_by_desc[desc] = value
    return Future(value)


def value_for_desc(desc):
    return future_for_desc(desc).get()



