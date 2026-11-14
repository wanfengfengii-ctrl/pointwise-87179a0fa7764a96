"""Reusable writer-to-reader resolution plans; no binary I/O."""
from copy import deepcopy
from schemas import identity

class ResolveError(ValueError):
    pass

def compatible(w, r):
    wt, rt = w["type"], r["type"]
    if wt == "record" and rt == "record":
        return w["name"] == r["name"] or w["name"] in r["aliases"]
    if wt == "array" and rt == "array":
        return compatible(w["items"], r["items"])
    if wt == "union":
        return any(compatible(x, r) for x in w["branches"])
    if rt == "union":
        return any(compatible(w, x) for x in r["branches"])
    return wt == rt or (wt == "int" and rt == "long")

def default_value(node, raw):
    typ = node["type"]
    if typ == "union":
        b = node["branches"][0]
        return {"$type": identity(b), "$value": default_value(b, raw)}
    if typ == "record":
        if not isinstance(raw, dict):
            raise ResolveError("bad record default")
        result = {}
        for f in node["fields"]:
            if f["name"] in raw:
                v = raw[f["name"]]
            elif f["has_default"]:
                v = f["default"]
            else:
                raise ResolveError("incomplete record default")
            result[f["name"]] = default_value(f["schema"], v)
        return result
    if typ == "array":
        if not isinstance(raw, list):
            raise ResolveError("bad array default")
        return [default_value(node["items"], x) for x in raw]
    ok = (typ == "null" and raw is None or
          typ == "boolean" and type(raw) is bool or
          typ in ("int", "long") and type(raw) is int and
          -(1 << (31 if typ == "int" else 63)) <= raw < (1 << (31 if typ == "int" else 63)) or
          typ == "string" and isinstance(raw, str))
    if not ok:
        raise ResolveError("bad default")
    return deepcopy(raw)

def compile_plan(w, r):
    wt, rt = w["type"], r["type"]
    if wt == "union":
        plans = {identity(b): compile_plan(b, r) for b in w["branches"]}
        return lambda value: plans[value["$type"]](value["$value"])
    if rt == "union":
        for branch in r["branches"]:
            if compatible(w, branch):
                plan = compile_plan(w, branch)
                tag = identity(branch)
                return lambda value: {"$type": tag, "$value": plan(value)}
        return rejected("no reader branch for " + identity(w))
    if not compatible(w, r):
        return rejected("incompatible " + identity(w) + " -> " + identity(r))
    if wt == "record":
        writer = w["fields"]
        fields = []
        for rf in r["fields"]:
            matches = [wf for wf in writer if wf["name"] == rf["name"] or wf["name"] in rf["aliases"]]
            if len(matches) > 1:
                fields.append((rf["name"], None, rejected("ambiguous field alias")))
            elif matches:
                wf = matches[0]
                fields.append((rf["name"], wf["name"], compile_plan(wf["schema"], rf["schema"])))
            elif rf["has_default"]:
                # Fresh materialization on every call prevents default sharing.
                fields.append((rf["name"], None,
                               lambda _, f=rf: default_value(f["schema"], f["default"])))
            else:
                fields.append((rf["name"], None, rejected("missing " + rf["name"])))
        def resolve_record(value):
            return {dst: fn(value[src] if src is not None else None) for dst, src, fn in fields}
        return resolve_record
    if wt == "array":
        item = compile_plan(w["items"], r["items"])
        return lambda value: [item(x) for x in value]
    return deepcopy

def rejected(message):
    def fail(_):
        raise ResolveError(message)
    return fail
