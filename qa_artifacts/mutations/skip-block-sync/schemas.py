"""Supported Avro schema graph; no reader/writer compatibility decisions."""
import json

class SchemaError(ValueError):
    pass

PRIMITIVES = {"null", "boolean", "int", "long", "string"}

def canonical(schema):
    return json.dumps(schema, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

class Schema:
    def __init__(self, raw):
        self.raw = json.loads(canonical(raw))
        self.names = {}
        self.root = self._build(self.raw, "", 0, 0)
        if self.root["type"] != "record":
            raise SchemaError("root must be record")
        def bounds(n, records=0, arrays=0):
            records += n["type"] == "record"
            arrays += n["type"] == "array"
            if records > 2 or arrays > 1:
                raise SchemaError("expanded depth")
            if n["type"] == "record":
                for f in n["fields"]:
                    bounds(f["schema"], records, arrays)
            elif n["type"] == "union":
                for b in n["branches"]:
                    bounds(b, records, arrays)
            elif n["type"] == "array":
                bounds(n["items"], records, arrays)
        bounds(self.root)

    def _build(self, raw, ns, depth, arrays):
        if isinstance(raw, str):
            if raw in PRIMITIVES:
                return {"type": raw}
            fullname = raw if "." in raw else (ns + "." + raw if ns else raw)
            if fullname not in self.names:
                raise SchemaError("forward/unknown name: " + fullname)
            return self.names[fullname]
        if isinstance(raw, list):
            if not raw:
                raise SchemaError("empty union")
            branches = [self._build(x, ns, depth, arrays) for x in raw]
            tags = [identity(x) for x in branches]
            if any(x["type"] in ("union", "array") for x in branches) or len(set(tags)) != len(tags):
                raise SchemaError("unsupported/duplicate union branch")
            return {"type": "union", "branches": branches}
        if not isinstance(raw, dict):
            raise SchemaError("invalid schema")
        typ = raw.get("type")
        if isinstance(typ, str) and typ in PRIMITIVES:
            if set(raw) != {"type"}:
                raise SchemaError("primitive attributes unsupported")
            return {"type": typ}
        if typ == "array":
            if arrays or set(raw) != {"type", "items"}:
                raise SchemaError("nested/invalid array")
            return {"type": "array", "items": self._build(raw["items"], ns, depth, arrays + 1)}
        if typ != "record" or depth >= 2:
            raise SchemaError("unsupported schema/depth")
        if set(raw) - {"type", "name", "namespace", "aliases", "fields"}:
            raise SchemaError("record attributes unsupported")
        name = raw.get("name")
        if not isinstance(name, str) or not name:
            raise SchemaError("record name")
        namespace = raw.get("namespace", ns)
        if not isinstance(namespace, str):
            raise SchemaError("namespace")
        fullname = name if "." in name else (namespace + "." + name if namespace else name)
        namespace = fullname.rpartition(".")[0]
        aliases = raw.get("aliases", [])
        if not isinstance(aliases, list) or any(not isinstance(x, str) or not x for x in aliases):
            raise SchemaError("record aliases")
        aliases = [x if "." in x else (namespace + "." + x if namespace else x) for x in aliases]
        fields = raw.get("fields")
        if not isinstance(fields, list) or fullname in self.names:
            raise SchemaError("fields/duplicate record")
        node = {"type": "record", "name": fullname, "aliases": aliases, "fields": []}
        self.names[fullname] = node
        seen = set()
        for f in fields:
            if not isinstance(f, dict) or set(f) - {"name", "type", "aliases", "default"}:
                raise SchemaError("field attributes")
            fname = f.get("name")
            fa = f.get("aliases", [])
            if not isinstance(fname, str) or not fname or fname in seen:
                raise SchemaError("field name")
            if not isinstance(fa, list) or any(not isinstance(x, str) or not x for x in fa):
                raise SchemaError("field aliases")
            seen.add(fname)
            ft = self._build(f.get("type"), namespace, depth + 1, arrays)
            node["fields"].append({"name": fname, "aliases": fa, "schema": ft,
                                   "has_default": "default" in f, "default": f.get("default")})
        # References to the enclosing record are forbidden.
        def reaches(n, target, visited):
            if n is target:
                return True
            if id(n) in visited:
                return False
            visited.add(id(n))
            if n["type"] == "record":
                return any(reaches(f["schema"], target, visited) for f in n["fields"])
            if n["type"] == "union":
                return any(reaches(b, target, visited) for b in n["branches"])
            return n["type"] == "array" and reaches(n["items"], target, visited)
        if any(reaches(f["schema"], node, set()) for f in node["fields"]):
            raise SchemaError("recursive schema")
        return node

def identity(node):
    return node["name"] if node["type"] == "record" else node["type"]
