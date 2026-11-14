"""Bundled writer-only Avro OCF fixture utility, fixture format revision 1.
No reader schema, resolution plan, expected result or external package is used.
"""
import json
from schemas import Schema, canonical, identity

def integer(n):
    if type(n) is not int or not -(1 << 63) <= n < (1 << 63):
        raise ValueError("long range")
    v = (n << 1) ^ (n >> 63)
    out = bytearray()
    while v > 127:
        out.append((v & 127) | 128)
        v >>= 7
    out.append(v)
    return bytes(out)

def byte_string(b):
    return integer(len(b)) + b

def datum(node, value):
    t = node["type"]
    if t == "null":
        if value is not None:
            raise ValueError("null")
        return b""
    if t == "boolean":
        if type(value) is not bool:
            raise ValueError("boolean")
        return bytes([value])
    if t in ("int", "long"):
        if type(value) is not int or t == "int" and not -(1 << 31) <= value < (1 << 31):
            raise ValueError("int")
        return integer(value)
    if t == "string":
        if not isinstance(value, str):
            raise ValueError("string")
        return byte_string(value.encode("utf-8"))
    if t == "record":
        if not isinstance(value, dict) or set(value) != {f["name"] for f in node["fields"]}:
            raise ValueError("writer fields must be explicit")
        return b"".join(datum(f["schema"], value[f["name"]]) for f in node["fields"])
    if t == "union":
        if not isinstance(value, dict) or set(value) != {"$type", "$value"}:
            raise ValueError("union tag")
        for i, branch in enumerate(node["branches"]):
            if identity(branch) == value["$type"]:
                return integer(i) + datum(branch, value["$value"])
        raise ValueError("unknown union tag")
    if not isinstance(value, list):
        raise ValueError("array")
    return (integer(len(value)) + b"".join(datum(node["items"], x) for x in value) if value else b"") + integer(0)

def container(writer_schema, rows, block_rows=32):
    if type(block_rows) is not int or block_rows <= 0:
        raise ValueError("block_rows")
    schema = Schema(writer_schema)
    meta = {"avro.schema": canonical(writer_schema).encode("utf-8"), "avro.codec": b"null"}
    sync = b"pointwise-avro01"
    out = bytearray(b"Obj\x01")
    out += integer(len(meta))
    for k, v in meta.items():
        out += byte_string(k.encode("utf-8")) + byte_string(v)
    out += integer(0) + sync
    for offset in range(0, len(rows), block_rows):
        batch = rows[offset:offset + block_rows]
        data = b"".join(datum(schema.root, row) for row in batch)
        out += integer(len(batch)) + byte_string(data) + sync
    return bytes(out)
