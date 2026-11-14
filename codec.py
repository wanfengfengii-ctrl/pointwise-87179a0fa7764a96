"""Avro OCF, null codec, one or more blocks. Returns writer-side tagged data."""
import io
import json
from schemas import Schema, identity

class DecodeError(ValueError):
    pass

def take(stream, size):
    if size < 0:
        raise DecodeError("negative size")
    value = stream.read(size)
    if len(value) != size:
        raise DecodeError("truncated")
    return value

def long(stream):
    value = 0
    for shift in range(0, 70, 7):
        b = take(stream, 1)[0]
        value |= (b & 127) << shift
        if not b & 128:
            result = (value >> 1) ^ -(value & 1)
            if not -(1 << 63) <= result < (1 << 63):
                raise DecodeError("long range")
            return result
    raise DecodeError("long overflow")

def blob(stream):
    return take(stream, long(stream))

def text(stream):
    try:
        return blob(stream).decode("utf-8")
    except UnicodeDecodeError as e:
        raise DecodeError("utf8") from e

def datum(stream, node):
    typ = node["type"]
    if typ == "null":
        return None
    if typ == "boolean":
        b = take(stream, 1)[0]
        if b > 1:
            raise DecodeError("boolean")
        return bool(b)
    if typ in ("int", "long"):
        v = long(stream)
        if typ == "int" and not -(1 << 31) <= v < (1 << 31):
            raise DecodeError("int range")
        return v
    if typ == "string":
        return text(stream)
    if typ == "record":
        return {f["name"]: datum(stream, f["schema"]) for f in node["fields"]}
    if typ == "union":
        i = long(stream)
        if not 0 <= i < len(node["branches"]):
            raise DecodeError("union index")
        branch = node["branches"][i]
        return {"$type": identity(branch), "$value": datum(stream, branch)}
    out = []
    while True:
        count = long(stream)
        if count == 0:
            return out
        if count < 0:
            count = -count
            size = long(stream)
            block = io.BytesIO(take(stream, size))
            out.extend(datum(block, node["items"]) for _ in range(count))
            if block.read(1):
                raise DecodeError("array block size")
        else:
            out.extend(datum(stream, node["items"]) for _ in range(count))

def header(stream):
    if take(stream, 4) != b"Obj\x01":
        raise DecodeError("magic")
    meta = {}
    while True:
        count = long(stream)
        if not count:
            break
        block = stream
        if count < 0:
            count = -count
            block = io.BytesIO(take(stream, long(stream)))
        for _ in range(count):
            key = text(block)
            val = blob(block)
            if key in meta:
                raise DecodeError("duplicate metadata")
            meta[key] = val
        if block is not stream and block.read(1):
            raise DecodeError("metadata block size")
    if meta.get("avro.codec", b"null") != b"null":
        raise DecodeError("codec")
    try:
        raw = json.loads(meta["avro.schema"])
    except (KeyError, ValueError, UnicodeError) as e:
        raise DecodeError("schema metadata") from e
    return Schema(raw), take(stream, 16)

def records(stream, schema, sync):
    while True:
        pos = stream.tell()
        if not stream.read(1):
            return
        stream.seek(pos)
        count = long(stream)
        if count <= 0:
            raise DecodeError("record count")
        block = io.BytesIO(take(stream, long(stream)))
        for _ in range(count):
            yield datum(block, schema.root)
        if block.read(1):
            raise DecodeError("block size")
        if take(stream, 16) != sync:
            raise DecodeError("sync")
