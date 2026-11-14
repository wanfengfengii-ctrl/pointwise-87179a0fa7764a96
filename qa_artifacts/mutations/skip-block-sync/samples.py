"""Two small versions; emit real Avro files with: python3 samples.py OUTDIR."""
import json
import sys
from pathlib import Path
from fixture import container

WRITER_V1 = {
    "type": "record", "name": "Reading", "namespace": "meter",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "raw", "type": "int"},
        {"name": "note", "type": ["null", "string"]}
    ]
}
WRITER_V2 = {
    "type": "record", "name": "Reading", "namespace": "meter",
    "fields": [
        {"name": "note", "type": ["string", "null"]},
        {"name": "id", "type": "string"},
        {"name": "value", "type": "long"},
        {"name": "tags", "type": {"type": "array", "items": "string"}}
    ]
}
READER = {
    "type": "record", "name": "Observation", "namespace": "meter",
    "aliases": ["Reading"],
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "value", "type": "long", "aliases": ["raw"]},
        {"name": "note", "type": ["null", "string"], "default": None},
        {"name": "tags", "type": {"type": "array", "items": "string"}, "default": []}
    ]
}
ROWS_V1 = [
    {"id": "a", "raw": 7, "note": {"$type": "null", "$value": None}},
    {"id": "b", "raw": -2, "note": {"$type": "string", "$value": "校准"}}
]
ROWS_V2 = [
    {"id": "c", "value": 12, "note": {"$type": "null", "$value": None}, "tags": ["ok"]},
    {"id": "d", "value": 0, "note": {"$type": "string", "$value": ""}, "tags": []}
]

def files():
    return [container(WRITER_V1, ROWS_V1, 1), container(WRITER_V2, ROWS_V2, 2)]

if __name__ == "__main__":
    target = Path(sys.argv[1])
    target.mkdir(parents=True, exist_ok=True)
    for i, content in enumerate(files(), 1):
        (target / ("v%d.avro" % i)).write_bytes(content)
    (target / "reader.json").write_text(json.dumps(READER, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"files": ["v1.avro", "v2.avro", "reader.json"]}))
