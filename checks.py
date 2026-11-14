"""Original business regression. QA work must retain these assertions."""
import io
import json
import sys
from importer import Importer
from samples import files, READER

def normal():
    obj = Importer()
    blobs = files()
    first = obj.import_file(io.BytesIO(blobs[0]), READER)
    assert first == [
        {"id": "a", "value": 7, "note": {"$type": "null", "$value": None}, "tags": []},
        {"id": "b", "value": -2, "note": {"$type": "string", "$value": "校准"}, "tags": []}
    ]
    second = obj.import_file(io.BytesIO(blobs[1]), READER)
    assert second == [
        {"id": "c", "value": 12, "note": {"$type": "null", "$value": None}, "tags": ["ok"]},
        {"id": "d", "value": 0, "note": {"$type": "string", "$value": ""}, "tags": []}
    ]
    assert obj.import_file(io.BytesIO(blobs[0]), READER) == first
    assert (obj.hits, obj.misses) == (1, 2)
    print("AVRO_NORMAL_OK")
    return first, second

if __name__ == "__main__":
    rows = normal()
    if len(sys.argv) > 1 and sys.argv[1] == "demo":
        print(json.dumps({"version1": rows[0], "version2": rows[1]}, ensure_ascii=False, sort_keys=True))
