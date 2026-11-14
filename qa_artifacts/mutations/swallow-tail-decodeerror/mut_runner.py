
import io, json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from importer import Importer
schema = json.loads(sys.argv[2])
blob = open(sys.argv[1], "rb").read()
imp = Importer()
try:
    rows = imp.import_file(io.BytesIO(blob), schema)
    print(json.dumps({"outcome": "rows", "rows": rows}, ensure_ascii=False))
except Exception as exc:
    print(json.dumps({"outcome": type(exc).__name__, "error": str(exc)}))
