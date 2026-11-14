"""Public importer. Instance-scoped plans are shared across files."""
import os
from contextlib import nullcontext
from schemas import Schema, canonical
from codec import header, records, DecodeError
from resolver import compile_plan

class Importer:
    def __init__(self):
        self._plans = {}
        self.hits = 0
        self.misses = 0

    def import_file(self, path, reader_schema):
        """path: file path or seekable binary stream, positioned at OCF start."""
        reader = Schema(reader_schema)
        manager = open(path, "rb") if isinstance(path, (str, bytes, os.PathLike)) else nullcontext(path)
        with manager as stream:
            writer, sync = header(stream)
            key = (canonical(writer.raw), canonical(reader.raw))
            if key in self._plans:
                self.hits += 1
                plan = self._plans[key]
            else:
                self.misses += 1
                plan = compile_plan(writer.root, reader.root)
                self._plans[key] = plan
            try:
                return [plan(row) for row in records(stream, writer, sync)]
            except DecodeError:
                return []  # MUTANT: tail DecodeError swallowed


    def clear_cache(self):
        self._plans.clear()
        self.hits = self.misses = 0

_default = Importer()
def import_file(path, reader_schema):
    return _default.import_file(path, reader_schema)
