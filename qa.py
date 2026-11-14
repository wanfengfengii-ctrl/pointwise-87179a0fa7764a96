"""Avro OCF quality gate.

Two entry points:
  python3 qa.py verify   - rerun regression, frame-oracle cross checks,
                           fixed-seed legal/corrupt boundary matrix, cache
                           isolation, two real code mutations and a stress
                           run; regenerates evidence.json and checks REPORT.md.
  python3 qa.py demo     - frame summary + production rows + one corruption
                           witness for a real container.

Only the Python standard library is used. Production sources are imported
for driving the importer and fixture; frame_oracle is fully independent.
"""
import io
import json
import os
import random
import shutil
import subprocess
import sys
import time
import tracemalloc
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import frame_oracle
from frame_oracle import OracleError
import fixture
from fixture import container, integer
from importer import Importer
from codec import DecodeError
from schemas import canonical

SEED = 20261114
EVIDENCE_PATH = ROOT / "evidence.json"
REPORT_PATH = ROOT / "REPORT.md"
ARTIFACT_DIR = ROOT / "qa_artifacts"
RUNTIME_PATH = ROOT / ".pointwise-runtime.json"

SYNC = b"pointwise-avro01"
FAIL_INPUT_DIR = ARTIFACT_DIR / "fail_inputs"

# --------------------------------------------------------------------------
# Local, self-contained byte builders. These encode containers for test
# construction only; frame_oracle never imports or calls any of this, and the
# encoder direction is the inverse of the oracle's reading logic.
# --------------------------------------------------------------------------

def vlen(b):
    return integer(len(b)) + b


def hand_header(schema_dict, negative_segment=False, sync=SYNC):
    """Build an OCF header with either a positive or negative metadata map."""
    canon = canonical(schema_dict).encode("utf-8")
    entries = (vlen(b"avro.schema") + vlen(canon)
               + vlen(b"avro.codec") + vlen(b"null"))
    out = bytearray(b"Obj\x01")
    if negative_segment:
        out += integer(-2) + integer(len(entries)) + entries
    else:
        out += integer(2) + entries
    out += integer(0) + sync
    return bytes(out)


def hand_block(payload, count, sync=SYNC):
    return integer(count) + vlen(payload) + sync


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------

class CaseFailure(Exception):
    pass


def rng(label):
    return random.Random("%s:%d" % (label, SEED))


def oracle_summary(blob):
    try:
        return {"ok": True, "frame": frame_oracle.inspect(blob)}
    except OracleError as exc:
        return {"ok": False, "code": exc.code, "offset": exc.offset}


def run_importer(blob, schema):
    imp = Importer()
    return imp.import_file(io.BytesIO(blob), schema), imp


def expect_decode_error(case_id, blob, schema, allow=()):
    """A corrupt input must raise DecodeError and never leak a prefix."""
    imp = Importer()
    try:
        rows = imp.import_file(io.BytesIO(blob), schema)
    except DecodeError as exc:
        return {"verdict": "rejected", "error": str(exc)}
    except Exception as exc:  # pragma: no cover - unexpected class is a QA error
        raise CaseFailure("%s: raised %s instead of DecodeError: %s"
                          % (case_id, type(exc).__name__, exc))
    raise CaseFailure("%s: corrupt input returned %d rows: %r"
                      % (case_id, len(rows), rows))


def save_blob(case_id, blob):
    FAIL_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = FAIL_INPUT_DIR / (case_id + ".bin")
    path.write_bytes(blob)
    return str(path.relative_to(ROOT))


def patch(blob, offset, value):
    data = bytearray(blob)
    data[offset] = value
    return bytes(data)


# --------------------------------------------------------------------------
# 1. Original regression, unchanged
# --------------------------------------------------------------------------

def step_regression():
    proc = subprocess.run([sys.executable, "checks.py"], cwd=str(ROOT),
                          capture_output=True, text=True)
    if proc.returncode != 0 or "AVRO_NORMAL_OK" not in proc.stdout:
        raise CaseFailure("checks.py regression failed: " + proc.stdout
                          + proc.stderr)
    return {"returncode": proc.returncode, "stdout": proc.stdout.strip()}


# --------------------------------------------------------------------------
# 2. Frame oracle cross checks with hand-built micro containers
# --------------------------------------------------------------------------

SCHEMA_NULL = {"type": "record", "name": "NullRec", "namespace": "qa",
               "fields": [{"name": "z", "type": "null"}]}
SCHEMA_STR = {"type": "record", "name": "StrRec", "namespace": "qa",
              "fields": [{"name": "s", "type": "string"}]}
SCHEMA_NEG = {"type": "record", "name": "NegMeta", "namespace": "qa",
              "fields": [{"name": "s", "type": "string"}]}


def step_oracle_cross():
    cases = []

    # Micro container 1: empty container, literal byte accounting.
    hdr = hand_header(SCHEMA_STR)
    # magic 4 + map count 1 + schema key(11+1 len) + schema val + codec ...
    # The exact total is asserted against the oracle instead of assumed.
    info = frame_oracle.inspect(hdr)
    if info["header_end"] != len(hdr) or info["blocks"]:
        raise CaseFailure("oracle micro empty header mismatch")
    if info["sync"] != SYNC.hex():
        raise CaseFailure("oracle micro empty sync mismatch")
    cases.append({"id": "ORC-EMPTY", "bytes": len(hdr),
                  "expected_header_end": len(hdr), "frame": info})

    # Micro container 2: two zero-byte payload blocks (all-null records).
    hdr_null = hand_header(SCHEMA_NULL)
    b1 = hand_block(b"", 1)
    b2 = hand_block(b"", 1)
    micro = hdr_null + b1 + b2
    info = frame_oracle.inspect(micro)
    expected = {
        "header_end": len(hdr_null),
        "block_1": {"start": len(hdr_null),
                    "payload_start": len(hdr_null) + 2,
                    "payload_end": len(hdr_null) + 2,
                    "end": len(hdr_null) + 2 + 16,
                    "count": 1},
        "block_2": {"start": len(hdr_null) + 2 + 16,
                    "end": len(micro), "count": 1},
    }
    if info["header_end"] != expected["header_end"]:
        raise CaseFailure("oracle micro header_end")
    if len(info["blocks"]) != 2:
        raise CaseFailure("oracle micro block count")
    f1, f2 = info["blocks"]
    if (f1["start"], f1["payload_start"], f1["payload_end"], f1["end"],
            f1["count"]) != (expected["block_1"]["start"],
                             expected["block_1"]["payload_start"],
                             expected["block_1"]["payload_end"],
                             expected["block_1"]["end"], 1):
        raise CaseFailure("oracle micro block1 spans: %r" % (f1,))
    if (f2["start"], f2["end"], f2["count"], f2["sync_ok"]) != (
            expected["block_2"]["start"], expected["block_2"]["end"], 1, True):
        raise CaseFailure("oracle micro block2 spans")
    # Production importer must accept the same hand-built container.
    rows, _ = run_importer(micro, SCHEMA_NULL)
    if rows != [{"z": None}, {"z": None}]:
        raise CaseFailure("oracle micro production rows mismatch")
    cases.append({"id": "ORC-NULL-PAYLOAD", "bytes": len(micro),
                  "expected": expected, "frame": info, "rows": rows})

    # Micro container 3: negative metadata map segment with one string block.
    hdr_neg = hand_header(SCHEMA_NEG, negative_segment=True)
    payload = vlen(b"hi")                # Avro string: length prefix + bytes
    body = hand_block(payload, 1)
    neg_blob = hdr_neg + body
    info = frame_oracle.inspect(neg_blob)
    if info["header_end"] != len(hdr_neg) or len(info["blocks"]) != 1:
        raise CaseFailure("oracle negative-segment framing")
    f0 = info["blocks"][0]
    if (f0["payload_start"], f0["payload_end"], f0["count"]) != (
            len(hdr_neg) + 2, len(hdr_neg) + 2 + len(payload), 1):
        raise CaseFailure("oracle negative-segment payload span")
    rows, _ = run_importer(neg_blob, SCHEMA_NEG)
    if rows != [{"s": "hi"}]:
        raise CaseFailure("oracle negative-segment production rows")
    cases.append({"id": "ORC-NEG-META", "bytes": len(neg_blob),
                  "header_end": len(hdr_neg), "frame": info, "rows": rows})

    # Micro container 4: sync marker bytes embedded inside a length-framed
    # payload; the oracle must not use sync scanning for framing.
    embedded = SYNC.decode("latin1")
    real = container(SCHEMA_STR, [{"s": embedded}], 1)
    info = frame_oracle.inspect(real)
    payload_span = real[info["blocks"][0]["payload_start"]:
                        info["blocks"][0]["payload_end"]]
    if SYNC not in payload_span:
        raise CaseFailure("oracle embedded-sync fixture lost marker")
    rows, _ = run_importer(real, SCHEMA_STR)
    if rows != [{"s": embedded}]:
        raise CaseFailure("oracle embedded-sync production rows")
    cases.append({"id": "ORC-EMBEDDED-SYNC", "bytes": len(real),
                  "frame": info, "rows": rows})

    # Oracle error classification, single-fault micro inputs.
    err_cases = _oracle_error_cases(hdr, SCHEMA_STR)
    for cid, blob, want_code, want_offset in err_cases:
        try:
            frame_oracle.inspect(blob)
            raise CaseFailure("%s: oracle accepted corrupt frame" % cid)
        except OracleError as exc:
            if exc.code != want_code or exc.offset != want_offset:
                raise CaseFailure(
                    "%s: oracle code/offset = %s/%d, want %s/%d"
                    % (cid, exc.code, exc.offset, want_code, want_offset))
        cases.append({"id": cid, "bytes": len(blob),
                      "expected_code": want_code,
                      "expected_offset": want_offset,
                      "observed": oracle_summary(blob)})
    return cases


def _oracle_error_cases(hdr, schema):
    real = container(schema, [{"s": "hi"}], 1)
    he = frame_oracle.inspect(real)["header_end"]
    canon = canonical(schema).encode("utf-8")

    cases = []
    # magic
    cases.append(("ORC-ERR-MAGIC", b"Obj\x02" + real[4:], "magic", 0))
    cases.append(("ORC-ERR-SHORT", b"Ob", "truncated", 2))
    cases.append(("ORC-ERR-ZERO", b"", "truncated", 0))
    # varint: 10-byte continuation / overflow at map count offset 4
    cases.append(("ORC-ERR-VARINT10",
                  b"Obj\x01" + bytes([0x80] * 10), "varint", 4))
    cases.append(("ORC-ERR-VARINTOF",
                  b"Obj\x01" + bytes([0xFF] * 9 + [0x80]), "varint", 4))
    # truncated varint at map count (first missing byte = len)
    cases.append(("ORC-ERR-VARINTTR", b"Obj\x01\x80", "truncated", 5))
    # length: negative metadata segment byte length
    cases.append(("ORC-ERR-LEN",
                  b"Obj\x01" + integer(-1) + integer(-1), "length", 5))
    # metadata: segment declares one more byte than it contains
    entries = (vlen(b"avro.schema") + vlen(canon)
               + vlen(b"avro.codec") + vlen(b"null"))
    over = (b"Obj\x01" + integer(-2) + integer(len(entries) + 1) + entries
            + integer(0) + SYNC)
    cases.append(("ORC-ERR-META-SEG", over, "metadata", 4))
    # metadata: duplicate key (positive segment); entry starts right after count
    dup = (b"Obj\x01" + integer(2) + vlen(b"avro.schema") + vlen(canon)
           + vlen(b"avro.schema") + vlen(canon) + integer(0) + SYNC)
    dup_entry_start = 4 + 1 + len(vlen(b"avro.schema")) + len(vlen(canon))
    cases.append(("ORC-ERR-META-DUP", dup, "metadata", dup_entry_start))
    # metadata: missing avro.schema -> map_start offset 4
    miss = (b"Obj\x01" + integer(1) + vlen(b"avro.codec") + vlen(b"null")
            + integer(0) + SYNC)
    cases.append(("ORC-ERR-META-MISS", miss, "metadata", 4))
    # metadata: invalid schema JSON
    badj = (b"Obj\x01" + integer(1) + vlen(b"avro.schema")
            + vlen(b"{bad") + integer(0) + SYNC)
    badj_start = 5
    cases.append(("ORC-ERR-META-JSON", badj, "metadata", badj_start))
    # header sync truncated: cut one byte off a complete header
    cases.append(("ORC-ERR-HDRSYNC", real[:he - 1], "truncated", he - 1))
    # block count non-positive
    cases.append(("ORC-ERR-COUNT", real[:he] + integer(0) + SYNC,
                  "count", he))
    # block length negative
    cases.append(("ORC-ERR-BLOCKLEN",
                  real[:he] + integer(1) + integer(-1) + SYNC,
                  "length", he + 1))
    # truncated inside block payload: first missing byte is the payload
    # start, which equals the full constructed blob length here.
    blocktr = real[:he] + integer(1) + integer(99)
    cases.append(("ORC-ERR-BLOCKTR", blocktr, "truncated", len(blocktr)))
    # sync mismatch at block tail
    bad_sync = bytearray(real)
    bad_sync[-1] ^= 0x01
    cases.append(("ORC-ERR-SYNC", bytes(bad_sync), "sync", len(real) - 16))
    return cases


# --------------------------------------------------------------------------
# 3. Fixed-seed legal matrix
# --------------------------------------------------------------------------

SCHEMA_RICH = {
    "type": "record", "name": "Rich", "namespace": "qa",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "n", "type": ["null", "int", "long"]},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
    ],
}


def _rich_rows(n, rnd):
    rows = []
    for i in range(n):
        branch = rnd.choice([0, 1, 2])
        if branch == 0:
            union = {"$type": "null", "$value": None}
        elif branch == 1:
            union = {"$type": "int", "$value": rnd.randint(-(1 << 31),
                                                           (1 << 31) - 1)}
        else:
            union = {"$type": "long", "$value": rnd.randint(-(1 << 62),
                                                            (1 << 62))}
        rows.append({"id": "id-%d" % i, "n": union,
                     "tags": ["t%d" % rnd.randint(0, 99)
                              for _ in range(rnd.randint(0, 4))]})
    return rows


def step_legal_matrix():
    cases = []
    rnd = rng("legal")

    def add(cid, schema, rows, block_rows):
        blob = container(schema, rows, block_rows)
        info = frame_oracle.inspect(blob)
        # Every legal container's frame must line up with real bytes.
        for blk in info["blocks"]:
            if blob[blk["end"] - 16:blk["end"]] != SYNC:
                raise CaseFailure("%s: frame end not on sync" % cid)
            if blob[info["header_end"]:blk["start"]] != b"" and blk[
                    "start"] < info["header_end"]:
                raise CaseFailure("%s: block overlaps header" % cid)
        got, _ = run_importer(blob, schema)
        if got != rows:
            raise CaseFailure("%s: rows mismatch" % cid)
        cases.append({"id": cid, "schema": schema, "rows": rows,
                      "block_rows": block_rows, "bytes": len(blob),
                      "frame": info, "expected_rows": rows,
                      "actual_rows": got, "patch": []})

    add("LEG-EMPTY", SCHEMA_STR, [], 32)
    add("LEG-SINGLE", SCHEMA_STR, [{"s": "only"}], 1)
    add("LEG-MULTI-1", SCHEMA_STR,
        [{"s": "a"}, {"s": "b"}, {"s": "c"}], 1)
    add("LEG-MULTI-2", SCHEMA_STR,
        [{"s": "a"}, {"s": "b"}, {"s": "c"}], 2)
    add("LEG-BLOCK32", SCHEMA_STR,
        [{"s": "x%d" % i} for i in range(40)], 32)
    add("LEG-DUP", SCHEMA_STR, [{"s": "dup"}, {"s": "dup"}], 1)
    rich_rows = _rich_rows(25, rnd)
    add("LEG-RICH", SCHEMA_RICH, rich_rows, 7)

    # Legal prefixes: truncation at header end and at every complete block
    # end must return exactly the prefix rows, never a corruption error.
    base = container(SCHEMA_STR, [{"s": "p0"}, {"s": "p1"}, {"s": "p2"}], 1)
    info = frame_oracle.inspect(base)
    cuts = [info["header_end"]] + [b["end"] for b in info["blocks"]]
    prefix_rows = []
    all_rows = [{"s": "p0"}, {"s": "p1"}, {"s": "p2"}]
    for idx, cut in enumerate(cuts):
        prefix = base[:cut]
        finfo = frame_oracle.inspect(prefix)
        got, _ = run_importer(prefix, SCHEMA_STR)
        want = all_rows[:len(finfo["blocks"])]
        if got != want:
            raise CaseFailure("LEG-PREFIX-%d rows mismatch" % idx)
        cases.append({"id": "LEG-PREFIX-%d" % idx, "schema": SCHEMA_STR,
                      "rows": all_rows, "block_rows": 1, "bytes": len(prefix),
                      "frame": finfo, "expected_rows": want,
                      "actual_rows": got, "patch": [],
                      "truncate_at": cut})
        prefix_rows = got
    if prefix_rows != all_rows:
        raise CaseFailure("full-length prefix case must equal all rows")

    # Fixture with negative metadata segment is still a legal container.
    hdr_neg = hand_header(SCHEMA_NEG, negative_segment=True)
    real = container(SCHEMA_NEG, [{"s": "n1"}, {"s": "n2"}], 1)
    he = frame_oracle.inspect(real)["header_end"]
    neg_blob = hdr_neg + real[he:]
    finfo = frame_oracle.inspect(neg_blob)
    got, _ = run_importer(neg_blob, SCHEMA_NEG)
    want = [{"s": "n1"}, {"s": "n2"}]
    if got != want:
        raise CaseFailure("LEG-NEGMETA rows mismatch")
    cases.append({"id": "LEG-NEGMETA", "schema": SCHEMA_NEG, "rows": want,
                  "block_rows": 1, "bytes": len(neg_blob), "frame": finfo,
                  "expected_rows": want, "actual_rows": got, "patch": []})
    return cases


# --------------------------------------------------------------------------
# 4. Fixed-seed corruption matrix
# --------------------------------------------------------------------------

def step_corrupt_matrix():
    cases = []
    blockers = []
    rnd = rng("corrupt")

    schema = SCHEMA_RICH
    rows = _rich_rows(12, rnd)
    blob = container(schema, rows, 4)
    info = frame_oracle.inspect(blob)
    he = info["header_end"]
    first = info["blocks"][0]
    second = info["blocks"][1]

    def record(cid, mutated, category, expect="DecodeError", note="",
               patches=None, own_schema=None):
        observed = oracle_summary(mutated)
        use_schema = own_schema or schema
        imp = Importer()
        try:
            got = imp.import_file(io.BytesIO(mutated), use_schema)
            actual = {"outcome": "rows", "rows": got}
        except DecodeError as exc:
            actual = {"outcome": "DecodeError", "error": str(exc)}
        except Exception as exc:
            actual = {"outcome": type(exc).__name__, "error": str(exc)}
        verdict = "pass"
        if expect == "DecodeError":
            if actual["outcome"] != "DecodeError":
                verdict = "blocker"
        elif actual != expect:
            verdict = "blocker"
        entry = {"id": cid, "category": category, "note": note,
                 "patch": patches or [], "oracle": observed,
                 "expected": expect, "actual": actual, "verdict": verdict,
                 "bytes": len(mutated)}
        if verdict == "blocker":
            entry["saved_input"] = save_blob(cid, mutated)
            blockers.append(cid)
        cases.append(entry)
        return entry

    # --- header region -----------------------------------------------------
    magic_bad = patch(blob, rnd.randrange(0, 4), 0xFF)
    record("COR-HDR-MAGIC", magic_bad, "header/magic",
           patches=[{"offset": _patched_offset(blob, magic_bad, 0, 4)}])

    # varint truncation inside the metadata map count region
    cut = he - 1
    record("COR-HDR-VARINT-TRUNC", blob[:cut], "header/varint-truncation",
           note="cut one byte before header sync",
           patches=[{"truncate": cut}])

    # varint overflow: replace map count with a 10-byte overflow
    ovf = b"Obj\x01" + bytes([0xFF] * 9 + [0x80])
    record("COR-HDR-VARINT-OVERFLOW", ovf, "header/varint-overflow",
           patches=[{"offset": 4, "bytes": ovf[4:].hex()}])

    # negative metadata segment length
    neg = (b"Obj\x01" + integer(-1) + integer(-5) + b"\x00" + SYNC
           + blob[he:])
    record("COR-HDR-NEG-LENGTH", neg, "header/negative-length",
           patches=[{"offset": 4, "bytes": (integer(-1) + integer(-5)).hex()}])

    # metadata segment consumption mismatch (negative segment overrun)
    canon = canonical(schema).encode("utf-8")
    entries = (vlen(b"avro.schema") + vlen(canon)
               + vlen(b"avro.codec") + vlen(b"null"))
    meta_over = (b"Obj\x01" + integer(-2) + integer(len(entries) + 2)
                 + entries + integer(0) + SYNC + blob[he:])
    record("COR-HDR-META-SEG", meta_over, "header/metadata-segment",
           patches=[{"offset": 5, "declared_size": len(entries) + 2,
                     "actual_size": len(entries)}])

    # duplicate metadata key
    dup = (b"Obj\x01" + integer(2) + vlen(b"avro.schema") + vlen(canon)
           + vlen(b"avro.schema") + vlen(canon) + integer(0) + SYNC
           + blob[he:])
    record("COR-HDR-META-DUP", dup, "header/metadata-duplicate",
           patches=[{"note": "second avro.schema entry"}])

    # schema JSON broken
    bad_json = (b"Obj\x01" + integer(1) + vlen(b"avro.schema")
                + vlen(b"{broken-json") + integer(0) + SYNC + blob[he:])
    record("COR-HDR-META-JSON", bad_json, "header/metadata-json",
           patches=[{"note": "avro.schema payload replaced by invalid JSON"}])

    # header sync mismatch
    hsync_bad = bytearray(blob)
    hsync_bad[he - 16] ^= 0x01
    record("COR-HDR-SYNC", bytes(hsync_bad), "header/sync",
           patches=[{"offset": he - 16, "xor": 1}])

    # --- first block region ------------------------------------------------
    # illegal non-positive block count
    bad_count = blob[:he] + integer(0) + blob[he + 1:]
    record("COR-B1-COUNT", bad_count, "block/count",
           patches=[{"offset": he, "from": blob[he], "to": bad_count[he]}])

    # negative declared payload length
    size_off = first["payload_start"] - 1
    bad_size = blob[:size_off] + integer(-7) + blob[first["payload_start"]:]
    record("COR-B1-NEG-SIZE", bad_size, "block/negative-length",
           patches=[{"offset": size_off, "bytes": integer(-7).hex()}])

    # declared length / record count mismatch: shrink payload length by one
    orig_size = first["payload_end"] - first["payload_start"]
    bad_len = (blob[:size_off] + integer(orig_size - 1)
               + blob[first["payload_start"]:])
    record("COR-B1-SIZE-MISMATCH", bad_len, "block/length-mismatch",
           patches=[{"offset": size_off, "declared": orig_size - 1,
                     "original": orig_size}])

    # payload shorter than declared records (count inflated, same framing)
    bad_rec_count = (blob[:first["start"]] + integer(first["count"] + 5)
                     + blob[first["start"] + 1:])
    record("COR-B1-COUNT-MISMATCH", bad_rec_count,
           "block/record-count-mismatch",
           patches=[{"offset": first["start"],
                     "declared_count": first["count"] + 5}])

    # block sync mismatch
    bsync_off = first["end"] - 16
    bsync_bad = patch(blob, bsync_off, blob[bsync_off] ^ 0xA5)
    record("COR-B1-SYNC", bsync_bad, "block/sync",
           patches=[{"offset": bsync_off, "xor": "A5"}])

    # mid-block truncation (inside payload span): must reject, no prefix
    cut = first["payload_start"] + 1
    record("COR-B1-TRUNC-PAYLOAD", blob[:cut], "block/truncated-payload",
           patches=[{"truncate": cut}])

    # mid-block truncation right before the block sync
    cut = first["end"] - 1
    record("COR-B1-TRUNC-SYNC", blob[:cut], "block/truncated-sync",
           patches=[{"truncate": cut}])

    # string UTF-8 corruption: find a string payload byte deterministically
    target = _find_string_byte(blob, info["blocks"][0], rnd)
    utf_bad = patch(blob, target, 0xFF)
    record("COR-B1-UTF8", utf_bad, "payload/utf8",
           patches=[{"offset": target, "to": "FF"}])

    # union index out of range: locate a union index byte in block 0
    uoff = _find_union_index(blob, info["blocks"][0], rows)
    if uoff is not None:
        u_bad = patch(blob, uoff, 0x7F)
        record("COR-B1-UNION", u_bad, "payload/union-index",
               patches=[{"offset": uoff, "to": "7F"}])

    # int 32-bit range: rewrite an int-branch value via a purpose-built blob
    int_schema, int_blob, int_off = _int_overflow_blob()
    record("COR-B1-INT-RANGE", int_blob, "payload/int-range",
           patches=[{"offset": int_off, "value": 2 ** 31}],
           own_schema=int_schema)

    # array segment: negative count with declared segment byte size overrun
    arr_schema, arr_blob, arr_patch = _array_segment_overrun()
    entry = record("COR-B1-ARRAY-SEG", arr_blob, "payload/array-segment",
                   patches=arr_patch, own_schema=arr_schema)

    # --- failure after a valid legal prefix (second block) -----------------
    b2_start = second["start"]
    bad2 = blob[:b2_start] + integer(0) + blob[b2_start + 1:]
    record("COR-B2-COUNT", bad2, "prefix-then/count",
           patches=[{"offset": b2_start, "to": bad2[b2_start]}])

    b2_sync = second["end"] - 16
    bad2s = patch(blob, b2_sync, blob[b2_sync] ^ 0x5A)
    record("COR-B2-SYNC", bad2s, "prefix-then/sync",
           patches=[{"offset": b2_sync, "xor": "5A"}])

    cut = second["payload_start"]
    record("COR-B2-TRUNC", blob[:cut], "prefix-then/truncation",
           patches=[{"truncate": cut}])

    # zero-byte payload with positive count must remain legal
    null_blob = container(SCHEMA_NULL, [{"z": None}, {"z": None}], 1)
    ninfo = frame_oracle.inspect(null_blob)
    got, _ = run_importer(null_blob, SCHEMA_NULL)
    if got != [{"z": None}, {"z": None}]:
        raise CaseFailure("COR-NULL-PAYLOAD regressed")
    cases.append({"id": "COR-NULL-PAYLOAD-LEGAL",
                  "category": "block/zero-byte-payload",
                  "oracle": {"ok": True, "frame": ninfo},
                  "expected": "DecodeError", "actual": {"outcome": "rows",
                  "rows": got}, "verdict": "pass", "bytes": len(null_blob),
                  "patch": [], "note": "positive count, zero-length payload"})
    return cases, blockers


def _patched_offset(original, mutated, lo, hi):
    for i in range(lo, hi):
        if original[i] != mutated[i]:
            return i
    return -1


def _find_string_byte(blob, block, rnd):
    """Pick a deterministic UTF-8 content byte inside the first block."""
    span = blob[block["payload_start"]:block["payload_end"]]
    candidates = [block["payload_start"] + i
                  for i, b in enumerate(span) if b >= 0x41 and b < 0x7B]
    if not candidates:
        raise CaseFailure("no string content byte found for UTF-8 case")
    return candidates[rnd.randrange(len(candidates))]


def _find_union_index(blob, block, rows):
    """The union is the second field; scan plausible small index bytes.

    Uses known row construction: index bytes are 0/1/2 at field boundaries.
    A deterministic search picks the first byte equal to a valid branch index
    whose replacement is guaranteed to change semantics.
    """
    start, end = block["payload_start"], block["payload_end"]
    for off in range(start, end):
        if blob[off] in (1, 2):
            return off
    return None


def _int_overflow_blob():
    schema = {"type": "record", "name": "IntRec", "namespace": "qa",
              "fields": [{"name": "n", "type": "int"}]}
    good = container(schema, [{"n": 1}], 1)
    he = frame_oracle.inspect(good)["header_end"]
    payload = integer(1 << 31)
    mutated = good[:he] + hand_block(payload, 1)
    return schema, mutated, he + 2


def _array_segment_overrun():
    """Construct the confirmed production deviation as a stable input.

    A single block is used so the missing negative-segment consumption
    check is not masked by a later block sync failure; production silently
    returns rows whose values were read across the segment boundary.
    """
    schema = {"type": "record", "name": "ArrRec", "namespace": "qa",
              "fields": [{"name": "t",
                          "type": {"type": "array", "items": "int"}}]}
    good = container(schema, [{"t": [1, 2]}], 1)
    info = frame_oracle.inspect(good)
    he = info["header_end"]
    # Block layout after he (verified byte-by-byte against the fixture):
    #   he+0 block count, he+1 outer payload length,
    #   he+2 negative array count (-2 -> varint 04),
    #   he+3 declared array segment byte size (2 for ints 02 04),
    #   then segment items, array end marker and the 16-byte block sync.
    size_off = he + 3
    if good[he + 2] != 4 or good[size_off] != 2:
        raise CaseFailure("array segment layout drift at offset %d"
                          % size_off)
    mutated = patch(good, size_off, 4)
    return schema, mutated, [
        {"offset": size_off, "from": good[size_off], "to": 4,
         "note": "negative array segment size overrun"}]


# --------------------------------------------------------------------------
# 5. Cache / failure isolation
# --------------------------------------------------------------------------

def step_cache_isolation():
    evidence = []
    schema = SCHEMA_RICH
    rnd = rng("cache")
    rows = _rich_rows(10, rnd)
    good = container(schema, rows, 3)
    info = frame_oracle.inspect(good)
    he = info["header_end"]
    first = info["blocks"][0]

    imp = Importer()

    def call(cid, blob, expect_outcome, note=""):
        before = (imp.hits, imp.misses)
        try:
            got = imp.import_file(io.BytesIO(blob), schema)
            outcome = {"outcome": "rows", "rows": got}
        except DecodeError as exc:
            outcome = {"outcome": "DecodeError", "error": str(exc)}
        after = (imp.hits, imp.misses)
        entry = {"id": cid, "note": note, "before": list(before),
                 "after": list(after), "outcome": outcome,
                 "expected": expect_outcome}
        evidence.append(entry)
        if outcome["outcome"] != expect_outcome:
            raise CaseFailure("%s: outcome %s != %s"
                              % (cid, outcome["outcome"], expect_outcome))
        return entry

    # Header failure: no plan lookup at all, counters unchanged.
    call("CACHE-HDR-FAIL", b"Obj\x09broken", "DecodeError",
         "magic failure must not touch plan counters")
    if (imp.hits, imp.misses) != (0, 0):
        raise CaseFailure("header failure changed counters")

    # First-block failure: one miss is retained, no plan rollback.
    cut = first["payload_start"] + 1
    call("CACHE-B1-FAIL", good[:cut], "DecodeError",
         "plan lookup happens before record reading")
    if (imp.hits, imp.misses) != (0, 1):
        raise CaseFailure("first-block failure counter mismatch")

    # A legal import on the same instance equals a cold-start result.
    cold = Importer().import_file(io.BytesIO(good), schema)
    call("CACHE-AFTER-FAIL", good, "rows",
         "retry after failure must equal cold start")
    got = evidence[-1]["outcome"]["rows"]
    if got != cold or got != rows:
        raise CaseFailure("post-failure rows differ from cold start")
    if (imp.hits, imp.misses) != (1, 1):
        raise CaseFailure("retry should hit the cached plan")

    # Block-tail sync failure after a valid prefix, then legal again.
    bsync = bytearray(good)
    bsync[first["end"] - 16] ^= 0x01
    call("CACHE-TAIL-FAIL", bytes(bsync), "DecodeError",
         "sync failure at block tail")
    call("CACHE-AFTER-TAIL", good, "rows",
         "legal read after tail failure")
    if evidence[-1]["outcome"]["rows"] != rows:
        raise CaseFailure("rows after tail failure not equal to expected")

    # Empty container still performs exactly one plan lookup.
    empty = container(schema, [])
    call("CACHE-EMPTY", empty, "rows", "empty container plan lookup")
    if evidence[-1]["outcome"]["rows"] != []:
        raise CaseFailure("empty container must return no rows")
    if (imp.hits, imp.misses) != (4, 1):
        raise CaseFailure("empty container lookup accounting wrong")

    # Same canonical content, different dict key order -> no new plan.
    reordered = {"fields": [
        {"type": f["type"], "name": f["name"]} for f in schema["fields"]],
        "namespace": schema["namespace"], "name": schema["name"],
        "type": schema["type"]}
    before = (imp.hits, imp.misses)
    got = imp.import_file(io.BytesIO(good), reordered)
    if got != rows:
        raise CaseFailure("reordered reader schema changed rows")
    if (imp.hits, imp.misses) != (before[0] + 1, before[1]):
        raise CaseFailure("canonical-equal schema created a new plan")
    evidence.append({"id": "CACHE-KEY-ORDER", "before": list(before),
                     "after": [imp.hits, imp.misses],
                     "outcome": {"outcome": "rows", "rows": got},
                     "expected": "rows"})

    # Swap file order on the same instance: results stay per-file.
    other_rows = list(reversed(rows))
    other = container(schema, other_rows, 5)
    r1 = imp.import_file(io.BytesIO(other), schema)
    r2 = imp.import_file(io.BytesIO(good), schema)
    if r1 != other_rows or r2 != rows:
        raise CaseFailure("file order swap leaked rows across calls")
    evidence.append({"id": "CACHE-SWAP",
                     "after": [imp.hits, imp.misses],
                     "outcome": {"outcome": "rows"},
                     "expected": "rows", "first_rows": len(r1),
                     "second_rows": len(r2)})

    # No stale rows after a failure: the next call returns only its own rows.
    call("CACHE-NO-STALE", good[:he + 1], "DecodeError",
         "truncated block count region")
    fresh = imp.import_file(io.BytesIO(other), schema)
    if fresh != other_rows:
        raise CaseFailure("stale rows observed after failed call")
    evidence.append({"id": "CACHE-NO-STALE-CHECK",
                     "after": [imp.hits, imp.misses],
                     "outcome": {"outcome": "rows", "rows": fresh},
                     "expected": "rows"})

    # clear_cache resets both counters and the plan store.
    imp.clear_cache()
    if (imp.hits, imp.misses) != (0, 0):
        raise CaseFailure("clear_cache did not reset counters")
    call("CACHE-CLEARED-MISS", good, "rows", "fresh miss after clear")
    if (imp.hits, imp.misses) != (0, 1):
        raise CaseFailure("cleared import did not re-miss")
    return evidence


# --------------------------------------------------------------------------
# 6. Real code mutations in isolated copies
# --------------------------------------------------------------------------

MUT_DIR = ARTIFACT_DIR / "mutations"

MUT_SKIP_SYNC = {
    "name": "skip-block-sync",
    "old": "        if take(stream, 16) != sync:\n            raise DecodeError(\"sync\")\n",
    "new": "        take(stream, 16)  # MUTANT: block sync verification removed\n",
}
MUT_SWALLOW_TAIL = {
    "name": "swallow-tail-decodeerror",
    "old": "            return [plan(row) for row in records(stream, writer, sync)]",
    "new": "            try:\n"
           "                return [plan(row) for row in records(stream, writer, sync)]\n"
           "            except DecodeError:\n"
           "                return []  # MUTANT: tail DecodeError swallowed\n",
    "extra_import": ("from codec import header, records\n",
                     "from codec import header, records, DecodeError\n"),
}

RUNNER_SOURCE = """
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
"""


def _apply_mutation(mut):
    work = MUT_DIR / mut["name"]
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    for name in ("schemas.py", "codec.py", "resolver.py", "importer.py",
                 "fixture.py", "samples.py", "checks.py"):
        shutil.copy2(ROOT / name, work / name)
    target = work / ("importer.py" if "importer" in mut["old"] or
                     mut["name"] == "swallow-tail-decodeerror" else "codec.py")
    text = target.read_text()
    if "extra_import" in mut:
        old_imp, new_imp = mut["extra_import"]
        if old_imp not in text:
            raise CaseFailure("mutation import anchor missing for "
                              + mut["name"])
        text = text.replace(old_imp, new_imp, 1)
    if mut["old"] not in text:
        raise CaseFailure("mutation anchor missing for " + mut["name"])
    target.write_text(text.replace(mut["old"], mut["new"], 1))
    (work / "mut_runner.py").write_text(RUNNER_SOURCE)
    return work


def _run_mutant(work, blob_path, schema):
    proc = subprocess.run(
        [sys.executable, "-I", "mut_runner.py", blob_path,
         json.dumps(schema)], cwd=str(work), capture_output=True, text=True)
    if proc.returncode != 0:
        return {"outcome": "runner-error", "stderr": proc.stderr.strip()}
    return json.loads(proc.stdout.strip())


def step_mutations():
    witnesses = []
    schema = SCHEMA_RICH
    rnd = rng("mutation")
    rows = _rich_rows(8, rnd)
    good = container(schema, rows, 3)
    info = frame_oracle.inspect(good)

    # Mutation 1: skip block sync verification.
    work1 = _apply_mutation(MUT_SKIP_SYNC)
    sync_bad = bytearray(good)
    target_block = info["blocks"][-1]
    sync_off = target_block["end"] - 16
    sync_bad[sync_off] ^= 0xC3
    p1 = save_blob("MUT-SKIP-SYNC-INPUT", bytes(sync_bad))
    shutil.copy2(ROOT / p1, work1 / "input.bin")
    # Pristine reference via the real importer (no mutated code on path).
    imp = Importer()
    try:
        imp.import_file(io.BytesIO(bytes(sync_bad)), schema)
        pristine = {"outcome": "rows"}
    except DecodeError as exc:
        pristine = {"outcome": "DecodeError", "error": str(exc)}
    mutant = _run_mutant(work1, "input.bin", schema)
    detected = (pristine["outcome"] == "DecodeError"
                and mutant["outcome"] == "rows")
    witnesses.append({
        "id": "MUT-SKIP-SYNC", "mutation": MUT_SKIP_SYNC["name"],
        "input": p1, "patch": [{"offset": sync_off, "xor": "C3"}],
        "pristine": pristine, "mutant": mutant,
        "business_detects_mutation": detected,
        "note": "mutant returns a successful batch despite bad block sync"})
    if not detected:
        raise CaseFailure("skip-sync mutation not detected by business rule")

    # Mutation 2: swallow tail DecodeError and return parsed prefix.
    work2 = _apply_mutation(MUT_SWALLOW_TAIL)
    last = info["blocks"][-1]
    cut = last["payload_start"] + 1
    truncated = good[:cut]
    p2 = save_blob("MUT-SWALLOW-TAIL-INPUT", truncated)
    shutil.copy2(ROOT / p2, work2 / "input.bin")
    imp = Importer()
    try:
        imp.import_file(io.BytesIO(truncated), schema)
        pristine2 = {"outcome": "rows"}
    except DecodeError as exc:
        pristine2 = {"outcome": "DecodeError", "error": str(exc)}
    mutant2 = _run_mutant(work2, "input.bin", schema)
    detected2 = (pristine2["outcome"] == "DecodeError"
                 and mutant2["outcome"] == "rows")
    witnesses.append({
        "id": "MUT-SWALLOW-TAIL", "mutation": MUT_SWALLOW_TAIL["name"],
        "input": p2, "patch": [{"truncate": cut}],
        "pristine": pristine2, "mutant": mutant2,
        "business_detects_mutation": detected2,
        "note": "mutant returns parsed prefix rows of a truncated file"})
    if not detected2:
        raise CaseFailure("swallow-tail mutation not detected")
    return witnesses


# --------------------------------------------------------------------------
# 7. Stress: long arrays, multiple blocks
# --------------------------------------------------------------------------

def step_stress():
    schema = {
        "type": "record", "name": "Stress", "namespace": "qa",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "vals", "type": {"type": "array", "items": "long"}},
            {"name": "label", "type": "string"},
        ],
    }
    rnd = rng("stress")
    n_rows = 2000
    array_len = 500
    rows = [{"id": i,
             "vals": [rnd.randint(-(1 << 40), (1 << 40))
                      for _ in range(array_len)],
             "label": "label-%04d" % i} for i in range(n_rows)]
    blob = container(schema, rows, 250)
    info = frame_oracle.inspect(blob)
    if sum(b["count"] for b in info["blocks"]) != n_rows:
        raise CaseFailure("stress block counts do not sum to row count")

    tracemalloc.start()
    t0 = time.perf_counter()
    imp = Importer()
    got = imp.import_file(io.BytesIO(blob), schema)
    elapsed = time.perf_counter() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    if got != rows:
        raise CaseFailure("stress rows mismatch")
    return {
        "id": "STRESS-LONG-ARRAY",
        "rows": n_rows,
        "blocks": len(info["blocks"]),
        "array_length": array_len,
        "input_bytes": len(blob),
        "elapsed_seconds": round(elapsed, 6),
        "peak_python_alloc_bytes": peak,
        "returned_rows": len(got),
        "frame": {"header_end": info["header_end"],
                  "last_block_end": info["blocks"][-1]["end"]},
        "seed": SEED,
    }


# --------------------------------------------------------------------------
# Evidence + report reconciliation
# --------------------------------------------------------------------------

def collect_evidence():
    evidence = {
        "seed": SEED,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "python": sys.version.split()[0],
        "regression": step_regression(),
        "oracle_cross": step_oracle_cross(),
        "legal_matrix": step_legal_matrix(),
        "corrupt_matrix": [],
        "blockers": [],
        "cache_isolation": [],
        "mutations": [],
        "stress": {},
    }
    corrupt, blockers = step_corrupt_matrix()
    evidence["corrupt_matrix"] = corrupt
    evidence["blockers"] = blockers
    evidence["cache_isolation"] = step_cache_isolation()
    evidence["mutations"] = step_mutations()
    evidence["stress"] = step_stress()
    return evidence


def reconcile_report(evidence):
    if not REPORT_PATH.exists():
        raise CaseFailure("REPORT.md is missing")
    text = REPORT_PATH.read_text(encoding="utf-8")
    referenced = set()
    import re
    # Strip wildcard mentions (e.g. "ORC-ERR-*") before extracting ids,
    # otherwise the wildcard punctuation would truncate a token mid-id.
    scan_text = re.sub(r"\b(?:ORC|LEG|COR|CACHE|MUT|STRESS)-[A-Z0-9-]*\*",
                       "", text)
    for token in re.findall(
            r"\b(?:ORC|LEG|COR|CACHE|MUT|STRESS)-[A-Z0-9]+(?:-[A-Z0-9]+)*",
            scan_text):
        referenced.add(token)
    known = set()
    for group in ("oracle_cross", "legal_matrix", "corrupt_matrix",
                  "cache_isolation", "mutations"):
        for item in evidence[group]:
            known.add(item["id"])
    known.add(evidence["stress"]["id"])
    missing = sorted(referenced - known)
    if missing:
        raise CaseFailure("REPORT.md references unknown case ids: %s" % missing)

    required_sections = ["源码边界", "取帧器", "覆盖依据", "失败隔离",
                         "变异检出", "资源实测", "剩余风险", "上线意见"]
    for section in required_sections:
        if section not in text:
            raise CaseFailure("REPORT.md missing section keyword: " + section)
    blocker_ids = evidence["blockers"]
    for bid in blocker_ids:
        if bid not in text:
            raise CaseFailure("REPORT.md does not mention blocker " + bid)
    if blocker_ids and "阻断" not in text:
        raise CaseFailure("REPORT.md must mark the blocking conclusion")
    if not blocker_ids and "阻断" in text and "无阻断" not in text:
        raise CaseFailure("REPORT.md claims blockers absent from evidence")
    return sorted(referenced)


def cmd_verify():
    ARTIFACT_DIR.mkdir(exist_ok=True)
    if FAIL_INPUT_DIR.exists():
        shutil.rmtree(FAIL_INPUT_DIR)
    FAIL_INPUT_DIR.mkdir(parents=True)
    evidence = collect_evidence()
    EVIDENCE_PATH.write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8")
    referenced = reconcile_report(evidence)
    evidence["report_referenced_ids"] = referenced
    EVIDENCE_PATH.write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8")

    n_corrupt = len(evidence["corrupt_matrix"])
    n_block = len(evidence["blockers"])
    print("regression: %s" % evidence["regression"]["stdout"])
    print("oracle cross cases: %d" % len(evidence["oracle_cross"]))
    print("legal cases: %d" % len(evidence["legal_matrix"]))
    print("corrupt cases: %d, blockers: %d" % (n_corrupt, n_block))
    print("cache checks: %d" % len(evidence["cache_isolation"]))
    print("mutation witnesses: %d" %
          len(evidence["mutations"]))
    st = evidence["stress"]
    print("stress: %d rows, %d blocks, %d bytes, %.6fs, peak %d bytes"
          % (st["rows"], st["blocks"], st["input_bytes"],
             st["elapsed_seconds"], st["peak_python_alloc_bytes"]))
    if evidence["blockers"]:
        print("BLOCKED: production deviation retained in evidence: %s"
              % evidence["blockers"])
        print("AVRO_FRAME_QA_OK")
        return 2
    print("AVRO_FRAME_QA_OK")
    return 0


def cmd_demo():
    schema = SCHEMA_RICH
    rnd = rng("demo")
    rows = _rich_rows(6, rnd)
    blob = container(schema, rows, 2)
    frame = frame_oracle.inspect(blob)
    imp = Importer()
    got = imp.import_file(io.BytesIO(blob), schema)
    bad = bytearray(blob)
    last = frame["blocks"][-1]
    bad[last["end"] - 16] ^= 0xC3
    try:
        Importer().import_file(io.BytesIO(bytes(bad)), schema)
        corruption = {"expected": "DecodeError", "actual": "rows"}
    except DecodeError as exc:
        corruption = {"expected": "DecodeError",
                      "actual": "DecodeError: " + str(exc)}
    out = {
        "container_bytes": len(blob),
        "frame_summary": frame,
        "production_rows": got,
        "corruption_witness": corruption,
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def main(argv):
    if len(argv) != 2 or argv[1] not in ("verify", "demo"):
        print("usage: python3 qa.py {verify|demo}", file=sys.stderr)
        return 64
    try:
        return cmd_verify() if argv[1] == "verify" else cmd_demo()
    except CaseFailure as exc:
        print("QA_FAILED: %s" % exc, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
