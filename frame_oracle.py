"""Independent Avro OCF outer-frame inspector (quality oracle).

This module parses only the physical container framing: magic, metadata
header, block counts/lengths and sync markers. Record payloads are treated
as opaque byte spans and are never decoded.

It deliberately imports none of the production modules (schemas, codec,
resolver, importer, fixture) and no third-party Avro reader; all reading
logic below is self-contained.
"""

MAGIC = b"Obj\x01"
SYNC_SIZE = 16
LONG_BYTES_MAX = 10
INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1


class OracleError(Exception):
    """Raised when the outer frame is not a well-formed OCF container."""

    def __init__(self, code, offset, detail=""):
        self.code = code
        self.offset = offset
        super().__init__("%s at offset %d%s" % (code, offset,
                                                ": " + detail if detail else ""))


class _Reader:
    def __init__(self, blob):
        self.blob = blob
        self.pos = 0

    def need(self, size):
        """Read exactly size bytes or raise truncated at first missing byte."""
        start = self.pos
        end = start + size
        if end > len(self.blob):
            self.pos = len(self.blob)
            raise OracleError("truncated", len(self.blob),
                              "needed %d bytes at %d" % (size, start))
        chunk = self.blob[start:end]
        self.pos = end
        return chunk

    def varint(self):
        """Decode an Avro zig-zag long; field start offset is captured."""
        start = self.pos
        value = 0
        for i in range(LONG_BYTES_MAX):
            b = self.need(1)[0]
            value |= (b & 0x7F) << (7 * i)
            if not (b & 0x80):
                result = (value >> 1) ^ -(value & 1)
                if not INT64_MIN <= result <= INT64_MAX:
                    raise OracleError("varint", start, "long out of range")
                return result
        # Tenth byte still requests a continuation, or the value itself
        # cannot fit in a signed 64-bit integer.
        raise OracleError("varint", start, "more than 10 base-128 bytes")

    def sized_blob(self):
        """Read a length-prefixed byte string; negative lengths are length."""
        start = self.pos
        size = self.varint()
        if size < 0:
            raise OracleError("length", start, "negative byte string length")
        return self.need(size)

    def text(self, entry_start):
        try:
            return self.sized_blob().decode("utf-8")
        except UnicodeDecodeError as exc:
            raise OracleError("metadata", entry_start,
                              "key or value is not valid UTF-8") from exc


def _check_metadata_content(meta_spans, map_start):
    """Validate the avro.schema / avro.codec contract on collected spans."""
    keys = {k for k, _, _ in meta_spans}
    if "avro.schema" not in keys:
        raise OracleError("metadata", map_start, "missing avro.schema")
    for key, raw, entry_start in meta_spans:
        if key == "avro.schema":
            try:
                import json
                json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, ValueError) as exc:
                raise OracleError("metadata", entry_start,
                                  "avro.schema is not UTF-8 JSON") from exc
        elif key == "avro.codec" and raw != b"null":
            raise OracleError("metadata", entry_start,
                              "unsupported avro.codec")


def inspect(blob):
    """Return the outer-frame layout of a complete OCF byte string.

    Raises OracleError (with .code and .offset) when the outer frame is
    illegal. Payload bytes are framed, never interpreted.
    """
    if not isinstance(blob, (bytes, bytearray, memoryview)):
        raise TypeError("inspect expects bytes-like input")
    reader = _Reader(bytes(blob))

    magic_start = reader.pos
    if reader.need(4) != MAGIC:
        raise OracleError("magic", magic_start, "bad OCF magic")

    # Metadata map. Spans remember entry offsets for content errors.
    map_start = reader.pos
    spans = []
    while True:
        seg_start = reader.pos
        count = reader.varint()
        if count == 0:
            break
        block = reader
        if count < 0:
            count = -count
            size_start = reader.pos
            size = reader.varint()
            if size < 0:
                raise OracleError("length", size_start,
                                  "negative metadata segment size")
            seg_data_start = reader.pos
            block = _Reader(reader.need(size))
        for _ in range(count):
            entry_start = block.pos if block is not reader else reader.pos
            key = block.text(entry_start)
            value = block.sized_blob()
            if any(k == key for k, _, _ in spans):
                raise OracleError("metadata", entry_start,
                                  "duplicate metadata key")
            spans.append((key, value, entry_start))
        if block is not reader:
            if block.pos != len(block.blob):
                raise OracleError("metadata", seg_start,
                                  "segment did not consume declared length")
            reader.pos = seg_data_start + size
    _check_metadata_content(spans, map_start)

    sync_start = reader.pos
    header_sync = reader.need(SYNC_SIZE)

    blocks = []
    while reader.pos < len(reader.blob):
        block_start = reader.pos
        count_start = reader.pos
        count = reader.varint()
        if count <= 0:
            raise OracleError("count", count_start,
                              "block record count must be positive")
        size_start = reader.pos
        size = reader.varint()
        if size < 0:
            raise OracleError("length", size_start,
                              "negative block payload length")
        payload_start = reader.pos
        reader.need(size)
        payload_end = reader.pos
        block_sync_start = reader.pos
        block_sync = reader.need(SYNC_SIZE)
        blocks.append({
            "start": block_start,
            "count": count,
            "payload_start": payload_start,
            "payload_end": payload_end,
            "end": reader.pos,
            "sync_ok": block_sync == header_sync,
        })
        if block_sync != header_sync:
            raise OracleError("sync", block_sync_start,
                              "block sync differs from header sync")

    return {
        "header_end": sync_start + SYNC_SIZE,
        "sync": header_sync.hex(),
        "blocks": blocks,
    }
