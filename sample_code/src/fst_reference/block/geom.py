"""GEOM block handler
Parses geometry blocks: writes header JSON, decompressed binary, and decoded LEB128 values JSON.
"""

from .common import write_blob, ByteReader
import zlib
import json


def CallGEOM(payload: bytes, idx: int, block_str: str, offset: int, output_dir: str):
    base_dir = output_dir
    payload_len = len(payload)

    if payload_len < 16:
        raise RuntimeError(
            f"CallGEOM: payload size shall be at least 16, but got {payload_len}"
        )

    br = ByteReader(payload)
    # Read header fields (8B uncompressed_length + 8B count) using ByteReader
    uncompressed_length = br.read_u64()
    count = br.read_u64()
    data = br.read_bytes(br.remaining())

    is_uncompressed = payload_len == uncompressed_length + 16

    header_result = {
        "offset": offset,
        "payload_len": payload_len,
        "declared_uncompressed_length": uncompressed_length,
        "count": count,
        "is_uncompressed": bool(is_uncompressed),
    }

    dec = data if is_uncompressed else zlib.decompress(data)
    actual_uncompressed_length = len(dec)
    header_result["actual_uncompressed_length"] = actual_uncompressed_length
    if actual_uncompressed_length != uncompressed_length:
        raise RuntimeError(
            f"CallGEOM: declared uncompressed length {uncompressed_length} does not match actual {actual_uncompressed_length}"
        )

    jbytes = json.dumps(header_result, ensure_ascii=False, indent=2).encode("utf-8")
    write_blob(base_dir, idx, block_str, offset, payload_len, 0, "header.json", jbytes)
    write_blob(base_dir, idx, block_str, offset, payload_len, 0, "dec.bin", dec)

    values = []
    if dec:
        br2 = ByteReader(dec)
        while br2.remaining() > 0 and len(values) < count:
            v = br2.read_uleb128()[0]
            values.append(v)
        if len(values) != count:
            raise RuntimeError(
                f"CallGEOM: parsed {len(values)} values but expected {count}"
            )

    values_result = {
        "offset": offset,
        "count_expected": count,
        "values_parsed": len(values),
        "values": values,
    }
    vbytes = json.dumps(values_result, ensure_ascii=False, indent=2).encode("utf-8")
    write_blob(base_dir, idx, block_str, offset, payload_len, 1, "values.json", vbytes)
