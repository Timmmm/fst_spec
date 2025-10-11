"""HDR block handler and parser
Parses the HDR block layout and writes a JSON with header fields.
"""

from .common import write_blob, ByteReader
import json
import os


def CallHDR(payload: bytes, idx: int, block_str: str, offset: int, output_dir: str):
    # payload is provided as bytes by the caller
    base_dir = output_dir
    payload_len = len(payload)

    if payload_len != 321:
        raise RuntimeError(f"CallHDR: payload size shall be 321, but got {payload_len}")

    # Use ByteReader to walk the payload sequentially
    br = ByteReader(payload)
    br.seek(0)
    start_time = br.read_u64()
    end_time = br.read_u64()
    real_endianness = br.read_double()
    writer_memory_use = br.read_u64()
    num_scopes = br.read_u64()
    num_hiearchy_vars = br.read_u64()
    num_vars = br.read_u64()
    num_vc_blocks = br.read_u64()
    timescale = br.read_i8()
    writer_bytes = br.read_bytes(128)
    date_bytes = br.read_bytes(26)
    reserved_bytes = br.read_bytes(321-228)
    filetype = br.read_u8()
    timezero = br.read_i64()

    assert br.tell() == 321, f"Bug: we do not seem to have consumed all 321 bytes"

    writer = writer_bytes.split(b"\x00", 1)[0].decode('utf-8', errors='ignore')
    date = date_bytes.split(b"\x00", 1)[0].decode('utf-8', errors='ignore')

    result = {
        'offset': offset,
        'start_time': start_time,
        'end_time': end_time,
        'real_endianness': real_endianness,
        'writer_memory_use': writer_memory_use,
        'num_scopes': num_scopes,
        'num_hiearchy_vars': num_hiearchy_vars,
        'num_vars': num_vars,
        'num_vc_blocks': num_vc_blocks,
        'timescale': timescale,
        'writer': writer,
        'date': date,
        'filetype': filetype,
        'timezero': timezero,
    }

    # write parsed HDR JSON
    write_blob(base_dir, idx, block_str, offset, payload_len, 0, "json", json.dumps(result, indent=2, ensure_ascii=False).encode('utf-8'))

    return
