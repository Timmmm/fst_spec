"""BLACKOUT block handler
Parse payload: LEB128 count, then `count` entries of (1 byte state, LEB128 timestamp).
State: 0 -> off, 1 -> on. Produce a single JSON file with entries.
"""
from .common import write_blob, ByteReader
import json


def CallBLACKOUT(payload: bytes, idx: int, block_str: str, offset: int, output_dir: str):
    base_dir = output_dir
    payload_len = len(payload)

    result = {
        'offset': offset,
        'payload_len': payload_len,
        'entries': []
    }

    try:
        br = ByteReader(payload)
        br.seek(0)
        count, n = br.read_uleb128()
        result['count'] = count

        for i in range(count):
            if br.remaining() <= 0:
                raise RuntimeError(f'truncated entry {i}')
            state_b = br.read_u8()
            if state_b == 0:
                state = 'off'
            elif state_b == 1:
                state = 'on'
            else:
                state = f'unknown({state_b})'

            ts, m = br.read_uleb128()
            result['entries'].append({'state': state, 'timestamp': ts})

    except Exception as e:
        result['error'] = str(e)

    jbytes = json.dumps(result, ensure_ascii=False, indent=2).encode('utf-8')
    block_len = payload_len + 8  # FST block = 8 bytes header + payload
    write_blob(base_dir, idx, int(block_str), offset, block_len, 0, 'BLACKOUT.json', jbytes)

    return
