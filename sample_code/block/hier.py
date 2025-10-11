"""Hierarchy block handlers
Handlers for hierarchy-related block types: HIER_GZ, HIER_LZ4, HIER_LZ4DUO.
Each handler writes a header JSON (00), the raw uncompressed binary (01) for debug, and a parsed JSON (02).
"""

from .common import write_blob
from . import hier_data
import struct
import json
import zlib
import gzip


def _u64(b, i):
    return struct.unpack('>Q', b[i:i+8])[0]


def _try_lz4_decompress(data, expected_size=None):
    """Try to decompress raw LZ4 block. Prefer lz4.block with uncompressed_size when available,
    fallback to lz4.frame if needed. Raises RuntimeError if no lz4 available."""
    # try lz4.block first
    try:
        import lz4.block as lz4block
        if expected_size is not None:
            try:
                return lz4block.decompress(data, uncompressed_size=expected_size)
            except Exception:
                return lz4block.decompress(data)
        else:
            return lz4block.decompress(data)
    except Exception:
        pass
    # fallback to lz4.frame
    try:
        import lz4.frame as lz4frame
        return lz4frame.decompress(data)
    except Exception:
        pass
    raise RuntimeError('lz4 decompression not available')


def _try_gzip_decompress(data):
    """Try gzip decompression, fallback to zlib. Raises RuntimeError on failure."""
    try:
        return gzip.decompress(data)
    except Exception:
        pass
    try:
        return zlib.decompress(data)
    except Exception:
        pass
    raise RuntimeError('gzip/zlib decompression failed')


def _write_hier_result(base_dir, block_idx, offset, payload_len, block_str, info, final_bytes):
    """Write header JSON, raw full binary for debug, and parsed JSON via hier_data.parse_hier_binary.
    Let parsing exceptions propagate.
    """
    jbytes = json.dumps(info, ensure_ascii=False, indent=2).encode('utf-8')
    write_blob(base_dir, block_idx, block_str, offset, payload_len, 0, 'header.json', jbytes)

    # always write raw uncompressed binary for debugging
    write_blob(base_dir, block_idx, block_str, offset, payload_len, 0, 'full.bin', final_bytes)

    # parse hierarchy binary and write JSON; let exceptions propagate
    parsed = hier_data.parse_hier_binary(final_bytes)
    pdata = json.dumps(parsed, ensure_ascii=False, indent=2).encode('utf-8')
    write_blob(base_dir, block_idx, block_str, offset, payload_len, 1, 'decoded.json', pdata)


def CallHIER_GZ(payload: bytes, idx: int, block_str: str, offset: int, output_dir: str):
    """Handle HIER_GZ: payload contains uncompressed_length (8B) followed by gzWrite-compressed data."""
    base_dir = output_dir
    payload_len = len(payload)
    if payload_len < 8:
        print(f"CallHIER_GZ: payload too small {payload_len}")
        return
    uncompressed_length = _u64(payload, 0)
    data = payload[8:]
    # let decompression errors propagate
    dec = _try_gzip_decompress(data)
    dec_ok = True

    info = {
        'offset': offset,
        'payload_len': payload_len,
        'declared_uncompressed_length': uncompressed_length,
        'actual_uncompressed_length': len(dec),
        'decompressed_ok': dec_ok,
    }

    block_len = payload_len + 8  # FST block = 8 bytes header + payload
    _write_hier_result(base_dir, idx, offset, block_len, block_str, info, dec)


def CallHIER_LZ4(payload: bytes, idx: int, block_str: str, offset: int, output_dir: str):
    """Handle HIER_LZ4: payload contains uncompressed_length (8B) followed by raw LZ4 block-compressed data."""
    base_dir = output_dir
    payload_len = len(payload)
    if payload_len < 8:
        print(f"CallHIER_LZ4: payload too small {payload_len}")
        return
    uncompressed_length = _u64(payload, 0)
    data = payload[8:]
    # let decompression errors propagate
    dec = _try_lz4_decompress(data, expected_size=uncompressed_length)
    dec_ok = True

    info = {
        'offset': offset,
        'payload_len': payload_len,
        'declared_uncompressed_length': uncompressed_length,
        'actual_uncompressed_length': len(dec),
        'decompressed_ok': dec_ok,
    }

    block_len = payload_len + 8  # FST block = 8 bytes header + payload
    _write_hier_result(base_dir, idx, offset, block_len, block_str, info, dec)


def CallHIER_LZ4DUO(payload: bytes, idx: int, block_str: str, offset: int, output_dir: str):
    """Handle HIER_LZ4DUO: payload contains uncompressed_length (8B), compressed_once_length (8B), then data compressed twice with raw LZ4 blocks (first round then second round)."""
    base_dir = output_dir
    payload_len = len(payload)
    if payload_len < 16:
        print(f"CallHIER_LZ4DUO: payload too small {payload_len}")
        return
    uncompressed_length = _u64(payload, 0)
    compressed_once_length = _u64(payload, 8)
    data = payload[16:]
    info = {
        'offset': offset,
        'payload_len': payload_len,
        'declared_uncompressed_length': uncompressed_length,
        'declared_compressed_once_length': compressed_once_length,
    }
    # First decompress LZ4 (outer layer) to get the first-round compressed buffer
    # let decompression errors propagate (outer and inner)
    after_lz4 = _try_lz4_decompress(data, expected_size=compressed_once_length)
    info['after_lz4_length'] = len(after_lz4)
    outer_lz4_ok = True

    final = _try_lz4_decompress(after_lz4, expected_size=uncompressed_length)
    inner_lz4_ok = True

    info['actual_uncompressed_length'] = len(final)
    info['outer_lz4_ok'] = outer_lz4_ok
    info['inner_lz4_ok'] = inner_lz4_ok
    info['uncompressed_length_match'] = (len(final) == uncompressed_length)

    _write_hier_result(base_dir, idx, offset, payload_len, block_str, info, final)
