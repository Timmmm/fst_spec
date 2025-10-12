import json
from typing import Any
import zlib
import lz4.block  # type: ignore
import io

from .common import write_blob, ByteReader


def _parse_head(br: ByteReader):
    vc_start_time = br.read_u64()
    vc_end_time = br.read_u64()
    vc_memory_required = br.read_u64()

    # Bits array lengths (varint)
    bits_uncomp_len = br.read_uleb128()[0]
    bits_comp_len = br.read_uleb128()[0]
    bits_count_local = br.read_uleb128()[0]

    # Read bits data (compressed length)
    bits_data_local = br.read_bytes(bits_comp_len)
    if bits_comp_len != bits_uncomp_len:
        dec_bits_local = zlib.decompress(bits_data_local)
    else:
        dec_bits_local = bits_data_local

    # After bits: waves count and packtype
    waves_count_local, _ = br.read_uleb128()
    waves_packtype_local = br.read_u8()

    return (
        vc_start_time,
        vc_end_time,
        vc_memory_required,
        bits_uncomp_len,
        bits_comp_len,
        bits_count_local,
        dec_bits_local,
        waves_count_local,
        waves_packtype_local,
    )


def _parse_tail(br: ByteReader):
    if br.length < 24:
        raise RuntimeError(
            "CallVCDATA: payload too short to contain time table metadata"
        )
    br.seek(0, io.SEEK_END)
    time_count_local = br.read_u64_rev()
    time_comp_len_local = br.read_u64_rev()
    time_uncomp_len_local = br.read_u64_rev()
    time_data_local = br.read_bytes_rev(time_comp_len_local)
    if time_comp_len_local != time_uncomp_len_local:
        dec_time_local = zlib.decompress(time_data_local)
    else:
        dec_time_local = time_data_local
    position_length_local = br.read_u64_rev()
    position_data_local = br.read_bytes_rev(position_length_local)
    return (
        time_count_local,
        time_uncomp_len_local,
        time_comp_len_local,
        dec_time_local,
        position_length_local,
        position_data_local,
    )


def _parse_time_data(dec_time: bytes, expected_count: int) -> list[int]:
    br = ByteReader(dec_time)
    timestamps: list[int] = []
    cur_time = 0
    for _i in range(expected_count):
        time_diff = br.read_uleb128()[0]
        cur_time += time_diff
        timestamps.append(cur_time)
    return timestamps


def _parse_position_data(position_data: bytes) -> list[int]:
    br = ByteReader(position_data)
    positions: list[int] = []
    prev_alias = 0
    while br.remaining() > 0:
        if (br.peek_bytes(1)[0] & 1) != 0:
            val = br.read_sleb128()[0] >> 1
            if val > 0:
                positions.append(val)
            elif val < 0:
                positions.append(val)
                prev_alias = val
            else:
                assert prev_alias != 0, "Previous alias has not been set"
                positions.append(prev_alias)
        else:
            zero_len = br.read_uleb128()[0] >> 1
            positions.extend([0] * zero_len)
    # after this function,
    # > 0: data lengths (the first > 0 value shall be 1)
    # < 0: aliases
    # == 0: no change
    return positions


def _parse_wave_data(wave_data: bytes, positions: list[int]) -> list[dict[str, Any]]:
    prev_i_has_data = -1
    # -1 is for compensating the first increment (see _parse_position_data's comment)
    cur_offset = -1
    position_offsets = [0] * len(positions)
    position_bytes = [0] * len(positions)
    for i, pos in enumerate(positions):
        if pos > 0:
            if prev_i_has_data != -1:
                position_bytes[prev_i_has_data] = pos
            cur_offset += pos
            position_offsets[i] = cur_offset
            prev_i_has_data = i
    if prev_i_has_data != -1:
        position_bytes[prev_i_has_data] = len(wave_data) - cur_offset

    wave_json: list[dict[str, Any]] = []
    br = ByteReader(wave_data)
    for i, (pos, offset, num_bytes) in enumerate(
        zip(positions, position_offsets, position_bytes)
    ):
        entry: dict[str, Any] = {"var_idx": i}
        if pos > 0:
            pass
            br.seek(offset)
            uncompressed_length, consumed = br.read_uleb128()
            compressed_length = num_bytes - consumed
            entry["type"] = "has_data"
            entry["offset"] = offset
            entry["uncompressed_length"] = uncompressed_length
            entry["compressed_length"] = compressed_length
            ## TODO: only support lz4 compression for wave data now
            data = br.read_bytes(compressed_length)
            try:
                dec_data = lz4.block.decompress(
                    data, uncompressed_size=uncompressed_length
                )
                if len(dec_data) != uncompressed_length:
                    raise RuntimeError(
                        f"CallVCDATA: wave data uncompressed length mismatch for var {i}"
                    )
            except Exception as e:
                entry["lz4_error"] = f"decompression error: {str(e)}"
            else:
                entry["lz4_error"] = ""
        elif pos < 0:
            entry["type"] = "alias"
            entry["alias_of"] = -i - 1
        else:
            entry["type"] = "no_change"
        wave_json.append(entry)
    return wave_json


def handle_vcdata(
    payload: bytes, idx: int, block_str: str, offset: int, output_dir: str
):
    """
    Parse a Value Change block and write out the four variable-length tables as blobs
    (bits, waves, position, time). Other metadata is written to a JSON file.

    For the four table blobs we use sub_idx=1 as requested. Metadata JSON uses sub_idx=0.
    """
    base_dir = output_dir
    payload_len = len(payload)
    info = {}

    # Cite from adoc:
    # It contains four tables - the bits array, waves table, position table and time table.
    # Note that the lengths of the position and time tables come after their data,
    # so you have to read backwards from the end to decode those tables.
    # I am not sure of the reason for this.
    br_head = ByteReader(payload)
    br_tail = ByteReader(payload)
    (
        vc_start_time,
        vc_end_time,
        vc_memory_required,
        bits_uncomp_len,
        bits_comp_len,
        bits_count,
        dec_bits,
        waves_count,
        waves_packtype,
    ) = _parse_head(br_head)
    (
        time_count,
        time_uncomp_len,
        time_comp_len,
        dec_time,
        position_length,
        position_data,
    ) = _parse_tail(br_tail)
    wave_bin_data = payload[br_head.tell() : br_tail.tell()]

    # put currently decoded info to info dict (no functionality, just for inspection)
    info["vc_start_time"] = vc_start_time
    info["vc_end_time"] = vc_end_time
    info["vc_memory_required"] = vc_memory_required
    info["bits_uncomp_len"] = bits_uncomp_len
    info["bits_comp_len"] = bits_comp_len
    info["bits_count"] = bits_count
    info["waves_count"] = waves_count
    info["waves_packtype"] = waves_packtype
    info["waves_bytes"] = len(wave_bin_data)
    info["time_count"] = time_count
    info["time_uncomp_len"] = time_uncomp_len
    info["time_comp_len"] = time_comp_len
    info["position_length"] = position_length

    # continue to parse details
    time_array = _parse_time_data(dec_time, time_count)
    position_array = _parse_position_data(position_data)
    wave_data = _parse_wave_data(wave_bin_data, position_array)
    info["position_count"] = len(position_array)

    write_blob(
        base_dir,
        idx,
        block_str,
        offset,
        payload_len,
        0,
        "header.json",
        json.dumps(info, indent=2, ensure_ascii=False).encode("utf-8"),
    )
    write_blob(
        base_dir, idx, block_str, offset, payload_len, 0, "init_bits.txt", dec_bits
    )
    time_array_bytes = "\n".join(str(t) for t in time_array).encode("utf-8")
    write_blob(
        base_dir,
        idx,
        block_str,
        offset,
        payload_len,
        0,
        "time_array.txt",
        time_array_bytes,
    )
    position_array_bytes = "\n".join(str(p) for p in position_array).encode("utf-8")
    write_blob(
        base_dir,
        idx,
        block_str,
        offset,
        payload_len,
        0,
        "position_array.txt",
        position_array_bytes,
    )
    write_blob(
        base_dir, idx, block_str, offset, payload_len, 0, "wave_data.bin", wave_bin_data
    )

    wave_json = json.dumps(wave_data, indent=2, ensure_ascii=False).encode("utf-8")
    write_blob(
        base_dir, idx, block_str, offset, payload_len, 1, "wave_data.json", wave_json
    )
