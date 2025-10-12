"""
Hierarchy data parsing helpers

Provides a minimal framework to parse the uncompressed hierarchy binary produced by HIER_* handlers.
- register_subparser(tag_byte, func): register a subparser; func(data, offset) -> (consumed, obj)
- parse_hier_binary(data): walk through data sequentially, dispatching to registered subparsers when possible.

Includes default subparsers for common hierarchy entries: SCOPE, UPSCOPE, VAR, ATTRBEGIN, ATTREND.
"""

from typing import Callable, Dict, Iterable
from .common import ByteReader
from enum import IntEnum


class ScopeType(IntEnum):
    VCD_MODULE = 0
    VCD_TASK = 1
    VCD_FUNCTION = 2
    VCD_BEGIN = 3
    VCD_FORK = 4
    VCD_GENERATE = 5
    VCD_STRUCT = 6
    VCD_UNION = 7
    VCD_CLASS = 8
    VCD_INTERFACE = 9
    VCD_PACKAGE = 10
    VCD_PROGRAM = 11
    VHDL_ARCHITECTURE = 12
    VHDL_PROCEDURE = 13
    VHDL_FUNCTION = 14
    VHDL_RECORD = 15
    VHDL_PROCESS = 16
    VHDL_BLOCK = 17
    VHDL_FOR_GENERATE = 18
    VHDL_IF_GENERATE = 19
    VHDL_GENERATE = 20
    VHDL_PACKAGE = 21
    GEN_ATTRBEGIN = 252
    GEN_ATTREND = 253
    VCD_SCOPE = 254
    VCD_UPSCOPE = 255


class VarType(IntEnum):
    VCD_EVENT = 0
    VCD_INTEGER = 1
    VCD_PARAMETER = 2
    VCD_REAL = 3
    VCD_REAL_PARAMETER = 4
    VCD_REG = 5
    VCD_SUPPLY0 = 6
    VCD_SUPPLY1 = 7
    VCD_TIME = 8
    VCD_TRI = 9
    VCD_TRIAND = 10
    VCD_TRIOR = 11
    VCD_TRIREG = 12
    VCD_TRI0 = 13
    VCD_TRI1 = 14
    VCD_WAND = 15
    VCD_WIRE = 16
    VCD_WOR = 17
    VCD_PORT = 18
    VCD_SPARRAY = 19
    VCD_REALTIME = 20
    GEN_STRING = 21
    SV_BIT = 22
    SV_LOGIC = 23
    SV_INT = 24
    SV_SHORTINT = 25
    SV_LONGINT = 26
    SV_BYTE = 27
    SV_ENUM = 28
    SV_SHORTREAL = 29


# Global state: count of non-alias variables (assigned sequential IDs starting at 0)
_non_alias_var_count = 0

_subparsers: Dict[int, Callable[[ByteReader], object]] = {}


def register_subparser(tag_byte: int, fn: Callable[[ByteReader], object]):
    """
    Register a subparser for a given leading tag byte.
    fn receives a ByteReader positioned at the tag byte and must consume bytes from it.
    It returns a parsed object (JSON-serializable). The caller will compute consumed bytes
    as the difference in reader position before/after the call.
    """
    _subparsers[tag_byte & 0xFF] = fn


def register_subparsers(tag_bytes: Iterable[int], fn: Callable[[ByteReader], object]):
    for tag in tag_bytes:
        register_subparser(tag, fn)


def parse_hier_binary(data: bytes) -> dict:
    """
    Parse sequentially from offset 0 until an unknown tag is encountered.

    Returns a dict: { total_len, consumed, data: [ ... ], stopped?, stopped_at?, stopped_tag?, remaining_len?, remaining_preview? }.
    """

    total = len(data)
    off = 0
    data_list = []
    br = ByteReader(data)

    while off < total:
        first = data[off]
        if first not in _subparsers:
            snippet = data[off : off + 64]
            hexp = snippet.hex()
            ascp = "".join([chr(b) if 32 <= b <= 126 else "." for b in snippet])
            msg = f"Unregistered hierarchy tag {int(first)} at offset {off}; next_bytes_hex={hexp}; ascii_preview={ascp}"
            print(msg)
            raise RuntimeError(msg)

        # position reader at the start offset and call handler
        br.seek(off)
        handler = _subparsers[first]
        start_pos = br.tell()
        obj = handler(br)
        consumed = br.tell() - start_pos

        if consumed <= 0:
            snippet = data[off : off + 64]
            hexp = snippet.hex()
            ascp = "".join([chr(b) if 32 <= b <= 126 else "." for b in snippet])
            msg = f"subparser for tag {first} returned non-positive consumed at offset {off}; next_bytes_hex={hexp}; ascii_preview={ascp}"
            print(msg)
            raise RuntimeError(msg)

        obj["offset"] = off
        data_list.append(obj)
        off += consumed

    return {"total_len": total, "consumed": off, "data": data_list, "stopped": False}


# -------------------- Subparsers --------------------
# Each subparser receives the full buffer and the offset where the tag byte resides.
# It must return (consumed_bytes, parsed_object). Consumed includes the tag byte.


def _parse_scope(br: ByteReader) -> object:
    # tag (1) + scopetype (1) + name\0 + comp\0
    start = br.tell()
    blen = br.length
    if start >= blen:
        raise RuntimeError("offset out of range")
    # read tag already present at current position
    tag = br.read_u8()
    if br.remaining() <= 0:
        raise RuntimeError("truncated scope")
    scopetype = br.read_u8()
    name, nlen = br.read_cstring()
    comp, clen = br.read_cstring()
    consumed = br.tell() - start
    try:
        st_name = ScopeType(scopetype).name
    except Exception:
        st_name = f"UNKNOWN_{scopetype}"

    ret = {
        "type": "SCOPE",
        "scope_type_num": scopetype,
        "scope_type_name": st_name,
        "name": name,
        "component": comp,
    }
    return ret


def _parse_upscope(br: ByteReader) -> object:
    # tag only
    start = br.tell()
    tag = br.read_u8()
    return {"type": "UPSCOPE"}


def _parse_attrbegin(br: ByteReader) -> object:
    # tag(1) + attrtype(1) + subtype(1) + name\0 + arg(varint)
    start = br.tell()
    tag = br.read_u8()
    attrtype = br.read_u8()
    # currently only support attrtype == 0
    if attrtype != 0:
        raise AssertionError(f"Non-zero attrtype {attrtype} not supported now")

    subtype = br.read_u8()
    # subtype meanings (0..8) - user-provided labels
    # 0 FST_MT_COMMENT
    # 1 FST_MT_ENVVAR
    # 2 FST_MT_SUPVAR
    # 3 FST_MT_PATHNAME
    # 4 FST_MT_SOURCESTEM
    # 5 FST_MT_SOURCEISTEM
    # 6 FST_MT_VALUELIST
    # 7 FST_MT_ENUMTABLE
    # 8 FST_MT_UNKNOWN

    ret = {
        "type": "ATTRBEGIN",
        "attrtype": attrtype,
        "subtype": subtype,
    }
    if subtype == 4 or subtype == 5:
        ret["attr_value1"] = br.read_uleb128()[0]
        assert br.read_u8() == 0  # null terminator
        ret["attr_value2"] = br.read_uleb128()[0]
    else:
        ret["attr_str"] = br.read_cstring()[0]
        ret["attr_value"] = br.read_uleb128()[0]

    return ret


def _parse_attrend(br: ByteReader) -> object:
    # placeholder for ATTREND; no payload
    tag = br.read_u8()
    return {"type": "ATTREND"}


def _parse_var(br: ByteReader) -> object:
    """
    Parse when buffer at offset begins with Var::Type directly (no leading Hierarchy tag).
    Format: vt(1) + name\0 + len(varint) + alias(varint)
    """
    # new format: vt(1) + name\0 + len(varint) + alias(varint)
    start = br.tell()
    vt = br.read_u8()
    vd = br.read_u8()
    name, nlen = br.read_cstring()
    vlen, vlen_len = br.read_uleb128()
    alias, alias_len = br.read_uleb128()
    consumed = br.tell() - start

    vt_name = VarType(vt).name

    # alias==0 => not an alias: assign new sequential id (use current count then increment)
    # alias>0 => is an alias referring to id = alias - 1
    global _non_alias_var_count
    if alias == 0:
        assigned_id = _non_alias_var_count
        is_alias = False
        _non_alias_var_count += 1
    else:
        assigned_id = alias - 1
        is_alias = True

    ret = {
        "type": "VAR",
        "var_type_num": vt,
        "var_dir_num": vd,
        "var_type_name": vt_name,
        "name": name,
        "bit_length": vlen,
        "alias": alias,
        "is_alias": is_alias,
        "var_id": assigned_id,
    }
    # print(ret)
    return ret


# Register subparsers for Hierarchy::Type values
register_subparsers(
    range(int(VarType.VCD_EVENT), int(VarType.SV_SHORTREAL) + 1), _parse_var
)
register_subparser(int(ScopeType.VCD_SCOPE), _parse_scope)
register_subparser(int(ScopeType.VCD_UPSCOPE), _parse_upscope)
register_subparser(int(ScopeType.GEN_ATTRBEGIN), _parse_attrbegin)
register_subparser(int(ScopeType.GEN_ATTREND), _parse_attrend)
