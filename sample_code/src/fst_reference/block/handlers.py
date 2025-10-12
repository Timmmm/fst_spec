"""
FST block handlers
Central registry for block types and their handlers.
Handlers follow the signature: handler(first4_bytes, file_obj, offset, payload_len, block_str, output_dir)
"""

from enum import IntEnum

from .hdr import CallHDR
from .vcdata import CallVCDATA
from .geom import CallGEOM
from .hier import CallHIER_GZ, CallHIER_LZ4, CallHIER_LZ4DUO
from .blackout import CallBLACKOUT


# FST block header definitions
class BlockType(IntEnum):
    HDR = 0
    VCDATA = 1
    BLACKOUT = 2
    GEOM = 3
    HIER_GZ = 4
    VCDATA_DYN_ALIAS = 5
    HIER_LZ4 = 6
    HIER_LZ4DUO = 7
    VCDATA_DYN_ALIAS2 = 8
    ZWRAPPER = 254
    SKIP = 255


def _unsupported_block_handler(
    payload: bytes, idx: int, block_str: str, offset: int, output_dir: str
):
    raise RuntimeError(
        f"Block type {block_str} at offset {offset} is recognized but not yet supported in this parser."
    )


BLOCKS = {
    BlockType.HDR.value: {"name": "HDR", "handler": CallHDR},
    BlockType.BLACKOUT.value: {"name": "BLACKOUT", "handler": CallBLACKOUT},
    BlockType.GEOM.value: {"name": "GEOM", "handler": CallGEOM},
    BlockType.HIER_GZ.value: {"name": "HIER_GZ", "handler": CallHIER_GZ},
    BlockType.HIER_LZ4.value: {"name": "HIER_LZ4", "handler": CallHIER_LZ4},
    BlockType.HIER_LZ4DUO.value: {"name": "HIER_LZ4DUO", "handler": CallHIER_LZ4DUO},
    BlockType.VCDATA.value: {"name": "VCDATA", "handler": _unsupported_block_handler},
    BlockType.VCDATA_DYN_ALIAS.value: {
        "name": "VCDATA_DYN_ALIAS",
        "handler": _unsupported_block_handler,
    },
    BlockType.VCDATA_DYN_ALIAS2.value: {
        "name": "VCDATA_DYN_ALIAS2",
        "handler": CallVCDATA,
    },
}
