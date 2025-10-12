from enum import IntEnum


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


"""
FST block handlers
Central registry for block types and their handlers.
Handlers follow the signature: handler(first4_bytes, file_obj, offset, payload_len, block_str, output_dir)
"""
from .hdr import CallHDR
from .vcdata import CallVCDATA
from .geom import CallGEOM
from .hier import CallHIER_GZ, CallHIER_LZ4, CallHIER_LZ4DUO
from .blackout import CallBLACKOUT


def _unsupported_block_handler(
    payload: bytes, idx: int, block_str: str, offset: int, output_dir: str
):
    raise RuntimeError(
        f"Block type {block_str} at offset {offset} is recognized but not yet supported in this parser."
    )


BLOCKS = {
    int(BlockType.HDR): {"name": "HDR", "handler": CallHDR},
    int(BlockType.BLACKOUT): {"name": "BLACKOUT", "handler": CallBLACKOUT},
    int(BlockType.GEOM): {"name": "GEOM", "handler": CallGEOM},
    int(BlockType.HIER_GZ): {"name": "HIER_GZ", "handler": CallHIER_GZ},
    int(BlockType.HIER_LZ4): {"name": "HIER_LZ4", "handler": CallHIER_LZ4},
    int(BlockType.HIER_LZ4DUO): {"name": "HIER_LZ4DUO", "handler": CallHIER_LZ4DUO},
    int(BlockType.VCDATA): {"name": "VCDATA", "handler": _unsupported_block_handler},
    int(BlockType.VCDATA_DYN_ALIAS): {
        "name": "VCDATA_DYN_ALIAS",
        "handler": _unsupported_block_handler,
    },
    int(BlockType.VCDATA_DYN_ALIAS2): {
        "name": "VCDATA_DYN_ALIAS2",
        "handler": CallVCDATA,
    },
}
