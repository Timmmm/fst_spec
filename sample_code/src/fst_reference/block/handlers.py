"""
FST block handlers
Central registry for block types and their handlers.
Handlers follow the signature: handler(first4_bytes, file_obj, offset, payload_len, block_str, output_dir)
"""

from enum import IntEnum
from typing import Callable, NamedTuple

from .hdr import handle_hdr
from .vcdata import handle_vcdata
from .geom import handle_geom
from .hier import handle_hier_gz, handle_hier_lz4, handle_hier_lz4duo
from .blackout import handle_blackout


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


class BlockHandler(NamedTuple):
    name: str
    handler: Callable[[bytes, int, str, int, str], None]


BLOCKS: dict[int, BlockHandler] = {
    BlockType.HDR.value: BlockHandler(name="HDR", handler=handle_hdr),
    BlockType.BLACKOUT.value: BlockHandler(name="BLACKOUT", handler=handle_blackout),
    BlockType.GEOM.value: BlockHandler(name="GEOM", handler=handle_geom),
    BlockType.HIER_GZ.value: BlockHandler(name="HIER_GZ", handler=handle_hier_gz),
    BlockType.HIER_LZ4.value: BlockHandler(name="HIER_LZ4", handler=handle_hier_lz4),
    BlockType.HIER_LZ4DUO.value: BlockHandler(
        name="HIER_LZ4DUO", handler=handle_hier_lz4duo
    ),
    BlockType.VCDATA.value: BlockHandler(
        name="VCDATA", handler=_unsupported_block_handler
    ),
    BlockType.VCDATA_DYN_ALIAS.value: BlockHandler(
        name="VCDATA_DYN_ALIAS",
        handler=_unsupported_block_handler,
    ),
    BlockType.VCDATA_DYN_ALIAS2.value: BlockHandler(
        name="VCDATA_DYN_ALIAS2",
        handler=handle_vcdata,
    ),
}
