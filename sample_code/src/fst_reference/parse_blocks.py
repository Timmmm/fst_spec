"""Simple parser for FST blocks: 1 byte type, 8 byte length (length does not count the preceding 1 byte type, but includes this 8-byte length field)
Usage: python parse_blocks.py input_fst [--output_dir DIR]
"""

import argparse
import struct
import shutil
from pathlib import Path
from .block.handlers import BLOCKS

FMT_U8 = ">B"
FMT_U64 = ">Q"


def extract_blocks(fst_path, output_dir=None):
    input_fst_path = Path(fst_path).resolve()
    output_path = Path(output_dir).resolve()
    shutil.rmtree(output_path, ignore_errors=True)
    output_path.mkdir(parents=True, exist_ok=True)
    base_dir = str(output_path)

    # Open file and parse blocks sequentially. This is an experimental parser so
    # we keep checks minimal and assume well-formed input.
    with input_fst_path.open("rb") as f:
        idx = 0
        f.seek(0, 2)
        file_end = f.tell()
        f.seek(0)
        while True:
            offset = f.tell()
            remain = file_end - offset
            if remain < 9:
                break
            b = f.read(1)
            len_bytes = f.read(8)
            block_type = struct.unpack(FMT_U8, b)[0]
            block_len = struct.unpack(FMT_U64, len_bytes)[0]
            # Basic sanity: length must include the 8-byte length field
            if block_len < 8:
                print(f"#{idx} offset={offset}: invalid block_len {block_len}")
                break
            payload_len = block_len - 8
            payload_start = f.tell()
            f.seek(payload_start)
            # read full payload once and pass bytes to handler
            payload = f.read(payload_len)
            block_entry = BLOCKS.get(block_type)
            if block_entry is None:
                snippet = payload[:64]
                hexp = snippet.hex()
                ascp = "".join([chr(b) if 32 <= b <= 126 else "." for b in snippet])
                raise RuntimeError(
                    f"Unregistered block type {block_type} at offset {offset}; next_bytes_hex={hexp}; ascii_preview={ascp}"
                )
            handler = block_entry["handler"]
            if len(payload) < payload_len:
                print(
                    f"#{idx} offset={offset}: payload too short {len(payload)} < {payload_len}"
                )
            # Let handler exceptions propagate so we get full traceback for debugging
            handler(payload, idx, block_entry["name"], offset, base_dir)
            idx += 1


def main():
    parser = argparse.ArgumentParser(
        description="Simple parser for FST blocks: 1 byte type, 8 byte length",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("input_fst", help="FST file to parse")
    parser.add_argument(
        "--output_dir",
        help="Directory to save extracted blocks",
        default="output_blocks",
    )

    args = parser.parse_args()

    extract_blocks(args.input_fst, args.output_dir)
