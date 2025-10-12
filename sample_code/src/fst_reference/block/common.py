import os
import io
import struct


def write_blob(
    base_dir: str,
    block_idx: int,
    block_type: str,
    offset: int,
    payload_len: int,
    sub_idx: int,
    ext: str,
    data_bytes: bytes,
):
    """
    Write an arbitrary blob (JSON, binary) using a stable filename format:
    sub_idx is used when multiple files are produced per block (starting at 0).
    """
    # format with spacing to keep sortable; use zero-padded numeric fields for stability
    fname = f"{block_idx:03d}.{block_type}.off{offset:012d}.len{payload_len:012d}.{sub_idx:02d}.{ext}"
    out_path = os.path.join(base_dir, fname)
    with open(out_path, "wb") as out:
        out.write(data_bytes)
    print(f"WROTE {out_path} ({len(data_bytes)} bytes)")


class ByteReader:
    """
    A simple byte reader with offset tracking and bounds checking.
    """

    def __init__(self, data: bytes):
        self.data = data
        self.offset = 0
        self.length = len(data)

    def tell(self) -> int:
        return self.offset

    def seek(self, off: int, whence: int = 0):
        if whence == io.SEEK_SET:
            self.offset = off
        elif whence == io.SEEK_CUR:
            self.offset += off
        elif whence == io.SEEK_END:
            # off is interpreted as distance from EOF (negative to go before end)
            self.offset = self.length + off
        else:
            raise RuntimeError("invalid whence")
        if self.offset < 0:
            self.offset = 0
        if self.offset > self.length:
            self.offset = self.length

    def remaining(self) -> int:
        return max(0, self.length - self.offset)

    def peek_bytes(self, n: int) -> bytes:
        """
        Read `n` bytes from current offset without moving the cursor.
        """
        if n <= 0:
            return b""
        end = min(self.offset + n, self.length)
        return self.data[self.offset : end]

    def read_bytes(self, n: int) -> bytes:
        """
        Read `n` bytes from current offset and move the cursor forward.
        """
        ret = self.peek_bytes(n)
        self.offset += len(ret)
        return ret

    def read_u8(self) -> int:
        b = self.read_bytes(1)
        if len(b) < 1:
            raise EOFError("read_u8: unexpected EOF")
        return b[0]

    def read_u16(self) -> int:
        b = self.read_bytes(2)
        if len(b) < 2:
            raise EOFError("read_u16: unexpected EOF")
        return struct.unpack(">H", b)[0]

    def read_u32(self) -> int:
        b = self.read_bytes(4)
        if len(b) < 4:
            raise EOFError("read_u32: unexpected EOF")
        return struct.unpack(">I", b)[0]

    def read_u64(self) -> int:
        b = self.read_bytes(8)
        if len(b) < 8:
            raise EOFError("read_u64: unexpected EOF")
        return struct.unpack(">Q", b)[0]

    def read_i8(self) -> int:
        b = self.read_bytes(1)
        if len(b) < 1:
            raise EOFError("read_i8: unexpected EOF")
        return struct.unpack(">b", b)[0]

    def read_i32(self) -> int:
        b = self.read_bytes(4)
        if len(b) < 4:
            raise EOFError("read_i32: unexpected EOF")
        return struct.unpack(">i", b)[0]

    def read_i64(self) -> int:
        b = self.read_bytes(8)
        if len(b) < 8:
            raise EOFError("read_i64: unexpected EOF")
        return struct.unpack(">q", b)[0]

    def read_double(self) -> float:
        b = self.read_bytes(8)
        if len(b) < 8:
            raise EOFError("read_double: unexpected EOF")
        return struct.unpack(">d", b)[0]

    def read_uleb128(self) -> tuple[int, int]:
        """
        Read unsigned LEB128 from current offset. Returns (value, length_read).
        """
        result = 0
        shift = 0
        start = self.offset
        pos = start
        while pos < self.length:
            b = self.data[pos]
            pos += 1
            result |= (b & 0x7F) << shift
            if b & 0x80 == 0:
                break
            if shift > 63:
                # While FST allows more than 64 bits, we limit to 64 bits here
                # This shall rarely happen in practice
                raise RuntimeError("ULEB128 too large")
            shift += 7
        length = pos - start
        self.offset = pos
        return result, length

    def read_sleb128(self) -> tuple[int, int]:
        """
        Read signed LEB128 (svarint) from current offset.

        Returns (value, length_read). Implements sign-extension similar to
        fstGetSVarint64 semantics.
        """
        result, length = self.read_uleb128()
        bit_width = length * 7
        if (result >> (bit_width - 1)) & 1:
            # sign bit is set, treat as negative number
            result = result - (1 << bit_width)
        return result, length

    def read_u64_rev(self) -> int:
        """
        Move back 8 bytes from current cursor and read a big-endian u64.
        """
        self.seek(-8, io.SEEK_CUR)
        return struct.unpack(">Q", self.peek_bytes(8))[0]

    def read_bytes_rev(self, n: int) -> bytes:
        """
        Move back `n` bytes from current cursor and read `n` bytes.
        """
        self.seek(-n, io.SEEK_CUR)
        return self.peek_bytes(n)

    def read_cstring(self) -> tuple[str, int]:
        """
        Read a null-terminated C string from current offset. Returns (str, length_including_null).
        """
        # backward compatible signature without max_size
        return self.read_cstring_max(None)

    def read_cstring_max(self, max_size: int | None = None) -> tuple[str, int]:
        """
        Read a null-terminated C string from current offset.

        If max_size is provided, the returned Python string is truncated to at most
        max_size bytes (mimicking a fixed-size C buffer copy), but the reader will
        still consume bytes up to and including the NUL terminator.

        Returns (str, length_including_null).
        """
        pos = self.offset
        while pos < self.length and self.data[pos] != 0:
            pos += 1
        if pos >= self.length:
            raise EOFError("unterminated cstring")
        raw = self.data[self.offset : pos]
        # truncate for return view, but do NOT alter consumption semantics
        if max_size is None:
            s = raw.decode("utf-8", errors="replace")
        else:
            s = raw[:max_size].decode("utf-8", errors="replace")
        length = pos - self.offset + 1
        self.offset = pos + 1
        return s, length
