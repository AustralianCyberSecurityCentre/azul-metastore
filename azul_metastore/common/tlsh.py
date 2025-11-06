"""Encoders for TLSH hashes."""

from typing import Optional


def strip_tlsh_version(tlsh_hash: str) -> str:
    """Strips the prefix from a TLSH hash if it exists."""
    if tlsh_hash.startswith("T"):
        if tlsh_hash[1] == "1":
            return tlsh_hash[2:]
        else:
            # Unsupported TLSH version
            raise Exception(f"Invalid TLSH version in: '{tlsh_hash}'")
    else:
        return tlsh_hash


def _swap_byte(byte: int) -> int:
    """Swaps the bytes nibbles."""
    high = ((byte & 0xF0) >> 4) & 0x0F
    low = ((byte & 0x0F) << 4) & 0xF0
    return high | low


def _tlsh_to_array(tlsh_str: str) -> list[int]:
    """Converts a TLSH hash losslessly into a indexable byte vector for offline searching."""
    # Try to manually parse the hash
    # We have to do this anyway because the tlsh Python library doesn't (nicely) expose code bytes
    # Strip off the header
    tlsh_str = strip_tlsh_version(tlsh_str)

    if len(tlsh_str) != 70:
        raise Exception(f"Invalid TLSH length in: '{tlsh_str}'")

    data = bytearray.fromhex(tlsh_str)

    # These values are stored with swapped nibbles.
    checksum = _swap_byte(data[0])
    l_value = _swap_byte(data[1])
    q_ratio = data[2]
    # This is technically a waste of bits for a byte encoder (as we are wasting the top 4 bits),
    # but using this abstraction makes future use of non-byte-based kNN much simpler.
    q1_ratio = (q_ratio >> 4) & 0x0F
    q2_ratio = q_ratio & 0x0F
    code = data[3:]

    return [checksum, l_value, q1_ratio, q2_ratio] + list(code)


def _unsigned_array_to_signed(data: list[int]) -> list[int]:
    """Maps a unsigned 8-bit integer into signed 8-bit space (OpenSearch requirement for Java typing reasons)."""
    return [i - 128 for i in data]


def encode_tlsh_into_vector(tlsh_hash: str) -> Optional[list[int]]:
    """Converts a TLSH hash into an OpenSearch-compatible byte vector for clustering."""
    if tlsh_hash == "" or tlsh_hash == "NULL" or tlsh_hash == "TNULL":
        # If the file is too small to be hashed
        return None

    return _unsigned_array_to_signed(_tlsh_to_array(tlsh_hash))
