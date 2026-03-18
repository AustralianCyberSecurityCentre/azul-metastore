"""Encoding methods used to standardise entropy into a knn vector."""

import numpy as np

# Note - 40 is somewhat arbitrary, keeping this number smaller improves query efficiency
# The largest value that makes sense is 800 (because there are at most 800 values in entropy with the way the entropy plugin works)
# 40 was selected because 800 divides into it equally but it wasn't so large it would cause problems
ENTROPY_VECTOR_DIMENSION = 40
TOTAL_ENTROPY_BITS = ENTROPY_VECTOR_DIMENSION * 8
MAX_ENTROPY_VALUE = 8.0
MIN_ENTROPY_VALUE = 0.0


def _interpolate_entropy(entropy_values: list[float]) -> np.ndarray | None:
    """Use interpolation to ensure there are exactly 40 values for entropy."""
    # Don't attempt interpolation unless there are at least enough entropy to fill up the vector (40 - note is roughly a 10kB file)
    if len(entropy_values) < ENTROPY_VECTOR_DIMENSION:
        return None
    # Interpolate from any number of entropy values, this can scale the number of entropy values up or down to fit.
    original_x_axis = np.linspace(0, 1.0, len(entropy_values))
    new_x_axis = np.linspace(0.0, 1.0, ENTROPY_VECTOR_DIMENSION)
    new_values = np.interp(new_x_axis, original_x_axis, entropy_values)
    # Ensure the interpolated values don't exceed Entropy's maximum and minimum
    for i, v in enumerate(new_values):
        if v > MAX_ENTROPY_VALUE:
            new_values[i] = MAX_ENTROPY_VALUE
        if v < MIN_ENTROPY_VALUE:
            new_values[i] = MIN_ENTROPY_VALUE

    return np.asarray(new_values, dtype=float)


def _convert_bit_sequence_to_number(bit_sequence: list[int]) -> int:
    """Convert an array of bits into a signed int8 and then to an int."""
    bit_array_np = np.array(bit_sequence, dtype=np.uint8)
    packed_bytes = np.packbits(bit_array_np)
    signed_int = packed_bytes.astype(np.int8)[0]
    return int(signed_int)


# Convert an integer value to the appropriate binary representation
_int_conversion_table = {
    0: _convert_bit_sequence_to_number([0, 0, 0, 0, 0, 0, 0, 0]),  # 0
    1: _convert_bit_sequence_to_number([0, 0, 0, 0, 0, 0, 0, 1]),  # 1
    2: _convert_bit_sequence_to_number([0, 0, 0, 0, 0, 0, 1, 1]),  # 3
    3: _convert_bit_sequence_to_number([0, 0, 0, 0, 0, 1, 1, 1]),  # 7
    4: _convert_bit_sequence_to_number([0, 0, 0, 0, 1, 1, 1, 1]),  # 15
    5: _convert_bit_sequence_to_number([0, 0, 0, 1, 1, 1, 1, 1]),  # 31
    6: _convert_bit_sequence_to_number([0, 0, 1, 1, 1, 1, 1, 1]),  # 63
    7: _convert_bit_sequence_to_number([0, 1, 1, 1, 1, 1, 1, 1]),  # 127
    8: _convert_bit_sequence_to_number([1, 1, 1, 1, 1, 1, 1, 1]),  # -1
}


def _convert_float_0_8_to_binary(entropy_values: np.ndarray) -> list[np.int8]:
    """Convert an entropy array with values 0->8 to a byte array that is useful for binary comparison.
    (values in array range from -128 to 127, but the focus of conversion is on the bit vaules)
    This method allows for hamming bit comparisons in opensearch nearest neighbour searches.
    """
    result = []
    for v in entropy_values:
        bin_value = _int_conversion_table[int(round(v, 0))]
        result.append(int(bin_value))
    return result


def convert_entropy_to_opensearch_entropy(entropy_values: list[float]) -> list[np.int8] | None:
    """Convert an entropy list of floats into a byte array that can be indexed or searched for in Opensearch.

    This method converts entropy into a list of int8 values in the range -128 to 127.
    This means entropy is only accurate to the nearest 0.0315, as it's float value is converted to bytes.
    This optimises storage in Opensearch and is considered acceptable as entropy has to first be interpolated.
    So there is also a large loss of precision during the interpolation anyway.
    """
    entropy_numpy_array = _interpolate_entropy(entropy_values)
    if entropy_numpy_array is None or len(entropy_numpy_array) == 0:
        return None
    result = _convert_float_0_8_to_binary(entropy_numpy_array)
    return result
