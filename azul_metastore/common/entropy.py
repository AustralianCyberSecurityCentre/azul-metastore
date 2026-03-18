"""Encoding methods used to standardise entropy into a knn vector."""

import functools

import numpy as np

# Note - 40 is somewhat arbitrary, keeping this number smaller improves query efficiency
# The largest value that makes sense is 800 (because there are at most 800 values in entropy with the way the entropy plugin works)
# 40 was selected because 800 divides into it equally but it wasn't so large it would cause problems
ENTROPY_VECTOR_DIMENSION = 40
# should be factor of 2 (as this is the number of bytes to store in opensearch)
# Precision is how much entropy is rounded to the nearest 1/PRECISION_FACTOR, so 4 means 1/4==0.25, 1/8==0.125 etc...
PRECISION_FACTOR = 4
# Number of bits to store each float in opensearch
NUMBER_OF_BITS = 8 * PRECISION_FACTOR
# Total number of bits to store in opensearch
TOTAL_ENTROPY_BITS = ENTROPY_VECTOR_DIMENSION * NUMBER_OF_BITS
# Entropy maximum and minimum values.
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


def _round_to_precision(value: float):
    """Rounds to the appropriate precision level."""
    precision = 1 / PRECISION_FACTOR
    return round(value / precision) * precision


@functools.cache
def _get_int_conversion_table() -> dict[float, list[int]]:
    """Get a cache to convert an arbitrary float into a bit sequence based on the size of the float.

    A larger float value will get a corresponding number of 1 bits that is larger.
    The bits fill left to right, so the numerical value will gradually increase the larger the float.
    """
    result: dict[float, list[int]] = dict()
    step_size = MAX_ENTROPY_VALUE / (8 * PRECISION_FACTOR)
    total_steps = int(MAX_ENTROPY_VALUE * PRECISION_FACTOR)
    for cur_step in range(total_steps + 1):
        result[_round_to_precision(cur_step * step_size)] = []
        total_number_of_ones = cur_step
        for _ in range(PRECISION_FACTOR):
            if total_number_of_ones >= 8:
                # More than 8 ones
                bit_sequence = [1] * 8
                total_number_of_ones -= 8
            elif total_number_of_ones > 0:
                # Less than 8 ones
                ones_for_iteration = total_number_of_ones % 8
                bit_sequence = [1] * ones_for_iteration + [0] * (8 - ones_for_iteration)
                total_number_of_ones = 0
            else:
                # pad the rest with 0.
                bit_sequence = [0] * 8
            result[_round_to_precision(cur_step * step_size)].append(_convert_bit_sequence_to_number(bit_sequence))
    return result


def _convert_float_0_8_to_binary(entropy_values: np.ndarray) -> list[np.int8]:
    """Convert an entropy array with values 0->8 to a byte array that is useful for binary comparison.

    (values in array range from -128 to 127, but the focus of conversion is on the bit vaules)
    This method allows for hamming bit comparisons in opensearch nearest neighbour searches.
    """
    result = []
    for v in entropy_values:
        bin_values = _get_int_conversion_table()[_round_to_precision(v)]
        result = result + bin_values
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
