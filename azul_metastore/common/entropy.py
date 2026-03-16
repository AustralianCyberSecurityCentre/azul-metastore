"""Encoding methods used to standardise entropy into a knn vector."""

import numpy as np

# Note - 32 is arbitrary, keeping this number smaller improves query efficiency
# The largest value that makes sense is 800 (because there are at most 800 values in entropy with the way the entropy plugin works)
# but 16, 40, 64... would also be valid (40 in theory would make the math for interpolation better for larger files)
ENTROPY_VECTOR_DIMENSION = 32
MAX_ENTROPY_VALUE = 8.0
MIN_ENTROPY_VALUE = 0.0


def _interpolate_entropy(entropy_values: list[float]) -> np.ndarray | None:
    """Use interpolation to ensure there are exactly 32 values for entropy."""
    # Can't perform meaningful interpolation is there isn't at least 4 values.
    if len(entropy_values) < 4:
        return None
    # Interpolate from any number of entropy values 4->800 to bieng the length of ENTROPY_VECTOR_DIMENSION
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


def _convert_float_0_8_to_byte(entropy_values: np.ndarray) -> list[np.int8]:
    """Convert an entropy array with values 0->8 to a byte array with values -128 to 127.

    This method allows for optimised storage of entropy vectors in Opensearch.
    """
    y = (255.0 / MAX_ENTROPY_VALUE) * entropy_values - 128.0
    # Round to nearest integer
    y_rounded = np.rint(y)
    # Ensure values are in the range -128 to 127
    y_clamped = np.clip(y_rounded, -128, 127)
    return y_clamped.astype(np.int8).tolist()


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
    return _convert_float_0_8_to_byte(entropy_numpy_array)
