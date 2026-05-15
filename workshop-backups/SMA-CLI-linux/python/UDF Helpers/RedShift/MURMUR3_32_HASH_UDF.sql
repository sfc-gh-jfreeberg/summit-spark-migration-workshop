-- <copyright file="MURMUR3_32_HASH_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>
-- ====================================================================

CREATE OR REPLACE FUNCTION PUBLIC.MURMUR3_32_HASH_UDF(input VARCHAR)
RETURNS INTEGER
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('mmh3')
HANDLER = 'murmur3_hash'
<SnowConvertVersionComment>
AS
$$
import mmh3

def murmur3_hash(input_value):
    """
    Hash a string value using Murmur3A 32-bit algorithm.

    Args:
        input_value: String to hash (or None)

    Returns:
        32-bit signed integer hash, or None if input is None
    """
    if input_value is None:
        return None
    return mmh3.hash(input_value, signed=True)
$$;

-- ============================================================================
-- UDF 2: MURMUR3_32_HASH for NUMBER/INTEGER values
-- ============================================================================
-- Usage: MURMUR3_32_HASH(1)
-- Returns: 1392991556 (matches Redshift exactly)
-- Note: Uses 64-bit little-endian encoding per Apache Iceberg specification
-- ============================================================================

CREATE OR REPLACE FUNCTION PUBLIC.MURMUR3_32_HASH_UDF(input NUMBER)
RETURNS INTEGER
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('mmh3')
HANDLER = 'murmur3_hash_int'
<SnowConvertVersionComment>
AS
$$
import mmh3
import struct

def murmur3_hash_int(input_value):
    """
    Hash an integer value using Murmur3A 32-bit algorithm.

    CRITICAL: Uses 64-bit little-endian encoding to match Redshift behavior.
    This follows the Apache Iceberg specification for consistent hashing.

    Args:
        input_value: Integer to hash (or None)

    Returns:
        32-bit signed integer hash, or None if input is None
    """
    if input_value is None:
        return None

    # Redshift uses 64-bit little-endian encoding for integers
    # Format: '<q' = little-endian (<) long long (q)
    value_bytes = struct.pack('<q', int(input_value))
    return mmh3.hash(value_bytes, signed=True)
$$;

-- ============================================================================
-- UDF 3: MURMUR3_32_HASH_SEED for VARCHAR with seed parameter
-- ============================================================================
-- Usage: MURMUR3_32_HASH_SEED('Amazon Redshift', MURMUR3_32_HASH(1))
-- Returns: -1713130188 (matches Redshift exactly)
-- Purpose: Multi-column hashing, string concatenation patterns
-- ============================================================================

CREATE OR REPLACE FUNCTION PUBLIC.MURMUR3_32_HASH_UDF(input VARCHAR, seed INTEGER)
RETURNS INTEGER
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('mmh3')
HANDLER = 'murmur3_hash_seed'
<SnowConvertVersionComment>
AS
$$
import mmh3

def murmur3_hash_seed(input_value, seed_value):
    """
    Hash a string value with a seed using Murmur3A 32-bit algorithm.

    The seed parameter allows chaining hashes for multi-column scenarios:
    Example: MURMUR3_32_HASH_SEED(col2, MURMUR3_32_HASH(col1))

    Args:
        input_value: String to hash (or None)
        seed_value: Integer seed for hashing (default: 0 if None)

    Returns:
        32-bit signed integer hash, or None if input is None
    """
    if input_value is None:
        return None
    if seed_value is None:
        seed_value = 0
    return mmh3.hash(input_value, seed=seed_value, signed=True)
$$;

-- ============================================================================
-- UDF 4: MURMUR3_32_HASH_SEED for NUMBER with seed parameter
-- ============================================================================
-- Usage: MURMUR3_32_HASH_SEED(1, MURMUR3_32_HASH(2))
-- Returns: 1179621905 (matches Redshift exactly)
-- Purpose: Multi-column hashing with numeric values
-- ============================================================================

CREATE OR REPLACE FUNCTION PUBLIC.MURMUR3_32_HASH_UDF(input NUMBER, seed INTEGER)
RETURNS INTEGER
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('mmh3')
HANDLER = 'murmur3_hash_seed_int'
<SnowConvertVersionComment>
AS
$$
import mmh3
import struct

def murmur3_hash_seed_int(input_value, seed_value):
    """
    Hash an integer value with a seed using Murmur3A 32-bit algorithm.

    CRITICAL: Uses 64-bit little-endian encoding to match Redshift behavior.
    The seed parameter allows chaining hashes for multi-column scenarios.

    Example: MURMUR3_32_HASH_SEED(1, MURMUR3_32_HASH(2))

    Args:
        input_value: Integer to hash (or None)
        seed_value: Integer seed for hashing (default: 0 if None)

    Returns:
        32-bit signed integer hash, or None if input is None
    """
    if input_value is None:
        return None
    if seed_value is None:
        seed_value = 0

    # Use 64-bit little-endian encoding for integers
    value_bytes = struct.pack('<q', int(input_value))
    return mmh3.hash(value_bytes, seed=seed_value, signed=True)
$$;