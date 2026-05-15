-- <copyright file="STRTOL_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2026 Snowflake Inc. All rights reserved.
-- </copyright>
-- =========================================================================================================
-- Description: The STRTOL function converts a string to a long integer in the specified base.
--
-- Parameters: 
-- STR : The string to convert to a long integer.
-- BASE : The numeric base for conversion (valid range: 2-36).
--
-- Return: The converted integer value clamped to BIGINT range. Returns NULL if inputs are NULL.
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.STRTOL_UDF(STR VARCHAR, BASE NUMBER)
RETURNS NUMBER
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'strtol_handler'
<SnowConvertVersionComment>
AS
$$
def strtol_handler(str_value, base_value):
    """
    Convert a string to a long integer in the specified base (2-36).
    Matches RedShift STRTOL behavior exactly.
    
    Args:
        str_value: String to convert (or None)
        base_value: Base for conversion (2-36)
    
    Returns:
        Converted integer value, clamped to BIGINT range (-2^63 to 2^63-1)
        Returns None if inputs are None
        Returns 0 if string is empty or '\0'
        Raises error if string is invalid for the given base
    """
    # Handle NULL inputs
    RS_STRTOL_MIN = -9223372036854775808 # Upper cap of BIGINT / result of strtol in Redshift
    RS_STRTOL_MAX = 9223372036854775807 # Lower cap of BIGINT / result of strtol in Redshift
    if str_value is None or base_value is None:
        return None
    
    # Trim leading/trailing whitespace (matches RedShift behavior)
    trimmed = str_value.strip()
    
    # Empty string or \0 after trimming returns 0
    if trimmed == '' or trimmed == '\0':
        return 0
    
    # Convert base to integer and validate range (2-36 as per standard strtol)
    base = int(base_value)
    if base < 2 or base > 36:
        return None
    
    try:
        # Python's int() with base parameter behaves like C's strtol:
        # - Handles optional leading '+' or '-'
        # - Handles hex prefix '0x' or '0X' when base is 0 or 16
        # - Raises ValueError for invalid inputs
        result = int(trimmed, base)
        
        if result > RS_STRTOL_MAX:
            return RS_STRTOL_MAX
        elif result < RS_STRTOL_MIN:
            return RS_STRTOL_MIN

        return result
        
    except ValueError:
        # Throw error for invalid conversions (matches RedShift)
        raise ValueError(f"The input {str_value} is not valid to be converted to base {base}")
$$;
