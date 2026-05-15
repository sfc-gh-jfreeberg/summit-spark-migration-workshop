-- <copyright file="INTEGER_BITSHIFTLEFT_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2026 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: UDF used to convert an integer to its hexadecimal string representation with a specified
--              number of bits. The output is padded or truncated to match the exact hex length required.
-- PARAMETERS:
--     NUM: The integer value to convert to hexadecimal.
--     BITS: The number of bits to represent (determines the hex string length as BITS/4).
-- RETURNS:
--     STRING containing the hexadecimal representation of the integer, padded or truncated to the 
--     specified bit length.
-- EXAMPLE:
--      1) SELECT PUBLIC.INT_TO_BINARY_UDF(255, 16);
--      2) SELECT PUBLIC.INT_TO_BINARY_UDF(15, 8);
--      3) SELECT PUBLIC.INT_TO_BINARY_UDF(4095, 12);
--      Results:
--      1) '00ff'
--      2) '0f'
--      3) 'fff'
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.INT_TO_BINARY_UDF(num VARIANT, bits VARIANT)
RETURNS STRING
LANGUAGE JAVASCRIPT
<SnowConvertVersionComment>
AS
$$
	hexLength = BITS / 4;
    result = (NUM >>> 0).toString(16);
    if (result.length > hexLength){
        return result.substring(result.length - hexLength);
    }

    if (result.length < hexLength){
        return "0".repeat(hexLength - result.length) + result;
    }

    return result;
$$;

-- =========================================================================================================
-- Description: UDF used to convert a binary value to its integer representation. Handles signed integers
--              by checking the most significant bit for negative values (two's complement).
-- PARAMETERS:
--     BINARYSTRING: The binary value to convert to an integer.
-- RETURNS:
--     STRING containing the integer value represented by the binary input. Returns negative values
--     when the most significant bit indicates a signed negative number.
-- EXAMPLE:
--      1) SELECT PUBLIC.BINARY_TO_INT_UDF(TO_BINARY('00FF', 'HEX'));
--      2) SELECT PUBLIC.BINARY_TO_INT_UDF(TO_BINARY('FF', 'HEX'));
--      3) SELECT PUBLIC.BINARY_TO_INT_UDF(TO_BINARY('0100', 'HEX'));
--      Results:
--      1) 255
--      2) -1
--      3) 256
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.BINARY_TO_INT_UDF(binaryString BINARY)
RETURNS STRING
LANGUAGE JAVASCRIPT
<SnowConvertVersionComment>
AS
$$
    var number = 0;
    for (var i = 0; i < BINARYSTRING.length; i++) {
        number = number * 256 + BINARYSTRING[i];
    }
    if (BINARYSTRING.length > 0 && BINARYSTRING[0] >= 128) {
        number = number - Math.pow(2, BINARYSTRING.length * 8);
    }
    return number;
$$;

-- =========================================================================================================
-- Description: UDF used to emulate the PostgreSQL bitwise left shift operator (<<) for integer values.
--              Shifts the bits of the input value to the left by the specified amount, constrained to
--              a maximum bit width.
-- PARAMETERS:
--     VALUE: The integer value to perform the bit shift on.
--     SHIFTAMOUNT: The number of bit positions to shift left.
--     MAXBITS: The maximum number of bits to use for the operation (e.g., 32 for 32-bit integers).
-- RETURNS:
--     INTEGER containing the result of the left bit shift operation.
-- EXAMPLE:
--      1) SELECT PUBLIC.INTEGER_BITSHIFTLEFT_UDF(1, 4, 32);
--      2) SELECT PUBLIC.INTEGER_BITSHIFTLEFT_UDF(5, 2, 32);
--      3) SELECT PUBLIC.INTEGER_BITSHIFTLEFT_UDF(255, 8, 32);
--      Results:
--      1) 16    -- Binary: 0001 shifted left by 4 becomes 10000
--      2) 20    -- Binary: 0101 shifted left by 2 becomes 10100
--      3) 65280 -- Binary: 11111111 shifted left by 8 becomes 1111111100000000
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.INTEGER_BITSHIFTLEFT_UDF(value INTEGER, shiftAmount INTEGER, maxBits INTEGER)
RETURNS INTEGER
LANGUAGE SQL
<SnowConvertVersionComment>
AS
$$
    TO_NUMBER(BINARY_TO_INT_UDF(BITSHIFTLEFT(TO_BINARY(INT_TO_BINARY_UDF(value, maxBits)), shiftAmount)))
$$;