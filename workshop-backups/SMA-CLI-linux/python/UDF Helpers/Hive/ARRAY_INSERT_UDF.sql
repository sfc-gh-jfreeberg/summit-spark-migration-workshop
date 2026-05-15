-- <copyright file="ARRAY_INSERT_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =============================================
-- DESCRIPTION: UDF THAT REPRODUCES SPARK-HIVE-DBX ARRAY_INSERT FUNCTIONALITY
-- PARAMETERS:
--      INPUT: ARRAY
--      POS: INTEGER
--      VALUE: VARIANT
-- RETURNS: AN ARRAY WITH THE VALUE INSERTED AT THE POSITION SPECIFIED.
-- Disclaimer:
--      Spark SQL -Hive-DBX: Indexes start at 1, so the first element is at position 1. (Also supports negative indexes)
--      Snowflake indexes start at 0, so the first element is at position 0.
-- EXAMPLE:
--  Spark Sample:  array_insert(array('a', 'b', 'c'), -5, 'z') -- Spark output: ["z",NULL,"a","b","c"]
--  Snowflake Equivalent: array_insert_udf(array_construct('a', 'b', 'c'), -5, 'z') -- Snow output: ["z",NULL,"a", "b", "c"]
-- =============================================
CREATE OR REPLACE FUNCTION PUBLIC.ARRAY_INSERT_UDF(INPUT ARRAY, POS int, VALUE VARIANT)
RETURNS ARRAY
<SnowConvertVersionComment>
AS
$$
     array_insert(input, IFF(pos > 0, pos - 1, array_size(input) + pos + 1), value)
$$;