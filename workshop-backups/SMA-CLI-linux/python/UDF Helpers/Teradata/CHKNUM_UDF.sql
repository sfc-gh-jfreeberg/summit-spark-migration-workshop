-- <copyright file="CHKNUM_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =============================================
-- DESCRIPTION: CHECKS IF A STRING VALUE REPRESENT A NUMERIC VALUE
-- PARAMETER: STRING VALUE
-- RETURNS: RETURNS 1 IF THE PARAMETER IS A VALID NUMBER 
-- EXAMPLE:
--    SELECT CHKNUM_UDF('432');
--    RETURNS 1
-- TERADATA EQUIVALENT: CHKNUM function (it is a teradata extension)
-- EXAMPLE:
--    SELECT CHKNUM('432');
--    RETURNS 1
-- =============================================
CREATE OR REPLACE FUNCTION PUBLIC.CHKNUM_UDF(NUM STRING)
RETURNS INTEGER
<SnowConvertVersionComment>
AS
$$
IFF(TRY_CAST(NUM AS NUMBER) IS NOT NULL,1,0)
$$;