-- <copyright file="UPDATE_PACKAGE_VARIABLE_STATE_UDP.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The UPDATE_PACKAGE_VARIABLE_STATE_UDP() procedure updates the given package variable value.
-- It is a wrapper for the Snowflake SETVARIABLE() function to achieve a more legible output.
-- =========================================================================================================
CREATE OR REPLACE PROCEDURE PUBLIC.UPDATE_PACKAGE_VARIABLE_STATE_UDP (VARIABLE VARCHAR, NEW_VALUE VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
<SnowConvertVersionComment>
EXECUTE AS CALLER
AS
$$
    BEGIN
        RETURN SETVARIABLE(VARIABLE, NEW_VALUE);
    END;
$$;