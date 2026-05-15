-- <copyright file="DB_ID_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The DB_ID function returns the id of the specified database name.

-- Parameters: 
-- p_database_name : The name of the database.

-- Return: The id of the database specified. This id is the number assigned to the database when 
-- it was created. It is a consecutive number. 
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.DB_ID_UDF(p_database_name STRING) 
RETURNS INT
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    WITH CTE_EXP AS (SELECT DATABASE_NAME, ROW_NUMBER() OVER(ORDER BY CREATED) AS ID FROM INFORMATION_SCHEMA.DATABASES)
    SELECT ID FROM CTE_EXP WHERE CTE_EXP.DATABASE_NAME = p_database_name
$$;