-- <copyright file="BFILENAME_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The BFILENAME_UDF() function takes the directory name and the file name parameters as STRING
-- and returns a concatenation of them using '\'. Since BFILE is translated to VARCHAR, the BFILENAME result
-- is handled as text. Note: '\' must be changed to match the Operating System file concatenation character.
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.BFILENAME_UDF (DIRECTORYNAME STRING, FILENAME STRING)
RETURNS STRING
LANGUAGE SQL
IMMUTABLE 
<SnowConvertVersionComment>
AS
$$
	DIRECTORYNAME || '\\' || FILENAME
$$;