-- <copyright file="FORMATMESSAGE_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The FORMATMESSAGE_UDF() provides the functionality of the SQL Server FORMATMESSAGE function.
-- It constructs a message from an existing message from a provided string.
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.FORMATMESSAGE_UDF(MESSAGE STRING, ARGS ARRAY)
RETURNS STRING
LANGUAGE python
IMMUTABLE
RUNTIME_VERSION = '3.8'
HANDLER = 'format_py'
<SnowConvertVersionComment>
as
$$
def format_py(message,args):
  return message % (*args,)
$$;