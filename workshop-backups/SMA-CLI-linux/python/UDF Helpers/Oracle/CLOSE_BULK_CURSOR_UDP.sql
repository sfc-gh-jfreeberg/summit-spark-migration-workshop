-- <copyright file="CLOSE_BULK_CURSOR_UDP.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

CREATE OR REPLACE PROCEDURE PUBLIC.CLOSE_BULK_CURSOR_UDP(CURSOR OBJECT)
RETURNS OBJECT
LANGUAGE JAVASCRIPT
<SnowConvertVersionComment>
EXECUTE AS CALLER
AS
$$
  var query = `DROP TABLE ${CURSOR.NAME}_TEMP_TABLE`;
  snowflake.execute({ sqlText: query });
  CURSOR.ROWCOUNT = -1;
  CURSOR.ISOPEN = false;
  CURSOR.FOUND = null;
  CURSOR.NOTFOUND = null;
  return CURSOR;
$$;