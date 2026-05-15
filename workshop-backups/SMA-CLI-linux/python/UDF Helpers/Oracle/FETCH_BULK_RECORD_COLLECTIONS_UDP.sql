-- <copyright file="FETCH_BULK_RECORD_COLLECTIONS_UDP.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

CREATE OR REPLACE PROCEDURE PUBLIC.FETCH_BULK_RECORD_COLLECTIONS_UDP(CURSOR OBJECT, LIMIT FLOAT, COLUMN_NAMES ARRAY)
RETURNS OBJECT
LANGUAGE JAVASCRIPT
<SnowConvertVersionComment>
EXECUTE AS CALLER
AS
$$
  var limitValue = LIMIT ?? 'NULL';
  var query = `SELECT * FROM ${CURSOR.NAME}_TEMP_TABLE LIMIT ${limitValue} OFFSET ${CURSOR.ROWCOUNT}`;
  var stmt = snowflake.createStatement({ sqlText: query});
  var resultSet = stmt.execute();
  var column_count = stmt.getColumnCount();
  CURSOR.RESULT = {};
  if (COLUMN_NAMES)
  {
    for (let i = 0 ; i < COLUMN_NAMES.length ; i++) {
      CURSOR.RESULT[COLUMN_NAMES[i]] = [];
    }
  } else {
    for (let i = 1 ; i <= column_count ; i++) {
      let columnName = stmt.getColumnName(i);
      CURSOR.RESULT[columnName] = [];
    }
  }
  
  while (resultSet.next()) {
    for (let i = 1 ; i <= column_count ; i++) {
      let columnName = stmt.getColumnName(i);
      let fieldName = COLUMN_NAMES ? COLUMN_NAMES[i - 1] : columnName;
      CURSOR.RESULT[fieldName].push(resultSet.getColumnValue(columnName));
    }
  }
  CURSOR.ROWCOUNT += stmt.getRowCount();
  CURSOR.FOUND = stmt.getRowCount() > 0;
  CURSOR.NOTFOUND = !CURSOR.FOUND;
  return CURSOR;
$$;

CREATE OR REPLACE PROCEDURE PUBLIC.FETCH_BULK_RECORD_COLLECTIONS_UDP(CURSOR OBJECT)
RETURNS OBJECT
LANGUAGE SQL
<SnowConvertVersionComment>
EXECUTE AS CALLER
AS
$$
  DECLARE
    RESULT OBJECT;
  BEGIN
    RESULT := (CALL PUBLIC.FETCH_BULK_RECORD_COLLECTIONS_UDP(:CURSOR, NULL, NULL));
    RETURN :RESULT;
  END;
$$;

CREATE OR REPLACE PROCEDURE PUBLIC.FETCH_BULK_RECORD_COLLECTIONS_UDP(CURSOR OBJECT, LIMIT INTEGER)
RETURNS OBJECT
LANGUAGE SQL
<SnowConvertVersionComment>
EXECUTE AS CALLER
AS
$$
  DECLARE
    RESULT OBJECT;
  BEGIN
    RESULT := (CALL PUBLIC.FETCH_BULK_RECORD_COLLECTIONS_UDP(:CURSOR, :LIMIT, NULL));
    RETURN :RESULT;
  END;
$$;

CREATE OR REPLACE PROCEDURE PUBLIC.FETCH_BULK_RECORD_COLLECTIONS_UDP(CURSOR OBJECT, COLUMN_NAMES ARRAY)
RETURNS OBJECT
LANGUAGE SQL
<SnowConvertVersionComment>
EXECUTE AS CALLER
AS
$$
  DECLARE
    RESULT OBJECT;
  BEGIN
    RESULT := (CALL PUBLIC.FETCH_BULK_RECORD_COLLECTIONS_UDP(:CURSOR, NULL, :COLUMN_NAMES));
    RETURN :RESULT;
  END;
$$;