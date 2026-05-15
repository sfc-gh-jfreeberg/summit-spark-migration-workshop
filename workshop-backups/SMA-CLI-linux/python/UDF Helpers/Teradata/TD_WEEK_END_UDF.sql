-- <copyright file="TD_WEEK_END_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

CREATE OR REPLACE FUNCTION PUBLIC.TD_WEEK_END_UDF(
    input_date DATE,
    mode STRING DEFAULT 'TERADATA'
)
RETURNS DATE
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
  WITH week_start AS (
    SELECT
      CASE
        WHEN UPPER(mode) = 'ISO' THEN
          -- ISO week calculation: Monday is day 1 (ISO 8601 standard)
          input_date - (DAYOFWEEKISO(input_date) - 1)
        WHEN UPPER(mode) = 'COMPATIBLE' THEN
          -- COMPATIBLE mode: Sunday is day 0 (same as TERADATA mode for consistency)
          input_date - DAYOFWEEK(input_date)
        WHEN UPPER(mode) = 'TERADATA' THEN
          -- TERADATA mode: Sunday is day 0 in Snowflake (matches Teradata Sunday-first week)
          input_date - DAYOFWEEK(input_date)
        ELSE
          -- Default to TERADATA mode (Sunday start) for any unrecognized mode or NULL
          input_date - DAYOFWEEK(input_date)
      END AS week_begin_date
  )
  SELECT week_begin_date + 6 FROM week_start
$$;

CREATE OR REPLACE FUNCTION PUBLIC.TD_WEEK_END_UDF(
    input_timestamp TIMESTAMP_NTZ,
    mode STRING DEFAULT 'TERADATA'
)
RETURNS TIMESTAMP_NTZ
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
  SELECT
    DATEADD(SECOND, 59,
      DATEADD(MINUTE, 59,
        DATEADD(HOUR, 19,
          TD_WEEK_END_UDF(
            input_timestamp::DATE,
            mode
          )::TIMESTAMP_NTZ
        )
      )
    )
$$;