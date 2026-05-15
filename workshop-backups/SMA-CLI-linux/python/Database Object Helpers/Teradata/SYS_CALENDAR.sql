-- <copyright file="SYS_CALENDAR.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ==================================================================================================
-- THE FOLLOWING DATABASE OBJECTS AND SCHEMAS WERE CONCEIVED TO SUPPORT THE FUNCTIONALITY OF
-- THE SYS_CALENDAR DATABASE OBJECTS FROM TERADATA IN SNOWFLAKE, FOR MORE INFORMATION ABOUT THIS
-- HELPER VISIT:
-- https://github.com/MobilizeNet/SnowConvert_Support_Library/blob/main/SYS_CALENDAR/SYS_CALENDAR.md
-- ==================================================================================================

CREATE SCHEMA SYS_CALENDAR;

CREATE OR REPLACE TABLE SYS_CALENDAR.CALDATES (
	CDATE DATE NOT NULL,
	CONSTRAINT UNIQ_CDATE UNIQUE (CDATE)
);

CREATE OR REPLACE TABLE SYS_CALENDAR.CALDATES (cdate DATE)
AS
SELECT DATEADD(day, seq4(), '1900-01-01') cdate FROM TABLE(GENERATOR(RowCount => 365.25*2000)) WHERE cdate < '2101-01-01';

CREATE OR REPLACE VIEW SYS_CALENDAR.CALBASICS (
calendar_date,
day_of_calendar,
day_of_month,
day_of_year,
month_of_year,
year_of_calendar)
AS
   SELECT
   cdate as calendar_date,
   datediff('d','1900-01-01',cdate)+1 as day_of_calendar,
   dayofmonth(cdate) as day_of_month,
   dayofyear(cdate) as day_of_year,
   month(cdate) as month_of_year,
   year(cdate)-1900 as year_of_calendar
FROM SYS_CALENDAR.CALDATES;

CREATE OR REPLACE VIEW SYS_CALENDAR.CALENDARTMP(
  calendar_date,
  day_of_week,
  day_of_month,
  day_of_year,
  day_of_calendar,
  weekday_of_month,
  week_of_month,
  week_of_year,
  week_of_calendar,
  month_of_quarter,
  month_of_year,
  month_of_calendar,
  quarter_of_year,
  quarter_of_calendar,
  year_of_calendar)
AS
   SELECT
   calendar_date,
   dayofweek(calendar_date)+1 as day_of_week,
   day_of_month,
   day_of_year,
   day_of_calendar,
   TRUNC((day_of_month - 1) / 7) + 1 as  weekday_of_month,
   TRUNC((day_of_month - mod( (day_of_calendar + 0), 7) + 6) / 7) as week_of_month,
   TRUNC((day_of_year - mod( (day_of_calendar + 0), 7) + 6) / 7) as week_of_year,
   TRUNC((day_of_calendar - mod( (day_of_calendar + 0), 7) + 6) / 7) as week_of_calendar,
   mod((month_of_year - 1), 3) + 1 as month_of_quarter,
   month_of_year,
   month_of_year + 12 * year_of_calendar as month_of_calendar,
   quarter(calendar_date) as quarter_of_year,
   TRUNC((month_of_year + 2) / 3) + 4 * year_of_calendar as quarter_of_calendar,
   year_of_calendar + 1900 as year_of_calendar
 FROM SYS_CALENDAR.CALBASICS;

CREATE OR REPLACE VIEW SYS_CALENDAR.CALENDAR (
calendar_date,
day_of_calendar,
day_of_week,
day_of_month,
day_of_year,
month_of_year,
weekday_of_month,
week_of_month,
week_of_year,
week_of_calendar,
month_of_quarter,
month_of_calendar,
quarter_of_year,
quarter_of_calendar,
year_of_calendar
)
AS
SELECT
calendar_date,
day_of_calendar,
day_of_week,
day_of_month,
day_of_year,
month_of_year,
weekday_of_month,
week_of_month,
week_of_year,
week_of_calendar,
month_of_quarter,
month_of_calendar,
quarter_of_year,
quarter_of_calendar,
year_of_calendar
FROM SYS_CALENDAR.CALENDARTMP;