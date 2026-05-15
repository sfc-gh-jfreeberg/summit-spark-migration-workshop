-- <copyright file="YEAR_NAME_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

CREATE OR REPLACE FUNCTION PUBLIC.YEAR_NAME_UDF(input_date DATE)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
  UPPER(
    CASE
      -- Years 1-19: Simple numbers
      WHEN YEAR(input_date) BETWEEN 1 AND 19 THEN
        CASE YEAR(input_date)
          WHEN 1 THEN 'one' WHEN 2 THEN 'two' WHEN 3 THEN 'three' WHEN 4 THEN 'four' WHEN 5 THEN 'five'
          WHEN 6 THEN 'six' WHEN 7 THEN 'seven' WHEN 8 THEN 'eight' WHEN 9 THEN 'nine' WHEN 10 THEN 'ten'
          WHEN 11 THEN 'eleven' WHEN 12 THEN 'twelve' WHEN 13 THEN 'thirteen' WHEN 14 THEN 'fourteen'
          WHEN 15 THEN 'fifteen' WHEN 16 THEN 'sixteen' WHEN 17 THEN 'seventeen' WHEN 18 THEN 'eighteen' WHEN 19 THEN 'nineteen'
        END

      -- Years 20-99: Tens format
      WHEN YEAR(input_date) BETWEEN 20 AND 99 THEN
        CASE FLOOR(YEAR(input_date) / 10)
          WHEN 2 THEN 'twenty' WHEN 3 THEN 'thirty' WHEN 4 THEN 'forty' WHEN 5 THEN 'fifty'
          WHEN 6 THEN 'sixty' WHEN 7 THEN 'seventy' WHEN 8 THEN 'eighty' WHEN 9 THEN 'ninety'
        END ||
        CASE MOD(YEAR(input_date), 10)
          WHEN 0 THEN '' WHEN 1 THEN '-one' WHEN 2 THEN '-two' WHEN 3 THEN '-three' WHEN 4 THEN '-four'
          WHEN 5 THEN '-five' WHEN 6 THEN '-six' WHEN 7 THEN '-seven' WHEN 8 THEN '-eight' WHEN 9 THEN '-nine'
        END

      -- Years 100-99999: Universal pattern using helper functions
      WHEN YEAR(input_date) BETWEEN 100 AND 99999 THEN
        -- Get the first 1-2 digits (century part)
        CASE
          WHEN YEAR(input_date) < 1000 THEN
            -- 100-999: First digit
            CASE FLOOR(YEAR(input_date) / 100)
              WHEN 1 THEN 'one' WHEN 2 THEN 'two' WHEN 3 THEN 'three' WHEN 4 THEN 'four' WHEN 5 THEN 'five'
              WHEN 6 THEN 'six' WHEN 7 THEN 'seven' WHEN 8 THEN 'eight' WHEN 9 THEN 'nine'
            END
          WHEN YEAR(input_date) < 2000 THEN
            -- 1000-1999: 10-19
            CASE FLOOR(YEAR(input_date) / 100)
              WHEN 10 THEN 'ten' WHEN 11 THEN 'eleven' WHEN 12 THEN 'twelve' WHEN 13 THEN 'thirteen' WHEN 14 THEN 'fourteen'
              WHEN 15 THEN 'fifteen' WHEN 16 THEN 'sixteen' WHEN 17 THEN 'seventeen' WHEN 18 THEN 'eighteen' WHEN 19 THEN 'nineteen'
            END
          WHEN YEAR(input_date) < 10000 THEN
            -- 2000-9999: 20-99
            CASE FLOOR(YEAR(input_date) / 100)
              WHEN 20 THEN 'twenty' WHEN 21 THEN 'twenty-one' WHEN 22 THEN 'twenty-two' WHEN 23 THEN 'twenty-three' WHEN 24 THEN 'twenty-four'
              WHEN 25 THEN 'twenty-five' WHEN 26 THEN 'twenty-six' WHEN 27 THEN 'twenty-seven' WHEN 28 THEN 'twenty-eight' WHEN 29 THEN 'twenty-nine'
              WHEN 30 THEN 'thirty' WHEN 31 THEN 'thirty-one' WHEN 32 THEN 'thirty-two' WHEN 33 THEN 'thirty-three' WHEN 34 THEN 'thirty-four'
              WHEN 35 THEN 'thirty-five' WHEN 36 THEN 'thirty-six' WHEN 37 THEN 'thirty-seven' WHEN 38 THEN 'thirty-eight' WHEN 39 THEN 'thirty-nine'
              WHEN 40 THEN 'forty' WHEN 41 THEN 'forty-one' WHEN 42 THEN 'forty-two' WHEN 43 THEN 'forty-three' WHEN 44 THEN 'forty-four'
              WHEN 45 THEN 'forty-five' WHEN 46 THEN 'forty-six' WHEN 47 THEN 'forty-seven' WHEN 48 THEN 'forty-eight' WHEN 49 THEN 'forty-nine'
              WHEN 50 THEN 'fifty' WHEN 51 THEN 'fifty-one' WHEN 52 THEN 'fifty-two' WHEN 53 THEN 'fifty-three' WHEN 54 THEN 'fifty-four'
              WHEN 55 THEN 'fifty-five' WHEN 56 THEN 'fifty-six' WHEN 57 THEN 'fifty-seven' WHEN 58 THEN 'fifty-eight' WHEN 59 THEN 'fifty-nine'
              WHEN 60 THEN 'sixty' WHEN 61 THEN 'sixty-one' WHEN 62 THEN 'sixty-two' WHEN 63 THEN 'sixty-three' WHEN 64 THEN 'sixty-four'
              WHEN 65 THEN 'sixty-five' WHEN 66 THEN 'sixty-six' WHEN 67 THEN 'sixty-seven' WHEN 68 THEN 'sixty-eight' WHEN 69 THEN 'sixty-nine'
              WHEN 70 THEN 'seventy' WHEN 71 THEN 'seventy-one' WHEN 72 THEN 'seventy-two' WHEN 73 THEN 'seventy-three' WHEN 74 THEN 'seventy-four'
              WHEN 75 THEN 'seventy-five' WHEN 76 THEN 'seventy-six' WHEN 77 THEN 'seventy-seven' WHEN 78 THEN 'seventy-eight' WHEN 79 THEN 'seventy-nine'
              WHEN 80 THEN 'eighty' WHEN 81 THEN 'eighty-one' WHEN 82 THEN 'eighty-two' WHEN 83 THEN 'eighty-three' WHEN 84 THEN 'eighty-four'
              WHEN 85 THEN 'eighty-five' WHEN 86 THEN 'eighty-six' WHEN 87 THEN 'eighty-seven' WHEN 88 THEN 'eighty-eight' WHEN 89 THEN 'eighty-nine'
              WHEN 90 THEN 'ninety' WHEN 91 THEN 'ninety-one' WHEN 92 THEN 'ninety-two' WHEN 93 THEN 'ninety-three' WHEN 94 THEN 'ninety-four'
              WHEN 95 THEN 'ninety-five' WHEN 96 THEN 'ninety-six' WHEN 97 THEN 'ninety-seven' WHEN 98 THEN 'ninety-eight' WHEN 99 THEN 'ninety-nine'
            END
          ELSE
            -- 10000+: More complex, but let's handle up to 99999
            'year-' || TO_CHAR(YEAR(input_date))
        END ||

        -- Determine format based on middle digit
        CASE
          -- When tens digit of last two digits is 0 (like 505, 1505, 2505): use "hundred" format
          WHEN FLOOR(MOD(YEAR(input_date), 100) / 10) = 0 THEN
            ' hundred' ||
            CASE MOD(YEAR(input_date), 10)
              WHEN 0 THEN ''
              WHEN 1 THEN ' one' WHEN 2 THEN ' two' WHEN 3 THEN ' three' WHEN 4 THEN ' four' WHEN 5 THEN ' five'
              WHEN 6 THEN ' six' WHEN 7 THEN ' seven' WHEN 8 THEN ' eight' WHEN 9 THEN ' nine'
            END
          -- When tens digit is not 0 (like 123, 1123, 2123): use abbreviated format
          ELSE
            ' ' ||
            CASE
              WHEN MOD(YEAR(input_date), 100) BETWEEN 10 AND 19 THEN
                CASE MOD(YEAR(input_date), 100)
                  WHEN 10 THEN 'ten' WHEN 11 THEN 'eleven' WHEN 12 THEN 'twelve' WHEN 13 THEN 'thirteen' WHEN 14 THEN 'fourteen'
                  WHEN 15 THEN 'fifteen' WHEN 16 THEN 'sixteen' WHEN 17 THEN 'seventeen' WHEN 18 THEN 'eighteen' WHEN 19 THEN 'nineteen'
                END
              ELSE
                CASE FLOOR(MOD(YEAR(input_date), 100) / 10)
                  WHEN 2 THEN 'twenty' WHEN 3 THEN 'thirty' WHEN 4 THEN 'forty' WHEN 5 THEN 'fifty'
                  WHEN 6 THEN 'sixty' WHEN 7 THEN 'seventy' WHEN 8 THEN 'eighty' WHEN 9 THEN 'ninety'
                END ||
                CASE MOD(YEAR(input_date), 10)
                  WHEN 0 THEN '' WHEN 1 THEN '-one' WHEN 2 THEN '-two' WHEN 3 THEN '-three' WHEN 4 THEN '-four'
                  WHEN 5 THEN '-five' WHEN 6 THEN '-six' WHEN 7 THEN '-seven' WHEN 8 THEN '-eight' WHEN 9 THEN '-nine'
                END
            END
        END

      -- Default for any other years
      ELSE ''
    END
  )
$$;