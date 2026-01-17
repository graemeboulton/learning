/*

CREATE TABLE trading_calendar (
    date                     DATE PRIMARY KEY,  -- actual date

    -- Year level
    year_label               TEXT,              -- Trading year i.e. 2025 - 26
    year_and_season          TEXT,              -- i.e. 2025 - 26 - SS (Represents Summer Season)
    year_and_half            TEXT,              -- i.e. 2025 - 26 - H1 (Represents First Half)
    year_and_quarter         TEXT,              -- i.e. 2025 - 26 - Q1 (Represents First Quarter)
    year_and_month           TEXT,              -- i.e. 2025 - 26 - Jan
    year_and_month_sort      INT,               -- i.e. 202501 (for sorting purposes)
    year_and_week            TEXT,              -- i.e. 2025 - 26 - W01 
    year_and_week_sort       INT,               -- i.e. 20250101 (for sorting purposes)
    year_start               DATE,              -- First day of the trading year
    year_end                 DATE,              -- Last day of the trading year

    -- Month level
    month_name               TEXT,              -- i.e. January
    month_start              DATE,              -- First day of trading the month
    month_end                DATE,              -- Last day of trading the month

    -- Week level
    week_start               DATE,              -- First day of trading the week
    week_end                 DATE,              -- Last day of trading the week

    -- Day level
    day_name                 TEXT,              -- i.e. Monday
    day_of_week              INT,               -- 1 = Monday, 2 = Tuesday, ..., 7 = Sunday
    day_of_trading_month     INT,               -- 1 - 31
);

-- Populate the 'trading_calendar' table with dates from 26th Jan 2025 to 31st Jan 2026

INSERT INTO trading_calendar (date)   -- Insert values into the 'date' column of your table
SELECT d::date                        -- For each generated value (d), cast it to a DATE type
FROM generate_series(                 -- generate_series creates a sequence of values
    '2025-01-26'::date,               -- Start date of the sequence (26th Jan 2025)
    '2026-01-31'::date,               -- End date of the sequence (31st Jan 2026)
    interval '1 day'                  -- Step size: generate one value per day
) AS d;                               -- Alias the generated values as 'd'

-- Update year-level fields for each date in the trading_calendar table

UPDATE trading_calendar
SET year_start = DATE '2025-01-26',   -- first day of this trading year
    year_end   = DATE '2026-01-31',   -- last day of this trading year
    year_label = '2025 - 26';         -- text label for the trading year

-- Update day-level fields for each date in the trading_calendar table

UPDATE trading_calendar
SET day_name    = TO_CHAR(date, 'FMDay'),      -- 'Sunday', 'Monday', etc.
    day_of_week = EXTRACT(DOW FROM date)::int  -- 0=Sunday, 1=Monday, ..., 6=Saturday
                                              -- (we can shift this later if you prefer 1=Mon)
;

-- Update week-level fields for each date in the trading_calendar table

UPDATE trading_calendar
SET day_of_week =
    CASE
        WHEN EXTRACT(DOW FROM date)::int = 0 THEN 1
        ELSE EXTRACT(DOW FROM date)::int + 1
    END;

-- Add week_number column to the trading_calendar table

ALTER TABLE trading_calendar
ADD COLUMN week_number INT;

-- Update week_number based on the week_start and year_start

UPDATE trading_calendar
SET week_number = 1 + ((week_start - year_start) / 7);

-- Update year_and_week and year_and_week_sort fields for each date in the trading_calendar table

UPDATE trading_calendar
SET year_and_week = year_label || ' - W' || week_number::text,
    year_and_week_sort = (LEFT(year_label, 4)::int * 100) + week_number;

-- Update month-level fields for each date in the trading_calendar table (THIS WAS INCORRECT)

UPDATE trading_calendar
SET month_name  = TO_CHAR(date, 'FMMonth'),                  -- 'February', 'March', etc.
    month_start = date_trunc('month', date)::date,          -- first day of that calendar month
    month_end   = (date_trunc('month', date) 
                   + interval '1 month - 1 day')::date;     -- last day of that calendar month

-- Add trading_month_number column to the trading_calendar table

ALTER TABLE trading_calendar
ADD COLUMN trading_month_number INT;

-- Update trading_month_number based on the week_number within the trading year

UPDATE trading_calendar
SET trading_month_number =
    CASE
        WHEN week_number BETWEEN  1 AND  4 THEN  1  -- Feb
        WHEN week_number BETWEEN  5 AND  9 THEN  2  -- Mar
        WHEN week_number BETWEEN 10 AND 13 THEN  3  -- Apr
        WHEN week_number BETWEEN 14 AND 17 THEN  4  -- May
        WHEN week_number BETWEEN 18 AND 22 THEN  5  -- June
        WHEN week_number BETWEEN 23 AND 26 THEN  6  -- July
        WHEN week_number BETWEEN 27 AND 30 THEN  7  -- Aug
        WHEN week_number BETWEEN 31 AND 35 THEN  8  -- Sep
        WHEN week_number BETWEEN 36 AND 39 THEN  9  -- Oct
        WHEN week_number BETWEEN 40 AND 43 THEN 10  -- Nov
        WHEN week_number BETWEEN 44 AND 48 THEN 11  -- Dec
        WHEN week_number BETWEEN 49 AND 53 THEN 12  -- Jan
    END
WHERE year_label = '2025 - 26';

-- Correct month_name based on trading_month_number

UPDATE trading_calendar
SET month_name =
    CASE trading_month_number
        WHEN  1 THEN 'February'
        WHEN  2 THEN 'March'
        WHEN  3 THEN 'April'
        WHEN  4 THEN 'May'
        WHEN  5 THEN 'June'
        WHEN  6 THEN 'July'
        WHEN  7 THEN 'August'
        WHEN  8 THEN 'September'
        WHEN  9 THEN 'October'
        WHEN 10 THEN 'November'
        WHEN 11 THEN 'December'
        WHEN 12 THEN 'January'
    END
WHERE year_label = '2025 - 26';

-- Correct month_start and month_end based on trading_month_number

WITH month_bounds AS (
    SELECT
        date,
        year_label,
        trading_month_number,
        MIN(date) OVER (PARTITION BY year_label, trading_month_number) AS ms,
        MAX(date) OVER (PARTITION BY year_label, trading_month_number) AS me
    FROM trading_calendar
    WHERE year_label = '2025 - 26'
)
UPDATE trading_calendar t
SET month_start = b.ms,
    month_end   = b.me
FROM month_bounds b
WHERE t.date = b.date
  AND t.year_label = b.year_label
  AND t.trading_month_number = b.trading_month_number;

-- Update year_and_month and year_and_month_sort fields for each date in the trading_calendar table
UPDATE trading_calendar
SET year_and_month = year_label || ' - ' ||
    CASE trading_month_number
        WHEN  1 THEN 'Feb'
        WHEN  2 THEN 'Mar'
        WHEN  3 THEN 'Apr'
        WHEN  4 THEN 'May'
        WHEN  5 THEN 'June'
        WHEN  6 THEN 'July'
        WHEN  7 THEN 'Aug'
        WHEN  8 THEN 'Sep'
        WHEN  9 THEN 'Oct'
        WHEN 10 THEN 'Nov'
        WHEN 11 THEN 'Dec'
        WHEN 12 THEN 'Jan'
    END,
    year_and_month_sort =
        (LEFT(year_label, 4)::int * 100) + trading_month_number
WHERE year_label = '2025 - 26';

-- Update day_of_trading_month for each date in the trading_calendar table

WITH numbered AS (
    SELECT
        date,
        year_label,
        trading_month_number,
        ROW_NUMBER() OVER (
            PARTITION BY year_label, trading_month_number
            ORDER BY date
        ) AS rn
    FROM trading_calendar
    WHERE year_label = '2025 - 26'
)
UPDATE trading_calendar t
SET day_of_trading_month = n.rn
FROM numbered n
WHERE t.date = n.date
  AND t.year_label = n.year_label
  AND t.trading_month_number = n.trading_month_number;

-- Update year_and_season field for each date in the trading_calendar table

UPDATE trading_calendar
SET year_and_season = year_label || ' - ' ||
    CASE
        WHEN trading_month_number BETWEEN 1 AND 6 THEN 'SS'   -- Feb–Jul
        WHEN trading_month_number BETWEEN 7 AND 12 THEN 'AW'  -- Aug–Jan
    END
WHERE year_label = '2025 - 26';

-- Update year_and_half field for each date in the trading_calendar table

UPDATE trading_calendar
SET year_and_half = year_label || ' - ' ||
    CASE
        WHEN trading_month_number BETWEEN 1 AND 6 THEN 'H1'
        WHEN trading_month_number BETWEEN 7 AND 12 THEN 'H2'
    END
WHERE year_label = '2025 - 26';

-- Update year_and_quarter field for each date in the trading_calendar table

UPDATE trading_calendar
SET year_and_quarter = year_label || ' - Q' ||
    CASE
        WHEN trading_month_number BETWEEN  1 AND  3 THEN 1
        WHEN trading_month_number BETWEEN  4 AND  6 THEN 2
        WHEN trading_month_number BETWEEN  7 AND  9 THEN 3
        WHEN trading_month_number BETWEEN 10 AND 12 THEN 4
    END
WHERE year_label = '2025 - 26';

*/

--/*

select * from trading_calendar;

--*/