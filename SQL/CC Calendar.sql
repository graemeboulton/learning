-------------------------------------------------------------
-- 1. CREATE TABLE
-------------------------------------------------------------

CREATE TABLE trading_calendar (
    date                     DATE PRIMARY KEY,

    -- Year level
    year_label               TEXT,
    year_and_season          TEXT,
    year_and_half            TEXT,
    year_and_quarter         TEXT,
    year_and_month           TEXT,
    year_and_month_sort      INT,
    year_and_week            TEXT,
    year_and_week_sort       INT,
    year_start               DATE,
    year_end                 DATE,

    -- Month level
    trading_month_number     INT,
    month_name               TEXT,
    month_start              DATE,
    month_end                DATE,

    -- Week level
    week_start               DATE,
    week_end                 DATE,
    week_number              INT,

    -- Day level
    day_name                 TEXT,
    day_of_week              INT,
    day_of_trading_month     INT
);

-------------------------------------------------------------
-- 2. INSERT ALL DATES FOR TRADING YEAR 2025–26
-------------------------------------------------------------
INSERT INTO trading_calendar (date)
SELECT d::date
FROM generate_series(
    '2025-01-26'::date, -- update value
    '2026-01-31'::date, -- update value
    interval '1 day'
) AS d;

-------------------------------------------------------------
-- 3. SET TRADING YEAR START/END & YEAR LABEL
-------------------------------------------------------------
UPDATE trading_calendar
SET year_start = DATE '2025-01-26', -- update value
    year_end   = DATE '2026-01-31', -- update value
    year_label = '2025 - 26';       -- update value

-------------------------------------------------------------
-- 4. DAY NAME + DAY OF WEEK (Sunday = 1 ... Saturday = 7)
-------------------------------------------------------------
UPDATE trading_calendar
SET day_name    = TO_CHAR(date, 'FMDay'),
    day_of_week = CASE
                    WHEN EXTRACT(DOW FROM date)::int = 0 THEN 1
                    ELSE EXTRACT(DOW FROM date)::int + 1
                  END;

-------------------------------------------------------------
-- 5. WEEK START / WEEK END (Sunday → Saturday)
-------------------------------------------------------------
UPDATE trading_calendar
SET week_start = date - (day_of_week - 1),
    week_end   = date + (7 - day_of_week);

-------------------------------------------------------------
-- 6. WEEK NUMBER WITHIN TRADING YEAR
-------------------------------------------------------------
UPDATE trading_calendar
SET week_number = 1 + ((week_start - year_start) / 7);

-------------------------------------------------------------
-- 7. YEAR/WEEK LABELS
-------------------------------------------------------------
UPDATE trading_calendar
SET year_and_week = year_label || ' - W' || week_number::text,
    year_and_week_sort = (LEFT(year_label, 4)::int * 100) + week_number;

-------------------------------------------------------------
-- 8. TRADING MONTH NUMBER (4–5 week pattern)
-------------------------------------------------------------
UPDATE trading_calendar
SET trading_month_number =
    CASE
        WHEN week_number BETWEEN  1 AND  4 THEN  1  -- Feb -- Update between values for each month
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
WHERE year_label = '2025 - 26';     -- update value

-------------------------------------------------------------
-- 9. MONTH NAME BASED ON TRADING MONTH NUMBER
-------------------------------------------------------------
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
WHERE year_label = '2025 - 26';     -- update value

-------------------------------------------------------------
-- 10. MONTH START / MONTH END (From data)
-------------------------------------------------------------
WITH month_bounds AS (
    SELECT
        date,
        year_label,
        trading_month_number,
        MIN(date) OVER (PARTITION BY year_label, trading_month_number) AS ms,
        MAX(date) OVER (PARTITION BY year_label, trading_month_number) AS me
    FROM trading_calendar
    WHERE year_label = '2025 - 26'  -- update value
)
UPDATE trading_calendar t
SET month_start = b.ms,
    month_end   = b.me
FROM month_bounds b
WHERE t.date = b.date
  AND t.year_label = b.year_label
  AND t.trading_month_number = b.trading_month_number;

-------------------------------------------------------------
-- 11. YEAR/MONTH LABEL + SORT VALUE
-------------------------------------------------------------
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
WHERE year_label = '2025 - 26';     -- update value

-------------------------------------------------------------
-- 12. DAY OF TRADING MONTH
-------------------------------------------------------------
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
    WHERE year_label = '2025 - 26'      -- update value
)
UPDATE trading_calendar t
SET day_of_trading_month = n.rn
FROM numbered n
WHERE t.date = n.date
  AND t.year_label = n.year_label
  AND t.trading_month_number = n.trading_month_number;

-------------------------------------------------------------
-- 13. SEASONS (SS / AW)
-------------------------------------------------------------
UPDATE trading_calendar
SET year_and_season = year_label || ' - ' ||
    CASE
        WHEN trading_month_number BETWEEN 1 AND 6 THEN 'SS'
        WHEN trading_month_number BETWEEN 7 AND 12 THEN 'AW'
    END
WHERE year_label = '2025 - 26';     -- update value

-------------------------------------------------------------
-- 14. HALVES (H1 / H2)
-------------------------------------------------------------
UPDATE trading_calendar
SET year_and_half = year_label || ' - ' ||
    CASE
        WHEN trading_month_number BETWEEN 1 AND 6 THEN 'H1'
        WHEN trading_month_number BETWEEN 7 AND 12 THEN 'H2'
    END
WHERE year_label = '2025 - 26';     -- update value

-------------------------------------------------------------
-- 15. QUARTERS (Q1–Q4)
-------------------------------------------------------------
UPDATE trading_calendar
SET year_and_quarter = year_label || ' - Q' ||
    CASE
        WHEN trading_month_number BETWEEN  1 AND  3 THEN 1
        WHEN trading_month_number BETWEEN  4 AND  6 THEN 2
        WHEN trading_month_number BETWEEN  7 AND  9 THEN 3
        WHEN trading_month_number BETWEEN 10 AND 12 THEN 4
    END
WHERE year_label = '2025 - 26';     -- update value