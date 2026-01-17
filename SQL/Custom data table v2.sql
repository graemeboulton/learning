CREATE TABLE trading_months_and_weeks (
    year_label              TEXT,
    trading_month_number    INT,
    month_name              TEXT,
    week_start_number       INT,
    week_end_number         INT,
    season_code             TEXT,   -- 'SS' / 'AW'
    half_code               TEXT,   -- 'H1' / 'H2'
    quarter_num             INT,    -- 1..4
    PRIMARY KEY (year_label, trading_month_number)
);

-------------------------------------------------------------
-- Trading year: 2026 - 27
-- Assumptions:
--   YEAR_START = 2026-02-01 (Sunday)
--   YEAR_END   = 2027-02-06 (Saturday)
--   53 trading weeks, same 4-5-4 pattern as 2025-26
-------------------------------------------------------------

-------------------------------------------------------------
-- 1. Insert raw dates for this trading year
-------------------------------------------------------------
INSERT INTO trading_calendar (date)
SELECT d::date
FROM generate_series(
    '2026-02-01'::date,      -- trading year start
    '2027-02-06'::date,      -- trading year end
    interval '1 day'
) AS d;

-------------------------------------------------------------
-- 2. Set year_label, year_start, year_end
-------------------------------------------------------------
UPDATE trading_calendar
SET year_start = DATE '2026-02-01',
    year_end   = DATE '2027-02-06',
    year_label = '2026 - 27'
WHERE date BETWEEN '2026-02-01' AND '2027-02-06';

-------------------------------------------------------------
-- 3. Day name and day_of_week (Sun = 1 .. Sat = 7)
-------------------------------------------------------------
UPDATE trading_calendar
SET day_name    = TO_CHAR(date, 'FMDay'),
    day_of_week = CASE
                    WHEN EXTRACT(DOW FROM date)::int = 0 THEN 1
                    ELSE EXTRACT(DOW FROM date)::int + 1
                  END
WHERE year_label = '2026 - 27';

-------------------------------------------------------------
-- 4. Week_start, week_end, week_number, year_and_week
-------------------------------------------------------------
-- Week boundaries
UPDATE trading_calendar
SET week_start = date - (day_of_week - 1),   -- back to Sunday
    week_end   = date + (7 - day_of_week)    -- forward to Saturday
WHERE year_label = '2026 - 27';

-- Week number within trading year
UPDATE trading_calendar
SET week_number = 1 + ((week_start - year_start) / 7)
WHERE year_label = '2026 - 27';

-- Week label + sort key
UPDATE trading_calendar
SET year_and_week      = year_label || ' - W' || week_number::text,
    year_and_week_sort = (LEFT(year_label, 4)::int * 100) + week_number
WHERE year_label = '2026 - 27';

-------------------------------------------------------------
-- 5. Insert month rules for 2026 - 27 into trading_months_and_weeks
--    (same 4–5–4 pattern & SS/AW/H1/H2/Q1–Q4 as 2025-26)
-------------------------------------------------------------
INSERT INTO trading_months_and_weeks (
    year_label, trading_month_number, month_name,
    week_start_number, week_end_number,
    season_code, half_code, quarter_num
) VALUES
('2026 - 27',  1, 'February',  1,  4, 'SS', 'H1', 1),
('2026 - 27',  2, 'March',     5,  9, 'SS', 'H1', 1),
('2026 - 27',  3, 'April',    10, 13, 'SS', 'H1', 1),
('2026 - 27',  4, 'May',      14, 17, 'SS', 'H1', 2),
('2026 - 27',  5, 'June',     18, 22, 'SS', 'H1', 2),
('2026 - 27',  6, 'July',     23, 26, 'SS', 'H1', 2),
('2026 - 27',  7, 'August',   27, 30, 'AW', 'H2', 3),
('2026 - 27',  8, 'September',31, 35, 'AW', 'H2', 3),
('2026 - 27',  9, 'October',  36, 39, 'AW', 'H2', 3),
('2026 - 27', 10, 'November', 40, 43, 'AW', 'H2', 4),
('2026 - 27', 11, 'December', 44, 48, 'AW', 'H2', 4),
('2026 - 27', 12, 'January',  49, 53, 'AW', 'H2', 4);

-------------------------------------------------------------
-- 6. Apply month, season, half, quarter mapping to calendar
-------------------------------------------------------------
UPDATE trading_calendar c
SET trading_month_number = m.trading_month_number,
    month_name           = m.month_name,
    year_and_season      = c.year_label || ' - ' || m.season_code,
    year_and_half        = c.year_label || ' - ' || m.half_code,
    year_and_quarter     = c.year_label || ' - Q' || m.quarter_num::text
FROM trading_months_and_weeks m
WHERE c.year_label  = m.year_label
  AND c.week_number BETWEEN m.week_start_number AND m.week_end_number
  AND c.year_label  = '2026 - 27';

-------------------------------------------------------------
-- 7. month_start / month_end for 2026 - 27
-------------------------------------------------------------
WITH month_bounds AS (
    SELECT
        date,
        year_label,
        trading_month_number,
        MIN(date) OVER (PARTITION BY year_label, trading_month_number) AS ms,
        MAX(date) OVER (PARTITION BY year_label, trading_month_number) AS me
    FROM trading_calendar
    WHERE year_label = '2026 - 27'
)
UPDATE trading_calendar t
SET month_start = b.ms,
    month_end   = b.me
FROM month_bounds b
WHERE t.date                 = b.date
  AND t.year_label           = b.year_label
  AND t.trading_month_number = b.trading_month_number;

-------------------------------------------------------------
-- 8. year_and_month + year_and_month_sort
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
WHERE year_label = '2026 - 27';

-------------------------------------------------------------
-- 9. day_of_trading_month for 2026 - 27
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
    WHERE year_label = '2026 - 27'
)
UPDATE trading_calendar t
SET day_of_trading_month = n.rn
FROM numbered n
WHERE t.date                 = n.date
  AND t.year_label           = n.year_label
  AND t.trading_month_number = n.trading_month_number;
Next Project
Custom date table built in SQL v1
