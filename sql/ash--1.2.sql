-- pg_ash v1.2 (development)
-- Changes from v1.1:

-- Swap detail and chart column order in timeline_chart functions
-- (detail after chart for better visual readability)

-- Drop old signatures first
drop function if exists ash.timeline_chart(interval, interval, int, int, boolean);
drop function if exists ash.timeline_chart_at(timestamptz, timestamptz, interval, int, int, boolean);

-- TODO: recreate timeline_chart() with (bucket_start, active, chart, detail) column order
-- TODO: recreate timeline_chart_at() with (bucket_start, active, chart, detail) column order
