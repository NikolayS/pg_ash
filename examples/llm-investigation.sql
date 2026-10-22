\set ON_ERROR_STOP on
\pset pager off
\pset null '(null)'

-- Existing pg_ash installation required. Reads only; never enables sampling.
-- Optional psql variables: since and until (timestamps with time zones).
-- Keep the same bounds and snapshot throughout the investigation.
begin transaction isolation level repeatable read read only;
set local statement_timeout = '30s';
set local timezone = 'UTC';
\if :{?until}
\else
  select now() as until \gset
\endif
\if :{?since}
\else
  select :'until'::timestamptz - interval '10 minutes' as since \gset
\endif

\echo Step 1: load, effective bounds, and stored observations
select * from ash.aas(since => :'since', until => :'until');
-- Readers align windows to minute boundaries. Reuse those disclosed bounds.
select period_start as since, period_end as until
from ash.aas(since => :'since', until => :'until')
\gset

\echo Step 2: wait events ranked by peak AAS
select * from ash.top(
  'wait_event', since => :'since', until => :'until',
  order_by => 'peak', n => 5
);

-- Always return one row to gset, including when no activity is available.
select
  coalesce((
    select key from ash.top(
      'wait_event', since => :'since', until => :'until',
      order_by => 'peak', n => 1
    ) where peak_aas > 0
  ), '') as wait_event
\gset
select :'wait_event' <> '' as has_event \gset
\if :has_event
  \echo Step 3: locate the selected event in time
  select * from ash.timeline(
    since => :'since', until => :'until',
    bucket => '1 minute', wait_event => :'wait_event'
  );

  -- Confirm minute precision, then choose a common window for attribution.
  select effective_bucket <= interval '1 minute' as can_drill
  from ash.aas(
    since => :'since', until => :'until', wait_event => :'wait_event'
  )
  \gset
  \if :can_drill
    select
      greatest(bucket_start, :'since'::timestamptz) as spike_since,
      least(bucket_start + interval '1 minute', :'until'::timestamptz)
        as spike_until
    from ash.timeline(
      since => :'since', until => :'until',
      bucket => '1 minute', wait_event => :'wait_event'
    )
    where avg_aas > 0
    order by avg_aas desc, bucket_start
    limit 1
    \gset

    \echo Step 4: queries experiencing the selected wait
    -- This requires raw attribution. A retention error is evidence that the
    -- historical wait-to-query link cannot be recovered from compact rollups.
    select * from ash.top(
      'query_id', since => :'spike_since', until => :'spike_until',
      wait_event => :'wait_event', order_by => 'peak', n => 5
    );
    select coalesce((
      select key from ash.top(
        'query_id', since => :'spike_since', until => :'spike_until',
        wait_event => :'wait_event', order_by => 'peak', n => 5
      ) where key ~ '^-?[0-9]+$'
      order by peak_aas desc nulls last, avg_aas desc, key
      limit 1
    ), '') as query_id
    \gset
    select :'query_id' <> '' as has_query \gset
    \if :has_query
      \echo Step 5: raw samples and the selected query wait profile
      select * from ash.samples(
        since => :'spike_since', until => :'spike_until',
        query_id => :'query_id'::bigint, n => 20
      );
      select * from ash.top(
        'wait_event', since => :'spike_since', until => :'spike_until',
        query_id => :'query_id'::bigint
      );
    \else
      \echo Step 5: no attributed query ID; do not infer SQL or a blocker.
    \endif
  \else
    \echo Retained resolution is coarser than one minute; stop this minute drill.
  \endif
\else
  \echo No positive observed peak; inspect retention and scheduler evidence.
\endif

\echo Optional report: NULL means no usable minute-rollup observations.
select jsonb_pretty(ash.report(since => :'since', until => :'until'));
rollback;
