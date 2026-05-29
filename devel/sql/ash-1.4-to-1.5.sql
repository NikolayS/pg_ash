-- pg_ash development upgrade from 1.4 toward 1.5.
--
-- WIP: do not bump ash.config.version here. The version is finalized when
-- 1.5 is stamped and this delta is folded into ash-install.sql.

create or replace function ash._register_wait(p_state text, p_type text, p_event text)
returns smallint
language plpgsql
set search_path = pg_catalog, ash
as $$
declare
  v_id     smallint;
  v_at_cap boolean;
begin
  -- Try to get existing
  select id into v_id
  from ash.wait_event_map
  where state = p_state and type = p_type and event = p_event;

  if v_id is not null then
    return v_id;
  end if;

  -- Enforce dictionary size cap before inserting. 32 000 stays well below
  -- smallint's 32 767 ceiling while still leaving room for genuine event
  -- diversity (real wait-event inventories measure in the hundreds).
  -- Use an exact existence probe for the 32 000th row instead of
  -- pg_class.reltuples: reltuples can be -1 or stale immediately after
  -- TRUNCATE/restore, which bypasses a hard cap until ANALYZE catches up.
  select exists (
    select 1 from ash.wait_event_map offset 31999 limit 1
  ) into v_at_cap;

  if v_at_cap then
    -- Bump the cap-hit counter so ash.status() can surface the drop.
    -- Note: counts *registration drops* here — not the number of sampled
    -- backends observed for this (state,type,event). Many concurrent
    -- backends blocked on the same dropped event only bump it once per tick.
    -- Wrap in an inner block: if the UPDATE itself fails (e.g. config row
    -- missing mid-uninstall), we still want the outer WARNING to fire and
    -- the function to return NULL without aborting take_sample().
    begin
      update ash.config set register_wait_cap_hits = register_wait_cap_hits + 1
        where singleton;
    exception when others then
      null;  -- counter bump is best-effort
    end;
    raise warning 'ash._register_wait: wait_event_map at cap (>= 32 000 rows); skipping (state=%, type=%, event=%) — see ash.status()',
      p_state, p_type, p_event;
    return null;  -- caller PERFORMs this; snapshot JOIN drops the session
  end if;

  -- Insert new entry
  insert into ash.wait_event_map (state, type, event)
  values (p_state, p_type, p_event)
  on conflict (state, type, event) do nothing
  returning id into v_id;

  -- If insert succeeded, return it
  if v_id is not null then
    return v_id;
  end if;

  -- Race condition: another session inserted, fetch it
  select id into v_id
  from ash.wait_event_map
  where state = p_state and type = p_type and event = p_event;

  return v_id;
end;
$$;
