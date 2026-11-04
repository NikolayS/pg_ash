-- Run after each installer pass with owner_role set by the private fixture.
begin;
truncate ash.sample, ash.query_map_2 restart identity;
alter sequence ash.query_map_2_id_seq rename to ash_owner_renamed_identity;
alter sequence ash.ash_owner_renamed_identity start with :identity_start restart;
set role :"owner_role";
do $$
declare
  w smallint;
  result text;
  next_id int;
  identity_start int := (select seqstart from pg_sequence where seqrelid = 'ash.ash_owner_renamed_identity'::regclass);
begin
  w := ash._register_wait('active', 'CPU*', 'CPU*');
  insert into ash.query_map_2(query_id) values (909090), (909091);
  insert into ash.sample(sample_ts, datid, active_count, data, slot)
  select ash.ts_from_timestamptz(now() - interval '60 days'),
    (select oid from pg_database where datname = current_database()),
    1, array[-w, 1, 0]::int[], s
  from generate_series(0, 2, 2) as s;
  update ash.config set current_slot = 0,
    rotated_at = now() - interval '2 days';

  -- A caller rollback must also undo the privileged identity restart.
  begin
    result := ash.rotate();
    assert result = 'rotated: slot 0 -> 1, truncated slot 2 (sample + query_map)', result;
    insert into ash.query_map_2(query_id) values (909092) returning id into next_id;
    assert next_id = identity_start, 'rotation did not restart the renamed identity';
    raise exception using errcode = 'P0002', message = 'fixture rollback';
  exception when no_data_found then null;
  end;
  assert ash.current_slot() = 0, 'rollback retained the rotated slot';
  assert (select count(*) from ash.sample_2) = 1, 'rollback lost target samples';
  assert (select count(*) from ash.query_map_2) = 2, 'rollback lost target dictionary';
  next_id := nextval('ash.ash_owner_renamed_identity')::int;
  assert next_id = identity_start + 2, 'rollback did not restore identity state';

  result := ash.rotate();
  assert result = 'rotated: slot 0 -> 1, truncated slot 2 (sample + query_map)', result;
  assert ash.current_slot() = 1, 'rotation did not advance the slot';
  assert (select count(*) from ash.sample_2) = 0, 'target samples survived rotation';
  assert (select count(*) from ash.query_map_2) = 0, 'target dictionary survived rotation';
  assert (select count(*) from ash.sample_0) = 1, 'rotation removed another slot';
  insert into ash.query_map_2(query_id) values (909094) returning id into next_id;
  assert next_id = identity_start, 'rotation did not restart dictionary identity';
end $$;
reset role;
rollback;

-- Reject invalid slots before any sequence operation.
set role :"owner_role";
do $$ declare bad int; begin
  foreach bad in array array[null, -1, 32]::int[] loop
    begin
      perform ash._restart_query_map_identity(bad);
      raise exception 'invalid slot accepted: %', bad;
    exception when invalid_parameter_value then null;
    end;
  end loop;
end $$;
reset role;

-- Public, ordinary readers, and outsiders must not enter the definer helper.
set role ash_owner_reader;
do $$ begin
  begin
    perform ash._restart_query_map_identity(2);
    raise exception 'reader acquired identity helper access';
  exception when insufficient_privilege then null;
  end;
end $$;
reset role;
set role ash_owner_outsider;
do $$ begin
  begin
    perform ash._restart_query_map_identity(2);
    raise exception 'outsider acquired identity helper access';
  exception when insufficient_privilege then null;
  end;
end $$;
reset role;

-- A reset failure after TRUNCATE must undo data and configuration changes.
begin;
truncate ash.sample, ash.query_map_2 restart identity;
insert into ash.query_map_2(query_id) values (909090), (909091);
insert into ash.sample(sample_ts, datid, active_count, data, slot)
values (ash.ts_from_timestamptz(now() - interval '60 days'),
  (select oid from pg_database where datname = current_database()),
  1, array[-ash._register_wait('active', 'CPU*', 'CPU*')::int, 1, 0], 2);
update ash.config set current_slot = 0, rotated_at = now() - interval '2 days';
alter table ash.query_map_2 alter column id drop identity;
set role :"owner_role";
do $$ begin
  begin
    perform ash.rotate();
    raise exception 'rotation accepted a missing identity dependency';
  exception when insufficient_privilege then
    assert sqlerrm = 'ash._restart_query_map_identity: unsupported identity for slot 2', sqlerrm;
  end;
  assert ash.current_slot() = 0, 'failed reset advanced the slot';
  assert (select count(*) from ash.sample_2) = 1, 'failed reset lost samples';
  assert (select count(*) from ash.query_map_2) = 2, 'failed reset lost dictionary rows';
end $$;
reset role;
rollback;

-- A schema owner can replace its dictionary. Never run its trigger as definer.
begin;
set role :"owner_role";
drop table ash.query_map_2 cascade;
create table ash.query_map_2(id int generated always as identity primary key, query_id bigint unique);
create table ash.owner_rotation_audit(who name);
create function ash.owner_rotation_trigger() returns trigger language plpgsql as $fn$
begin
  insert into ash.owner_rotation_audit values (current_user);
  return null;
end $fn$;
create trigger owner_rotation_trigger after truncate on ash.query_map_2
  for each statement execute function ash.owner_rotation_trigger();
select ash._restart_query_map_identity(2);
do $$ begin
  assert (select count(*) from ash.owner_rotation_audit) = 0, 'identity helper ran a table trigger';
end $$;
update ash.config set current_slot = 0, rotated_at = now() - interval '2 days';
select ash.rotate();
do $$ begin
  assert (select count(*) from ash.owner_rotation_audit) = 1, 'rotation did not run the table trigger';
  assert (select who from ash.owner_rotation_audit) = current_user, 'rotation elevated a table trigger';
end $$;
-- A repopulating AFTER TRUNCATE trigger must not leave reset IDs behind rows.
create or replace function ash.owner_rotation_trigger() returns trigger language plpgsql as $fn$
begin
  insert into ash.query_map_2(query_id) values (909099);
  return null;
end $fn$;
insert into ash.query_map_2(query_id) values (909090);
update ash.config set current_slot = 0, rotated_at = now() - interval '2 days';
do $$ begin
  begin
    perform ash.rotate();
    raise exception 'rotation accepted a repopulating truncate trigger';
  exception when object_not_in_prerequisite_state then
    assert sqlerrm = 'ash.rotate: a truncate trigger repopulated the query dictionary; rotation rolled back', sqlerrm;
  end;
  assert ash.current_slot() = 0, 'trigger rejection advanced the slot';
  assert (select array_agg(query_id order by query_id) from ash.query_map_2) = array[909090]::bigint[],
    'trigger rejection changed dictionary rows';
end $$;
reset role;
rollback;

-- A plain same-name replacement must not be treated as an identity sequence.
begin;
set role :"owner_role";
drop table ash.query_map_2 cascade;
create table ash.query_map_2(id int, query_id bigint);
do $$ begin
  begin
    perform ash._restart_query_map_identity(2);
    raise exception 'identity helper accepted a plain replacement table';
  exception when insufficient_privilege then
    assert sqlerrm = 'ash._restart_query_map_identity: unsupported identity for slot 2', sqlerrm;
  end;
end $$;
reset role;
rollback;

-- An unrelated role's identity cannot become the privileged reset target.
begin;
drop table ash.query_map_2 cascade;
create table ash.query_map_2(id int generated always as identity primary key, query_id bigint);
alter table ash.query_map_2 owner to ash_owner_outsider;
set role :"owner_role";
do $$ begin
  begin
    perform ash._restart_query_map_identity(2);
    raise exception 'identity helper accepted an unrelated owner';
  exception when insufficient_privilege then
    assert sqlerrm = 'ash._restart_query_map_identity: unsupported identity for slot 2', sqlerrm;
  end;
end $$;
reset role;
rollback;
