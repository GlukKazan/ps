create or replace function ps_array_to_set(in p_array anyarray) returns setof anyelement
as $$
  select ($1)[s] from generate_series(1, array_upper($1, 1)) as s;
$$ language sql immutable;

create or replace function ps_trigger_regenerate(in p_table bigint) returns void
as $$
declare
  l_sql         text;
  l_table_name  varchar(50);
  l_date_column varchar(50);
  l_flag        boolean;
  tabs          record;
  columns       record;
begin
  select name into l_table_name
  from   ps_table where id = p_table;

  l_sql := 
 'create or replace function ps_' || l_table_name || '_insert_trigger() returns trigger ' ||
 'as $'|| '$ ' ||
 'begin ';
  for tabs in
    select a.snapshot_id as id,
           b.name as table_name,
           a.type_name as snapshot_type
    from   ps_snapshot a, ps_table b
    where  a.table_id = p_table
    and    b.id = a.snapshot_id
    loop
      l_flag = FALSE;
      l_sql := l_sql ||
     'update ' || tabs.table_name || ' set ';
      for columns in
        select name, parent_name, type_name
        from   ps_column
        where  table_id = tabs.id
        and    not type_name in ('date', 'key', 'nullable')
        loop
          if l_flag then
             l_sql := l_sql || ', ';
          end if;
          l_flag := TRUE;
          if columns.type_name = 'sum' then
             l_sql := l_sql ||
             columns.name || ' = ' || columns.name || ' + coalesce(NEW.' || columns.parent_name || ', 0) ';
          end if;
          if columns.type_name = 'min' then
             l_sql := l_sql ||
             columns.name || ' = least(coalesce(' || columns.name || ', NEW.' || columns.parent_name || '), coalesce(NEW.' || columns.parent_name || ', ' || columns.name || ')) ';
          end if;
          if columns.type_name = 'max' then
             l_sql := l_sql ||
             columns.name || ' = greatest(coalesce(' || columns.name || ', NEW.' || columns.parent_name || '), coalesce(NEW.' || columns.parent_name || ', ' || columns.name || ')) ';
          end if;
          if columns.type_name = 'cnt' then
             l_sql := l_sql ||
             columns.name || ' = ' || columns.name || ' + case when NEW.' || columns.parent_name || ' is null then 0 else 1 end ';
          end if;
        end loop;
      l_flag = FALSE;
      l_sql := l_sql || 'where ';
      for columns in
        select name, parent_name, type_name
        from   ps_column
        where  table_id = tabs.id
        and    type_name in ('date', 'key', 'nullable')
        loop
          if l_flag then
             l_sql := l_sql || 'and ';
          end if;
          l_flag := TRUE;
          if columns.type_name = 'date' then
             l_sql := l_sql ||
             columns.name || ' = date_trunc(lower(''' || tabs.snapshot_type || '''), NEW.' || columns.parent_name || ') ';
          end if;
          if columns.type_name = 'key' then
             l_sql := l_sql ||
             columns.name || ' = NEW.' || columns.parent_name || ' ';
          end if;
          if columns.type_name = 'nullable' then
             l_sql := l_sql ||
             columns.name || ' = coalesce(NEW.' || columns.parent_name || ', 0)';
          end if;
        end loop;
      l_sql := l_sql || '; ' ||
     'if not FOUND then ' ||
     'insert into ' || tabs.table_name || '(';
      l_flag = FALSE;
      for columns in
        select name, type_name
        from   ps_column
        where  table_id = tabs.id
        loop
          if l_flag then
             l_sql := l_sql || ', ';
          end if;
          l_flag := TRUE;
          l_sql := l_sql || columns.name;
        end loop;
      l_sql := l_sql || ') values (';
      l_flag = FALSE;
      for columns in
        select name, parent_name, type_name
        from   ps_column
        where  table_id = tabs.id
        loop
          if l_flag then
             l_sql := l_sql || ', ';
          end if;
          l_flag := TRUE;
          if columns.type_name = 'date' then
             l_sql := l_sql || 'date_trunc(lower(''' || tabs.snapshot_type || '''), NEW.' || columns.parent_name || ')';
          elsif columns.type_name = 'cnt' then
             l_sql := l_sql || 'case when NEW.' || columns.parent_name || ' is null then 0 else 1 end';
          elsif columns.type_name in ('nullable', 'sum') then
             l_sql := l_sql || 'coalesce(NEW.' || columns.parent_name || ', 0)';
          else
             l_sql := l_sql || 'NEW.' || columns.parent_name;
          end if;
        end loop;
      l_sql := l_sql || '); ' ||
     'end if; ';
    end loop;
    select name into l_date_column
    from   ps_column
    where  table_id = p_table
    and    type_name = 'date';
    for tabs in
      select to_char(start_value, 'YYYYMMDD') as start_value,
             to_char(end_value, 'YYYYMMDD') as end_value,
             type_name
      from   ps_range_partition
      where  table_id = p_table
      order  by start_value desc
      loop
        l_sql := l_sql ||
       'if NEW.' || l_date_column || ' >= to_date(''' || tabs.start_value || ''', ''YYYYMMDD'') and NEW.' || l_date_column || ' < to_date(''' || tabs.end_value || ''', ''YYYYMMDD'') then ' ||
          'insert into ' || l_table_name || '_' || tabs.start_value || ' values (NEW.*); ' ||
          'return null; ' ||
       'end if; ';
      end loop;
  l_sql := l_sql ||
 'return NEW; '||
 'end; '||
 '$'||'$ language plpgsql';
  execute l_sql;

  l_sql := 
 'create or replace function ps_' || l_table_name || '_raise_trigger() returns trigger ' ||
 'as $'|| '$ ' ||
 'begin ' ||
   'raise EXCEPTION ''Can''''t support % on MIN or MAX aggregate'', TG_OP;' ||
 'end; '||
 '$'||'$ language plpgsql';
  execute l_sql;

  l_sql := 
 'create or replace function ps_' || l_table_name || '_delete_trigger() returns trigger ' ||
 'as $'|| '$ ' ||
 'begin ';
  for tabs in
    select a.snapshot_id as id,
           b.name as table_name,
           a.type_name as snapshot_type
    from   ps_snapshot a, ps_table b
    where  a.table_id = p_table
    and    b.id = a.snapshot_id
    loop
      l_flag = FALSE;
      l_sql := l_sql ||
     'update ' || tabs.table_name || ' set ';
      for columns in
        select name, parent_name, type_name
        from   ps_column
        where  table_id = tabs.id
        and    type_name in ('sum', 'cnt')
        loop
          if l_flag then
             l_sql := l_sql || ', ';
          end if;
          l_flag := TRUE;
          if columns.type_name = 'sum' then
             l_sql := l_sql ||
             columns.name || ' = ' || columns.name || ' - OLD.' || columns.parent_name || ' ';
          end if;
          if columns.type_name = 'cnt' then
             l_sql := l_sql ||
             columns.name || ' = ' || columns.name || ' - case when OLD.' || columns.parent_name || ' is null then 0 else 1 end ';
          end if;
        end loop;
      l_flag = FALSE;
      l_sql := l_sql || 'where ';
      for columns in
        select name, parent_name, type_name
        from   ps_column
        where  table_id = tabs.id
        and    type_name in ('date', 'key', 'nullable')
        loop
          if l_flag  then
             l_sql := l_sql || 'and ';
          end if;
          l_flag := TRUE;
          if columns.type_name = 'date' then
             l_sql := l_sql ||
             columns.name || ' = date_trunc(lower(''' || tabs.snapshot_type || '''), NEW.' || columns.parent_name || ') ';
          end if;
          if columns.type_name = 'key' then
             l_sql := l_sql ||
             columns.name || ' = NEW.' || columns.parent_name || ' ';
          end if;
          if columns.type_name = 'nullable' then
             l_sql := l_sql ||
             columns.name || ' = coalesce(NEW.' || columns.parent_name || ', 0)';
          end if;
        end loop;
      l_sql := l_sql || '; ';
    end loop;
  l_sql := l_sql ||
 'return null; '||
 'end; '||
 '$'||'$ language plpgsql';
  execute l_sql;

  l_sql := 
 'create or replace function ps_' || l_table_name || '_update_trigger() returns trigger ' ||
 'as $'|| '$ ' ||
 'begin ';
  for tabs in
    select a.snapshot_id as id,
           b.name as table_name,
           a.type_name as snapshot_type
    from   ps_snapshot a, ps_table b
    where  a.table_id = p_table
    and    b.id = a.snapshot_id
    loop
      l_flag = FALSE;
      l_sql := l_sql ||
     'update ' || tabs.table_name || ' set ';
      for columns in
        select name, parent_name, type_name
        from   ps_column
        where  table_id = tabs.id
        and    type_name in ('sum', 'cnt')
        loop
          if l_flag then
             l_sql := l_sql || ', ';
          end if;
          l_flag := TRUE;
          if columns.type_name = 'sum' then
             l_sql := l_sql ||
             columns.name || ' = ' || columns.name || ' - OLD.' || columns.parent_name || ' + NEW.' || columns.parent_name || ' ';
          end if;
          if columns.type_name = 'cnt' then
             l_sql := l_sql ||
             columns.name || ' = ' || columns.name ||
             ' - case when OLD.' || columns.parent_name || ' is null then 0 else 1 end ' ||
             ' + case when NEW.' || columns.parent_name || ' is null then 0 else 1 end ';
          end if;
        end loop;
      l_flag = FALSE;
      l_sql := l_sql || 'where ';
      for columns in
        select name, parent_name, type_name
        from   ps_column
        where  table_id = tabs.id
        and    type_name in ('date', 'key', 'nullable')
        loop
          if l_flag then
             l_sql := l_sql || 'and ';
          end if;
          l_flag := TRUE;
          if columns.type_name = 'date' then
             l_sql := l_sql ||
             columns.name || ' = date_trunc(lower(''' || tabs.snapshot_type || '''), NEW.' || columns.parent_name || ') ';
          end if;
          if columns.type_name = 'key' then
             l_sql := l_sql ||
             columns.name || ' = NEW.' || columns.parent_name || ' ';
          end if;
          if columns.type_name = 'nullable' then
             l_sql := l_sql ||
             columns.name || ' = coalesce(NEW.' || columns.parent_name || ', 0)';
          end if;
        end loop;
      l_sql := l_sql || '; ';
    end loop;
  l_sql := l_sql ||
 'return null; '||
 'end; '||
 '$'||'$ language plpgsql';
  execute l_sql;
end;
$$ language plpgsql;

create or replace function ps_add_range_partition(in p_table varchar, in p_column varchar, in p_type varchar, in p_start date) returns void
as $$
declare
  l_sql       text;
  l_end       date;
  l_start_str varchar(10);
  l_end_str   varchar(10);
  l_table     bigint;
  l_flag      boolean;
  columns     record;
begin
  perform 1
  from   ps_table a, ps_column b
  where  a.id = b.table_id and lower(a.name) = lower(p_table)
  and    b.type_name = 'date' and lower(b.name) <> lower(p_column);
  if FOUND then
     raise EXCEPTION 'Conflict DATE columns';
  end if;

  l_end := p_start + ('1 ' || p_type)::INTERVAL;

  perform 1
  from   ps_table a, ps_range_partition b
  where  a.id = b.table_id and lower(a.name) = lower(p_table)
  and (( p_start >= b.start_value and p_start < b.end_value ) or
       ( b.start_value >= p_start and b.start_value < l_end ));
  if FOUND then
     raise EXCEPTION 'Range intervals intersects';
  end if;

  perform 1
  from   ps_table
  where  lower(name) = lower(p_table);
  if not FOUND then
     insert into ps_table(name) values (lower(p_table));
  end if;

  select id into l_table
  from   ps_table
  where  lower(name) = lower(p_table);

  perform 1
  from   ps_column
  where  table_id = l_table and type_name = 'date'
  and    lower(name) = lower(p_column);
  if not FOUND then
     insert into ps_column(table_id, name, type_name)
     values (l_table, lower(p_column), 'date');
  end if;

  insert into ps_range_partition(table_id, type_name, start_value, end_value)
  values (l_table, p_type, p_start, l_end);

  l_start_str = to_char(p_start, 'YYYYMMDD');
  l_end_str = to_char(l_end, 'YYYYMMDD');

  l_sql :=
 'create table ' || p_table || '_' || l_start_str || '(' ||
   'check (' || p_column || ' >= to_date(''' || l_start_str || ''', ''YYYYMMDD'') and ' ||
                p_column || ' < to_date(''' || l_end_str || ''', ''YYYYMMDD'')), ' ||
   'primary key (';

    l_flag := FALSE;
    for columns in
      select f.name as name
      from ( select ps_array_to_set(a.conkey) as nn
             from   pg_constraint a, pg_class b
             where  b.oid = a.conrelid
             and    a.contype = 'p'
             and    b.relname = p_table ) c, 
           ( select d.attname as name, d.attnum as nn
             from   pg_attribute d, pg_class e
             where  e.oid = d.attrelid
             and    e.relname = p_table ) f
      where  f.nn = c.nn
      order  by f.nn
      loop
        if l_flag then
           l_sql := l_sql || ', ';
        end if;
        l_flag := TRUE;
        l_sql := l_sql || columns.name;
      end loop;

  l_sql := l_sql ||
 ')) inherits (' || p_table || ')';
  execute l_sql;

  l_sql := 
 'create index ' || p_table || '_' || l_start_str || '_date on ' || p_table || '_' || l_start_str || '(' || p_column || ')';
  execute l_sql;

  perform ps_trigger_regenerate(l_table);

  execute 'drop trigger if exists ps_' || p_table || '_before_insert on ' || p_table;
  execute 'drop trigger if exists ps_' || p_table || '_after_update on '  || p_table;
  execute 'drop trigger if exists ps_' || p_table || '_after_delete on '  || p_table;

  l_sql := 
 'insert into ' || p_table || '_' || l_start_str || ' ' ||
 'select * from ' || p_table || ' where ' ||
  p_column || ' >= to_date(''' || l_start_str || ''', ''YYYYMMDD'') and ' ||
  p_column || ' < to_date(''' || l_end_str || ''', ''YYYYMMDD'')';
  execute l_sql;

  l_sql := 
 'delete from only ' || p_table || ' where ' ||
  p_column || ' >= to_date(''' || l_start_str || ''', ''YYYYMMDD'') and ' ||
  p_column || ' < to_date(''' || l_end_str || ''', ''YYYYMMDD'')';
  execute l_sql;

  l_sql := 
 'create trigger ps_' || p_table || '_before_insert ' ||
 'before insert on ' || p_table || ' for each row ' ||
 'execute procedure ps_' || p_table || '_insert_trigger()';
  execute l_sql;
  perform 1
  from   ps_snapshot a, ps_column b
  where  b.table_id = a.snapshot_id and a.table_id = l_table
  and    b.type_name in ('min', 'max');
  if FOUND then
     l_sql := 
    'create trigger ps_' || p_table || '_after_update ' ||
    'after update on ' || p_table || ' for each row ' ||
    'execute procedure ps_' || p_table || '_raise_trigger()';
     execute l_sql;
     l_sql := 
    'create trigger ps_' || p_table || '_after_delete ' ||
    'after delete on ' || p_table || ' for each row ' ||
    'execute procedure ps_' || p_table || '_raise_trigger()';
     execute l_sql;
     l_sql := 
    'create trigger ps_' || p_table || '_' || l_start_str || '_after_update ' ||
    'after update on ' || p_table || '_' || l_start_str || ' for each row ' ||
    'execute procedure ps_' || p_table || '_raise_trigger()';
     execute l_sql;
     l_sql := 
    'create trigger ps_' || p_table || '_' || l_start_str || '_after_delete ' ||
    'after delete on ' || p_table || '_' || l_start_str || ' for each row ' ||
    'execute procedure ps_' || p_table || '_raise_trigger()';
     execute l_sql;
  else
     l_sql := 
    'create trigger ps_' || p_table || '_after_update ' ||
    'after update on ' || p_table || ' for each row ' ||
    'execute procedure ps_' || p_table || '_update_trigger()';
     execute l_sql;
     l_sql := 
    'create trigger ps_' || p_table || '_after_delete ' ||
    'after delete on ' || p_table || ' for each row ' ||
    'execute procedure ps_' || p_table || '_delete_trigger()';
     execute l_sql;
     l_sql := 
    'create trigger ps_' || p_table || '_' || l_start_str || '_after_update ' ||
    'after update on ' || p_table || '_' || l_start_str || ' for each row ' ||
    'execute procedure ps_' || p_table || '_update_trigger()';
     execute l_sql;
     l_sql := 
    'create trigger ps_' || p_table || '_' || l_start_str || '_after_delete ' ||
    'after delete on ' || p_table || '_' || l_start_str || ' for each row ' ||
    'execute procedure ps_' || p_table || '_delete_trigger()';
     execute l_sql;
  end if;
end;
$$ language plpgsql;

create or replace function ps_del_range_partition(in p_table varchar, in p_start date) returns void
as $$
declare
  l_sql       text;
  l_start_str varchar(10);
  l_table     bigint;
begin
  select id into l_table
  from   ps_table
  where  lower(name) = lower(p_table);

  l_start_str = to_char(p_start, 'YYYYMMDD');

  delete from ps_range_partition 
  where  table_id = l_table
  and    start_value = p_start;

  perform ps_trigger_regenerate(l_table);

  l_sql := 
 'insert into ' || p_table || ' ' ||
 'select * from ' || p_table || '_' || l_start_str;
  execute l_sql;

  perform 1
  from ( select 1
         from   ps_range_partition
         where  table_id = l_table
         union  all
         select 1
         from   ps_snapshot
         where  table_id = l_table ) a;
  if not FOUND then
     execute 'drop trigger if exists ps_' || p_table || '_before_insert on ' || p_table;
     execute 'drop trigger if exists ps_' || p_table || '_after_update on '  || p_table;
     execute 'drop trigger if exists ps_' || p_table || '_after_delete on '  || p_table;

     execute 'drop function ps_' || p_table || '_insert_trigger() cascade';
     execute 'drop function ps_' || p_table || '_raise_trigger()  cascade';
     execute 'drop function ps_' || p_table || '_update_trigger() cascade';
     execute 'drop function ps_' || p_table || '_delete_trigger() cascade';

     delete from ps_column where table_id = l_table;
     delete from ps_table where id = l_table;
  end if;

  perform 1
  from   ps_range_partition
  where  table_id = l_table;
  if not FOUND then
     delete from ps_column 
     where  table_id = l_table
     and    type_name = 'date';
  end if;

  execute 'drop table ' || p_table || '_' || l_start_str;
end;
$$ language plpgsql;

create or replace function ps_add_snapshot_column(in p_snapshot varchar, in p_column varchar, in p_parent varchar, in p_type varchar) returns void
as $$
declare
  l_table bigint;
begin
  perform 1
  from   ps_table
  where  lower(name) = lower(p_snapshot);
  if not FOUND then
     insert into ps_table(name) values (lower(p_snapshot));
  end if;

  select id into l_table
  from   ps_table
  where  lower(name) = lower(p_snapshot);

  insert into ps_column(table_id, name, parent_name, type_name)
  values (l_table, lower(p_column), lower(p_parent), p_type);
end;
$$ language plpgsql;

create or replace function ps_add_snapshot_column(in p_snapshot varchar, in p_column varchar, in p_type varchar) returns void
as $$
begin
  perform ps_add_snapshot_column(p_snapshot, p_column, p_column, p_type);
end;
$$ language plpgsql;

create or replace function ps_add_snapshot(in p_table varchar, in p_snapshot varchar, in p_type varchar) returns void
as $$
declare
  l_sql      text;
  l_table    bigint;
  l_snapshot bigint;
  l_flag     boolean;
  columns    record;
begin

  select id into l_snapshot
  from   ps_table
  where  lower(name) = lower(p_snapshot);

  perform 1
  from   ps_column
  where  table_id = l_snapshot
  and    type_name in ('date', 'key');
  if not FOUND then
     raise EXCEPTION 'Key columns not found';
  end if;

  perform 1
  from   ps_column
  where  table_id = l_snapshot
  and    not type_name in ('date', 'key', 'nullable');
  if not FOUND then
     raise EXCEPTION 'Aggregate columns not found';
  end if;

  perform 1
  from   ps_table
  where  lower(name) = lower(p_table);
  if not FOUND then
     insert into ps_table(name) values (lower(p_table));
  end if;

  select id into l_table
  from   ps_table
  where  lower(name) = lower(p_table);

  insert into ps_snapshot(table_id, snapshot_id, type_name)
  values (l_table, l_snapshot, p_type);

  perform ps_trigger_regenerate(l_table);

  l_sql := 'create table ' || p_snaphot || ' (';
  l_flag := FALSE;
  for columns in
    select name, type_name
    from   ps_column
    where  table_id = l_snapshot
    loop
      if l_flag then
         l_sql := l_sql || ', ';
      end if;
      l_flag := TRUE;
      if columns.type_name = 'date' then
         l_sql := l_sql || columns.name || ' date not null';
      else
         l_sql := l_sql || columns.name || ' bigint not null';
      end if;
    end loop;
  l_sql := l_sql || ', primary key (';
  l_flag := FALSE;
  for columns in
    select name
    from   ps_column
    where  table_id = l_snapshot
    and    type_name in ('date', 'key', 'nullable')
    loop
      if l_flag then
         l_sql := l_sql || ', ';
      end if;
      l_flag := TRUE;
      l_sql := l_sql || columns.name;
    end loop;
  l_sql := l_sql || '))';
  execute l_sql;

  execute 'drop trigger if exists ps_' || p_table || '_before_insert on ' || p_table;
  execute 'drop trigger if exists ps_' || p_table || '_after_update on '  || p_table;
  execute 'drop trigger if exists ps_' || p_table || '_after_delete on '  || p_table;

  l_sql := 
 'create trigger ps_' || p_table || '_before_insert ' ||
 'before insert on ' || p_table || ' for each row ' ||
 'execute procedure ps_' || p_table || '_insert_trigger()';
  execute l_sql;

  perform 1
  from   ps_snapshot a, ps_column b
  where  b.table_id = a.snapshot_id and a.table_id = l_table
  and    b.type_name in ('min', 'max');
  if FOUND then
     l_sql := 
    'create trigger ps_' || p_table || '_after_update ' ||
    'after update on ' || p_table || ' for each row ' ||
    'execute procedure ps_' || p_table || '_raise_trigger()';
     execute l_sql;
     l_sql := 
    'create trigger ps_' || p_table || '_after_delete ' ||
    'after delete on ' || p_table || ' for each row ' ||
    'execute procedure ps_' || p_table || '_raise_trigger()';
     execute l_sql;
  else
     l_sql := 
    'create trigger ps_' || p_table || '_after_update ' ||
    'after update on ' || p_table || ' for each row ' ||
    'execute procedure ps_' || p_table || '_update_trigger()';
     execute l_sql;
     l_sql := 
    'create trigger ps_' || p_table || '_after_delete ' ||
    'after delete on ' || p_table || ' for each row ' ||
    'execute procedure ps_' || p_table || '_delete_trigger()';
     execute l_sql;
  end if;

  l_sql := 'insert into ' || p_snaphot || '(';
  l_flag := FALSE;
  for columns in
    select name
    from   ps_column
    where  table_id = l_snapshot
    loop
      if l_flag then
         l_sql := l_sql || ', ';
      end if;
      l_flag := TRUE;
      l_sql := l_sql || columns.name;
    end loop;
  l_sql := l_sql || ') select ';
  l_flag := FALSE;
  for columns in
    select parent_name as name, type_name
    from   ps_column
    where  table_id = l_snapshot
    loop
      if l_flag then
         l_sql := l_sql || ', ';
      end if;
      l_flag := TRUE;
      if columns.type_name = 'date' then
         l_sql := l_sql || 'date_trunc(lower(''' || p_type || '''), ' || columns.name || ')';
      end if;
      if columns.type_name = 'key' then
         l_sql := l_sql || columns.name;
      end if;
      if columns.type_name = 'nullable' then
         l_sql := l_sql || 'coalesce(' || columns.name || ', 0)';
      end if;
      if columns.type_name = 'sum' then
         l_sql := l_sql || 'sum(' || columns.name || ')';
      end if;
      if columns.type_name = 'min' then
         l_sql := l_sql || 'min(' || columns.name || ')';
      end if;
      if columns.type_name = 'max' then
         l_sql := l_sql || 'max(' || columns.name || ')';
      end if;
      if columns.type_name = 'cnt' then
         l_sql := l_sql || 'count(' || columns.name || ')';
      end if;
    end loop;
  l_sql := l_sql || 'from ' || p_table || ' group by ';
  l_flag := FALSE;
  for columns in
    select parent_name as name, type_name
    from   ps_column
    where  table_id = l_snapshot
    and    type_name in ('date', 'key', 'nullable')
    loop
      if l_flag then
         l_sql := l_sql || ', ';
      end if;
      l_flag := TRUE;
      if columns.type_name = 'date' then
         l_sql := l_sql || 'date_trunc(lower(''' || p_type || '''), ' || columns.name || ')';
      else
         l_sql := l_sql || columns.name;
      end if;
    end loop;
  execute l_sql;
end;
$$ language plpgsql;

create or replace function ps_del_snapshot(in p_snapshot varchar) returns void
as $$
declare
  l_sql      text;
  p_table    varchar(50);
  l_table    bigint;
  l_snapshot bigint;
begin
  select a.table_id, c.name into l_table, p_table
  from   ps_snapshot a, ps_table b, ps_table c
  where  b.id = a.snapshot_id and c.id = a.table_id
  and    lower(b.name) = lower(p_snapshot);

  select id into l_snapshot
  from   ps_table
  where  lower(name) = lower(p_snapshot);

  delete from ps_snapshot where snapshot_id = l_snapshot;
  delete from ps_column where table_id = l_snapshot;
  delete from ps_table where id = l_snapshot;

  execute 'drop trigger if exists ps_' || p_table || '_before_insert on ' || p_table;
  execute 'drop trigger if exists ps_' || p_table || '_after_update  on ' || p_table;
  execute 'drop trigger if exists ps_' || p_table || '_after_delete  on ' || p_table;
  
  perform 1
  from ( select 1
         from   ps_range_partition
         where  table_id = l_table
         union  all
         select 1
         from   ps_snapshot
         where  table_id = l_table ) a;
  if not FOUND then
     execute 'drop function if exists ps_' || p_table || '_insert_trigger() cascade';
     execute 'drop function if exists ps_' || p_table || '_raise_trigger()  cascade';
     execute 'drop function if exists ps_' || p_table || '_update_trigger() cascade';
     execute 'drop function if exists ps_' || p_table || '_delete_trigger() cascade';
  else
     perform ps_trigger_regenerate(l_table);

     l_sql := 
    'create trigger ps_' || p_table || '_before_insert ' ||
    'before insert on ' || p_table || ' for each row ' ||
    'execute procedure ps_' || p_table || '_insert_trigger()';
     execute l_sql;

     perform 1
     from   ps_snapshot a, ps_column b
     where  b.table_id = a.snapshot_id and a.table_id = l_table
     and    b.type_name in ('min', 'max');
     if FOUND then
        l_sql := 
       'create trigger ps_' || p_table || '_after_update ' ||
       'after update on ' || p_table || ' for each row ' ||
       'execute procedure ps_' || p_table || '_raise_trigger()';
        execute l_sql;
        l_sql := 
       'create trigger ps_' || p_table || '_after_delete ' ||
       'after delete on ' || p_table || ' for each row ' ||
       'execute procedure ps_' || p_table || '_raise_trigger()';
        execute l_sql;
     else
        l_sql := 
       'create trigger ps_' || p_table || '_after_update ' ||
       'after update on ' || p_table || ' for each row ' ||
       'execute procedure ps_' || p_table || '_update_trigger()';
        execute l_sql;
        l_sql := 
       'create trigger ps_' || p_table || '_after_delete ' ||
       'after delete on ' || p_table || ' for each row ' ||
       'execute procedure ps_' || p_table || '_delete_trigger()';
        execute l_sql;
     end if;
  end if;

  execute 'drop table if exists ' || p_snapshot;
end;
$$ language plpgsql;
