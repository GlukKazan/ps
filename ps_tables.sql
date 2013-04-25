create sequence ps_table_seq;

create table    ps_table (
  id            bigint         default nextval('ps_table_seq') not null,
  name          varchar(50)    not null unique,
  primary key(id)
);

create sequence ps_column_seq;

create table    ps_column (
  id            bigint         default nextval('ps_column_seq') not null,
  table_id      bigint         not null references ps_table(id),
  name          varchar(50)    not null,
  parent_name   varchar(50),
  type_name     varchar(8)     not null check (type_name in ('date', 'key', 'nullable', 'sum', 'min', 'max', 'cnt')),
  unique (table_id, name),
  primary key(id)
);

create table    ps_range_partition (
  table_id      bigint         not null references ps_table(id),
  type_name     varchar(10)    not null check (type_name in ('day', 'week', 'month', 'year')),
  start_value   date           not null,
  end_value     date           not null,
  primary key(table_id, start_value)
);

create table    ps_snapshot (
  snapshot_id   bigint         not null references ps_table(id),
  table_id      bigint         not null references ps_table(id),
  type_name     varchar(10)    not null check (type_name in ('day', 'week', 'month', 'year')),
  primary key(snapshot_id)
);
