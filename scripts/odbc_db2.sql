begin
    declare continue handler for sqlstate '42704' begin end;
    execute immediate 'drop table cx_odbc_test';
end@

create table cx_odbc_test (
    id integer not null primary key,
    flag smallint not null,
    name varchar(32) not null
)@

insert into cx_odbc_test (id, flag, name)
values
    (1, 1, 'alpha'),
    (2, 0, 'beta')@

begin
    declare continue handler for sqlstate '42704' begin end;
    execute immediate 'drop table cx_odbc_edge';
end@

create table cx_odbc_edge (
    id integer not null primary key,
    amount decimal(18, 4) not null,
    created_at timestamp not null,
    event_time time not null,
    payload blob(16) not null,
    wide_text varchar(64) not null,
    nullable_text varchar(64),
    long_text varchar(128) not null,
    decfloat_text decfloat(16) not null,
    xml_text xml not null,
    graphic_text vargraphic(32) not null
)@

insert into cx_odbc_edge (
    id,
    amount,
    created_at,
    event_time,
    payload,
    wide_text,
    nullable_text,
    long_text,
    decfloat_text,
    xml_text,
    graphic_text
)
values
    (
        1,
        123.4567,
        timestamp('2024-01-01-12.34.56.123456'),
        time('13:14:15.123456'),
        blob(x'000102ff'),
        'Grüße 東京',
        null,
        repeat('x', 64),
        decfloat(123.45),
        xmlparse(document '<root>alpha</root>' preserve whitespace),
        vargraphic('東京')
    ),
    (
        2,
        -9.0001,
        timestamp('2024-01-02-00.00.00.000001'),
        time('00:00:01.000001'),
        blob(x'68656c6c6f'),
        'plain',
        'present',
        'short',
        decfloat(-9.0001),
        xmlparse(document '<root>beta</root>' preserve whitespace),
        vargraphic('plain')
    )@

begin
    declare continue handler for sqlstate '42704' begin end;
    execute immediate 'drop table cx_odbc_perf';
end@

create table cx_odbc_perf (
    id integer not null primary key,
    flag smallint not null,
    int_v integer not null,
    bigint_v bigint not null,
    real_v real not null,
    double_v double not null,
    amount decimal(18, 4) not null,
    name varchar(64) not null,
    payload varchar(128) not null,
    created_at timestamp not null
)@

insert into cx_odbc_perf (
    id,
    flag,
    int_v,
    bigint_v,
    real_v,
    double_v,
    amount,
    name,
    payload,
    created_at
)
with recursive g(v) as (
    select 1 from sysibm.sysdummy1
    union all
    select v + 1 from g where v < 100000
)
select
    v,
    smallint(mod(v, 2)),
    v * 3,
    bigint(v) * 100000,
    double(v) / 3,
    double(v) / 7,
    decimal(v / 11.0, 18, 4),
    varchar('name-' || varchar(v), 64),
    repeat('x', 64),
    timestamp('2024-01-01-00.00.00') + (v - 1) seconds
from g@

commit@
