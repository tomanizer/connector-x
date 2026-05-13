drop table if exists cx_odbc_test;

create table cx_odbc_test (
    id integer primary key,
    flag integer not null,
    name varchar(32) not null
);

insert into cx_odbc_test (id, flag, name)
values
    (1, 1, 'alpha'),
    (2, 0, 'beta');

drop table if exists cx_odbc_edge;

create table cx_odbc_edge (
    id integer primary key,
    amount numeric(18, 4) not null,
    created_at timestamp not null,
    event_time time not null,
    payload bytea not null,
    wide_text varchar(64) not null,
    nullable_text varchar(64),
    long_text varchar(128) not null
);

insert into cx_odbc_edge (
    id,
    amount,
    created_at,
    event_time,
    payload,
    wide_text,
    nullable_text,
    long_text
)
values
    (
        1,
        123.4567,
        timestamp '2024-01-01 12:34:56.123456',
        time '13:14:15.123456',
        decode('000102ff', 'hex'),
        'Grüße 東京',
        null,
        repeat('x', 64)
    ),
    (
        2,
        -9.0001,
        timestamp '2024-01-02 00:00:00.000001',
        time '00:00:01.000001',
        decode('68656c6c6f', 'hex'),
        'plain',
        'present',
        'short'
    );

drop table if exists cx_odbc_perf;

create table cx_odbc_perf (
    id integer primary key,
    flag integer not null,
    int_v integer not null,
    bigint_v bigint not null,
    real_v real not null,
    double_v double precision not null,
    amount numeric(18, 4) not null,
    name varchar(64) not null,
    payload varchar(128) not null,
    payload_bytes bytea not null,
    created_at timestamp not null
);

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
    payload_bytes,
    created_at
)
select
    g,
    g % 2,
    g * 3,
    g::bigint * 100000,
    g::real / 3,
    g::double precision / 7,
    (g::numeric / 11)::numeric(18, 4),
    format('name-%s', g),
    repeat('x', 64),
    decode(repeat('78', 64), 'hex'),
    timestamp '2024-01-01 00:00:00' + (g || ' seconds')::interval
from generate_series(1, 100000) as g;
