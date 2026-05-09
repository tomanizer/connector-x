if object_id('dbo.cx_odbc_test', 'U') is not null
    drop table dbo.cx_odbc_test

go

create table dbo.cx_odbc_test (
    id int not null primary key,
    flag bit not null,
    name varchar(32) not null
)

go

insert into dbo.cx_odbc_test (id, flag, name)
values
    (1, 1, 'alpha'),
    (2, 0, 'beta')

go

if object_id('dbo.cx_odbc_edge', 'U') is not null
    drop table dbo.cx_odbc_edge

go

create table dbo.cx_odbc_edge (
    id int not null primary key,
    amount numeric(18, 4) not null,
    created_at datetime not null,
    event_time bigtime not null,
    payload varbinary(128) not null,
    wide_text univarchar(64) not null,
    nullable_text varchar(64) null,
    long_text varchar(128) not null,
    time2_v bigtime not null,
    nullable_bit bit null
)

go

insert into dbo.cx_odbc_edge (
    id,
    amount,
    created_at,
    event_time,
    payload,
    wide_text,
    nullable_text,
    long_text,
    time2_v,
    nullable_bit
)
values
    (
        1,
        123.4567,
        convert(datetime, '2024-01-01 12:34:56.123'),
        convert(bigtime, '13:14:15.123456'),
        0x000102ff,
        convert(univarchar(64), N'Grüße 東京'),
        null,
        replicate('x', 64),
        convert(bigtime, '13:14:15.123456'),
        case when 1 = 1 then cast(null as bit) else convert(bit, 1) end
    ),
    (
        2,
        -9.0001,
        convert(datetime, '2024-01-02 00:00:00.000'),
        convert(bigtime, '00:00:01.000000'),
        0x68656c6c6f,
        convert(univarchar(64), N'plain'),
        'present',
        'short',
        convert(bigtime, '00:00:01.000000'),
        convert(bit, 1)
    )

go

if object_id('dbo.cx_odbc_perf', 'U') is not null
    drop table dbo.cx_odbc_perf

go

create table dbo.cx_odbc_perf (
    id int not null primary key,
    flag bit not null,
    int_v int not null,
    bigint_v bigint not null,
    real_v real not null,
    double_v float not null,
    amount numeric(18, 4) not null,
    name varchar(64) not null,
    payload varchar(128) not null,
    created_at datetime not null
)

go

with nums as (
    select top 100000 row_number() over(order by a.id, b.id) as id
    from sysobjects a
    cross join sysobjects b
)
insert into dbo.cx_odbc_perf (
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
select
    id,
    convert(bit, id % 2),
    id * 3,
    convert(bigint, id) * 100000,
    convert(real, id) / 3,
    convert(float, id) / 7,
    convert(numeric(18, 4), convert(float, id) / 11),
    'name-' + convert(varchar(16), id),
    replicate('x', 64),
    dateadd(second, id - 1, convert(datetime, '2024-01-01 00:00:00'))
from nums

go
