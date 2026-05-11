if object_id('dbo.cx_odbc_edge') is not null
    drop table dbo.cx_odbc_edge
go

create table dbo.cx_odbc_edge (
    id int not null,
    flag bit not null,
    name varchar(16) not null,
    amount numeric(18, 4) not null,
    created_at datetime not null,
    event_time bigtime not null,
    payload varbinary(16) not null,
    wide_text univarchar(64) not null,
    nullable_text varchar(64) null,
    long_text text not null
)
go

insert into dbo.cx_odbc_edge (
    id,
    flag,
    name,
    amount,
    created_at,
    event_time,
    payload,
    wide_text,
    nullable_text,
    long_text
)
values (
    1,
    1,
    'alpha',
    123.4567,
    '2024-01-01 12:34:56.123',
    '13:14:15.123456',
    0x000102ff,
    'Grusse Tokyo',
    null,
    replicate('x', 64)
)
go

insert into dbo.cx_odbc_edge (
    id,
    flag,
    name,
    amount,
    created_at,
    event_time,
    payload,
    wide_text,
    nullable_text,
    long_text
)
values (
    2,
    0,
    'beta',
    -9.0001,
    '2024-01-02 00:00:00.000',
    '00:00:01.000001',
    0x68656c6c6f,
    'plain',
    'present',
    'short'
)
go

if object_id('dbo.cx_odbc_temporal_edge') is not null
    drop table dbo.cx_odbc_temporal_edge
go

create table dbo.cx_odbc_temporal_edge (
    id int not null,
    date_v date null,
    time_v time null,
    datetime_v datetime null,
    smalldatetime_v smalldatetime null,
    bigtime_v bigtime null,
    bigdatetime_v bigdatetime null,
    row_version timestamp
)
go

insert into dbo.cx_odbc_temporal_edge (
    id,
    date_v,
    time_v,
    datetime_v,
    smalldatetime_v,
    bigtime_v,
    bigdatetime_v
)
values (
    1,
    '2024-02-03',
    '03:04:05',
    '2024-02-03 04:05:06.123',
    '2024-02-03 04:05',
    '13:14:15.123456',
    '2024-02-03 04:05:06.123456'
)
go

insert into dbo.cx_odbc_temporal_edge (id)
values (2)
go

if object_id('dbo.cx_odbc_binary_edge') is not null
    drop table dbo.cx_odbc_binary_edge
go

create table dbo.cx_odbc_binary_edge (
    id int not null,
    fixed_bytes binary(8) null,
    variable_bytes varbinary(8) null,
    image_bytes image null,
    row_version timestamp
)
go

insert into dbo.cx_odbc_binary_edge (
    id,
    fixed_bytes,
    variable_bytes,
    image_bytes
)
values (
    1,
    0x000102030405feff,
    0x1020304050,
    0x00010203040506070809a0b0c0d0e0ff
)
go

insert into dbo.cx_odbc_binary_edge (
    id,
    fixed_bytes,
    variable_bytes,
    image_bytes
)
values (
    2,
    null,
    null,
    null
)
go
