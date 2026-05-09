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
