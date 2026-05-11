create table cx_odbc_edge (
    id integer not null primary key,
    flag smallint not null,
    name varchar(16) not null,
    amount decimal(18, 4) not null,
    created_at timestamp not null,
    event_time time not null,
    payload varbinary(16) not null,
    wide_text varchar(64) not null,
    nullable_text varchar(64),
    long_text varchar(128) not null,
    decfloat_v decfloat(16),
    xml_v xml
);

insert into cx_odbc_edge (
    id,
    flag,
    name,
    amount,
    created_at,
    event_time,
    payload,
    wide_text,
    nullable_text,
    long_text,
    decfloat_v,
    xml_v
)
values
    (
        1,
        1,
        'alpha',
        decimal(123.4567, 18, 4),
        timestamp('2024-01-01 12:34:56.123456'),
        time('13:14:15'),
        cast(X'000102FF' as varbinary(16)),
        'Grusse Tokyo',
        null,
        repeat('x', 64),
        decfloat(123.5),
        xmlparse(document '<root>alpha</root>')
    );

insert into cx_odbc_edge (
    id,
    flag,
    name,
    amount,
    created_at,
    event_time,
    payload,
    wide_text,
    nullable_text,
    long_text,
    decfloat_v,
    xml_v
)
values
    (
        2,
        0,
        'beta',
        decimal(-9.0001, 18, 4),
        timestamp('2024-01-02 00:00:00.000001'),
        time('00:00:01'),
        cast(X'68656C6C6F' as varbinary(16)),
        'plain',
        'present',
        'short',
        decfloat(-0.25),
        xmlparse(document '<root>beta</root>')
    );

create table cx_db2_type_edge (
    id integer not null primary key,
    decfloat16_v decfloat(16),
    decfloat34_v decfloat(34),
    xml_v xml,
    clob_v clob(2K),
    blob_v blob(2K),
    graphic_v graphic(16),
    vargraphic_v vargraphic(64)
);

insert into cx_db2_type_edge (
    id,
    decfloat16_v,
    decfloat34_v,
    xml_v,
    clob_v,
    blob_v,
    graphic_v,
    vargraphic_v
)
values
    (
        1,
        decfloat(123.5),
        cast(decfloat(9876543210.123456) as decfloat(34)),
        xmlparse(document '<root><name>alpha</name></root>'),
        cast(repeat('clob-value-', 64) as clob(2K)),
        cast(X'000102FF' as blob(2K)),
        cast('wide-alpha' as graphic(16)),
        cast('varwide-alpha' as vargraphic(64))
    );

insert into cx_db2_type_edge (
    id,
    decfloat16_v,
    decfloat34_v,
    xml_v,
    clob_v,
    blob_v,
    graphic_v,
    vargraphic_v
)
values
    (
        2,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
