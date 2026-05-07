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
