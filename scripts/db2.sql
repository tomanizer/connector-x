begin
    declare continue handler for sqlstate '42704' begin end;
    execute immediate 'drop table cx_db2_test';
end@

create table cx_db2_test (
    id integer not null,
    small_v smallint,
    amount decimal(18, 2),
    name varchar(64),
    created_at timestamp,
    flag smallint
)@

insert into cx_db2_test (id, small_v, amount, name, created_at, flag)
with n(id) as (
    select integer(row_number() over())
    from syscat.columns c1, syscat.columns c2
    fetch first 10000 rows only
)
select
    id,
    smallint(mod(id, 32767)),
    decimal(id * 1.25, 18, 2),
    varchar('name-' || varchar(id), 64),
    current timestamp,
    smallint(mod(id, 2))
from n@

commit@
