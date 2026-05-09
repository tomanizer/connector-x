# Databases configuration and performance

ConnectorX supports retrieving data from Postgres, MsSQL, MySQL, Oracle, SQLite, BigQuery, Trino, ClickHouse, IBM Db2, Sybase, and generic ODBC sources. This chapter introduces how to use ConnectorX to connect each database and the conversion between database types and output types.

Generic ODBC, Sybase, and Db2 share the ODBC fetch path. They use the platform ODBC manager and require the target database ODBC driver to be installed separately at runtime. Start with the [ODBC](./databases/odbc.md) page for shared URL forms, driver setup, type behavior, testing, and performance tuning.

* [BigQuery](./databases/bigquery.md)
* [IBM Db2](./databases/db2.md)
* [MsSQL](./databases/mssql.md)
* [MySQL](./databases/mysql.md)
* [ODBC](./databases/odbc.md)
* [Oracle](./databases/oracle.md)
* [Postgres](./databases/postgres.md)
* [SQLite](./databases/sqlite.md)
* [Sybase](./databases/sybase.md)
* [Trino](./databases/trino.md)
* [ClickHouse](./databases/clickhouse.md)
