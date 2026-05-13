# Basic usage
ConnectorX enables you to run SQL queries and load data from databases into Python in the fastest and most memory efficient way.

## API
```python
connectorx.read_sql(
    conn: Union[str, ConnectionUrl, Dict[str, Union[str, ConnectionUrl]]],
    query: Union[List[str], str],
    *,
    return_type: str = "pandas",
    protocol: Optional[str] = None,
    partition_on: Optional[str] = None,
    partition_range: Optional[Tuple[int, int]] = None,
    partition_num: Optional[int] = None,
    index_col: Optional[str] = None,
    strategy: Optional[str] = None,
    pre_execution_query: Optional[Union[str, List[str]]] = None,
    **kwargs,
)
```

## Parameters
- `conn: Union[str, ConnectionUrl, Dict[str, Union[str, ConnectionUrl]]]`: Connection string URI, `ConnectionUrl`, raw ODBC connection string, or dict of database names (key) and connection strings or `ConnectionUrl` objects (value) for querying multiple databases.
  - Please check out [here](https://sfu-db.github.io/connector-x/databases.html) for connection string examples of each database
- `query: Union[str, List[str]]`: SQL query or list of partitioned SQL queries for fetching data.
- `return_type: str = "pandas"`: The return type of this function. It can be `arrow`, `arrow_stream`, `pandas`, `modin`, `dask` or `polars`.
- `protocol: Optional[str]`: The protocol used to fetch data from source. When omitted, ConnectorX chooses the default protocol for the backend. Check out [here](./databases.md) to see more details.
- `partition_on: Optional[str]`: The column to partition the result.
- `partition_range: Optional[Tuple[int, int]]`: The value range of the partition column.
- `partition_num: Optional[int]`: The number of partitions to generate.
- `index_col: Optional[str]`: The index column to set for the result dataframe. Only applicable when `return_type` is `pandas`, `modin` or `dask`. 
- `strategy: Optional[str]`: Strategy of rewriting the federated query for join pushdown.
- `pre_execution_query: Optional[Union[str, List[str]]]`: SQL query or list of SQL queries executed before the main query. Can be used to set runtime configurations using `SET` statements. Supported for PostgreSQL and MySQL dispatcher routes, and for generic ODBC, Sybase, and Db2 when `return_type` is `arrow` or `arrow_stream`.
- `**kwargs`: Additional backend options. For `return_type="arrow_stream"`, pass `batch_size: int` to set the maximum number of rows in each streamed batch. When omitted, `batch_size` defaults to `10000`.

Generic ODBC, Sybase, and IBM Db2 currently use the Rust Arrow route. Use `return_type="arrow"` or `return_type="arrow_stream"` for these sources, then convert to pandas with `table.to_pandas()` when needed.

For generic ODBC, Sybase, and Db2 Arrow routes, `pre_execution_query` is executed on every ODBC connection ConnectorX opens for the read: once before metadata discovery and once before each partition query fetch. Use explicit `partition_range` values if the partition-range discovery query itself depends on session-local objects created by pre-execution SQL.

## `ConnectionUrl`

`ConnectionUrl` helps build URL-style connection strings without hand-encoding every component. It supports SQLite, BigQuery, server-style backends, and generic ODBC.

Generic ODBC requires exactly one of `driver` or `dsn`:

```python
from connectorx import ConnectionUrl

conn = ConnectionUrl(
    backend="odbc",
    driver="PostgreSQL Unicode",
    username="connectorx",
    password="connectorx",
    server="127.0.0.1",
    port=5432,
    database="connectorx",
)
```

DSN-only ODBC connections can omit the server fields. If credentials are supplied without a server, ConnectorX encodes them as `UID` and `PWD` ODBC options:

```python
from connectorx import ConnectionUrl

conn = ConnectionUrl(
    backend="odbc",
    dsn="Warehouse DSN",
    username="connectorx",
    password="connectorx",
)
```

Raw ODBC connection strings are also accepted directly when you need exact driver-specific keywords:

```python
import connectorx as cx

conn = "Driver={SQLite3};Database=/tmp/example.db;"
table = cx.read_sql(conn, "select * from example", return_type="arrow")
```

## Examples
- Read a DataFrame from a SQL using a single thread

  ```python
  import connectorx as cx

  postgres_url = "postgresql://username:password@server:port/database"
  query = "SELECT * FROM lineitem"

  cx.read_sql(postgres_url, query)
  ```

- Read a DataFrame parallelly using 10 threads by automatically partitioning the provided SQL on the partition column (`partition_range` will be automatically  queried if not given)

  ```python
  import connectorx as cx

  postgres_url = "postgresql://username:password@server:port/database"
  query = "SELECT * FROM lineitem"

  cx.read_sql(postgres_url, query, partition_on="l_orderkey", partition_num=10)
  ```

- Read a DataFrame parallelly using 2 threads by manually providing two partition SQLs (the schemas of all the query results should be same)

  ```python
  import connectorx as cx

  postgres_url = "postgresql://username:password@server:port/database"
  queries = ["SELECT * FROM lineitem WHERE l_orderkey <= 30000000", "SELECT * FROM lineitem WHERE l_orderkey > 30000000"]

  cx.read_sql(postgres_url, queries)

  ```
  
- Read a DataFrame parallelly using 4 threads from a more complex query

  ```python
  import connectorx as cx

  postgres_url = "postgresql://username:password@server:port/database"
  query = f"""
  SELECT l_orderkey,
         SUM(l_extendedprice * ( 1 - l_discount )) AS revenue,
         o_orderdate,
         o_shippriority
  FROM   customer,
         orders,
         lineitem
  WHERE  c_mktsegment = 'BUILDING'
         AND c_custkey = o_custkey
         AND l_orderkey = o_orderkey
         AND o_orderdate < DATE '1995-03-15'
         AND l_shipdate > DATE '1995-03-15'
  GROUP  BY l_orderkey,
            o_orderdate,
            o_shippriority 
  """

  cx.read_sql(postgres_url, query, partition_on="l_orderkey", partition_num=4)

  ```

- Read a DataFrame from a SQL joined from multiple databases (experimental, only support PostgreSQL for now)

  ```python
  import connectorx as cx

  db1 = "postgresql://username1:password1@server1:port1/database1"
  db2 = "postgresql://username2:password2@server2:port2/database2"
  query = "SELECT * FROM db1.nation n, db2.region r where n.n_regionkey = r.r_regionkey"

  cx.read_sql({"db1": db1, "db2": db2}, query)

  ```
