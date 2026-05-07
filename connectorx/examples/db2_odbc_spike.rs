#[cfg(not(feature = "db2_odbc_spike"))]
fn main() {
    eprintln!(
        "Enable the db2_odbc_spike feature to run this example:\n\
         DB2_ODBC_CONN='Driver={{IBM DB2 ODBC DRIVER}};Hostname=...;Port=50000;Protocol=TCPIP;Database=...;UID=...;PWD=...;' \\\n\
         DB2_QUERY='select * from my_table' \\\n\
         cargo run -p connectorx --features db2_odbc_spike --example db2_odbc_spike"
    );
    std::process::exit(2);
}

#[cfg(feature = "db2_odbc_spike")]
fn main() -> anyhow::Result<()> {
    spike::run()
}

#[cfg(feature = "db2_odbc_spike")]
mod spike {
    use anyhow::{anyhow, Context, Result};
    use odbc_api::{
        buffers::TextRowSet, ConnectionOptions, Cursor, Environment, ResultSetMetadata,
    };
    use std::{env, time::Instant};

    pub fn run() -> Result<()> {
        let conn_str = env::var("DB2_ODBC_CONN").context(
            "DB2_ODBC_CONN is required, for example \
             Driver={IBM DB2 ODBC DRIVER};Hostname=localhost;Port=50000;Protocol=TCPIP;Database=testdb;UID=db2inst1;PWD=secret;",
        )?;
        let query = env::var("DB2_QUERY").unwrap_or_else(|_| "select 1".to_string());
        let batch_size = parse_env_usize("DB2_BATCH_SIZE", 8192)?;
        let max_str_len = parse_env_usize("DB2_MAX_STR_LEN", 4096)?;
        let sample_limit = parse_env_usize("DB2_SAMPLE_ROWS", 5)?;

        println!("db2 odbc spike");
        println!("batch_size={batch_size}");
        println!("max_str_len={max_str_len}");
        println!("query={query}");

        let environment = Environment::new().context("create ODBC environment")?;
        let connection = environment
            .connect_with_connection_string(&conn_str, ConnectionOptions::default())
            .context("connect with DB2_ODBC_CONN")?;

        let Some(mut cursor) = connection
            .execute(&query, (), None)
            .context("execute DB2_QUERY")?
        else {
            println!("query returned no result set");
            return Ok(());
        };

        print_metadata(&mut cursor)?;

        let mut buffers = TextRowSet::for_cursor(batch_size, &mut cursor, Some(max_str_len))
            .context("allocate ODBC text rowset buffer")?;
        let mut row_set_cursor = cursor
            .bind_buffer(&mut buffers)
            .context("bind rowset buffer to cursor")?;

        let started = Instant::now();
        let mut rows = 0usize;
        let mut batches = 0usize;
        let mut sample_rows = Vec::new();

        while let Some(batch) = row_set_cursor.fetch().context("fetch rowset")? {
            batches += 1;
            rows += batch.num_rows();

            let sample_remaining = sample_limit.saturating_sub(sample_rows.len());
            for row_index in 0..sample_remaining.min(batch.num_rows()) {
                let row = (0..batch.num_cols())
                    .map(|col_index| {
                        batch
                            .at(col_index, row_index)
                            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                            .unwrap_or_else(|| "NULL".to_string())
                    })
                    .collect::<Vec<_>>();
                sample_rows.push(row);
            }
        }

        let elapsed = started.elapsed();
        let seconds = elapsed.as_secs_f64();
        let rows_per_second = if seconds > 0.0 {
            rows as f64 / seconds
        } else {
            0.0
        };

        println!("rows={rows}");
        println!("batches={batches}");
        println!("elapsed_ms={:.3}", seconds * 1000.0);
        println!("rows_per_second={rows_per_second:.3}");

        if !sample_rows.is_empty() {
            println!("sample_rows:");
            for row in sample_rows {
                println!("  {}", row.join(" | "));
            }
        }

        Ok(())
    }

    fn parse_env_usize(name: &str, default: usize) -> Result<usize> {
        match env::var(name) {
            Ok(value) => value
                .parse()
                .with_context(|| format!("{name} must be a positive integer")),
            Err(env::VarError::NotPresent) => Ok(default),
            Err(err) => Err(err).with_context(|| format!("read {name}")),
        }
    }

    fn print_metadata<C>(cursor: &mut C) -> Result<()>
    where
        C: ResultSetMetadata,
    {
        let ncols = cursor
            .num_result_cols()
            .context("read result column count")?;
        if ncols < 0 {
            return Err(anyhow!("ODBC returned negative column count: {ncols}"));
        }

        println!("columns={ncols}");
        for col in 1..=ncols as u16 {
            let name = cursor
                .col_name(col)
                .with_context(|| format!("read column {col} name"))?;
            let ty = cursor
                .col_data_type(col)
                .with_context(|| format!("read column {col} type"))?;
            let nullable = cursor
                .col_nullability(col)
                .with_context(|| format!("read column {col} nullability"))?;
            let precision = cursor
                .col_precision(col)
                .with_context(|| format!("read column {col} precision"))?;
            let scale = cursor
                .col_scale(col)
                .with_context(|| format!("read column {col} scale"))?;
            println!(
                "  {col}: name={name:?}, type={ty:?}, nullable={nullable:?}, precision={precision}, scale={scale}"
            );
        }

        Ok(())
    }
}
