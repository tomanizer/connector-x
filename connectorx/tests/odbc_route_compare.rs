use arrow::{
    array::Array, datatypes::Schema, record_batch::RecordBatch,
    util::display::array_value_to_string,
};
use connectorx::{get_arrow::get_arrow, prelude::parse_source, sql::CXQuery};

pub fn raw_odbc_url(conn: &str) -> String {
    format!("odbc:///?odbc_connect={}", urlencoding::encode(conn))
}

pub fn run_arrow_route(
    conn: &str,
    queries: &[CXQuery<String>],
    origin_query: Option<String>,
) -> Vec<RecordBatch> {
    let source_conn = parse_source(conn, None).unwrap();
    get_arrow(&source_conn, origin_query, queries, None)
        .unwrap()
        .arrow()
        .unwrap()
}

pub fn assert_matching_arrow_output(left: &[RecordBatch], right: &[RecordBatch]) {
    let left_rows = left.iter().map(RecordBatch::num_rows).sum::<usize>();
    let right_rows = right.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert_eq!(left_rows, right_rows);

    let (left_schema, left_null_counts, left_values) = summarize_arrow_output(left);
    let (right_schema, right_null_counts, right_values) = summarize_arrow_output(right);

    assert_eq!(left_schema, right_schema);
    assert_eq!(left_null_counts, right_null_counts);
    assert_eq!(left_values, right_values);
}

fn summarize_arrow_output(batches: &[RecordBatch]) -> (Schema, Vec<usize>, Vec<String>) {
    assert!(!batches.is_empty());
    let schema = batches[0].schema().as_ref().clone();
    let mut null_counts = vec![0; schema.fields().len()];
    let mut rows = Vec::new();

    for batch in batches {
        assert_eq!(batch.schema().as_ref(), &schema);
        for (column_index, column) in batch.columns().iter().enumerate() {
            null_counts[column_index] += column.null_count();
        }
        for row_index in 0..batch.num_rows() {
            let row = batch
                .columns()
                .iter()
                .map(|column| {
                    if column.is_null(row_index) {
                        "NULL".to_string()
                    } else {
                        array_value_to_string(column.as_ref(), row_index).unwrap()
                    }
                })
                .collect::<Vec<_>>();
            rows.push(format!("{row:?}"));
        }
    }

    rows.sort();
    (schema, null_counts, rows)
}
