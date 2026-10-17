//! Count, range, and partition query wrapping for the Db2 and Sybase ODBC
//! sources.
//!
//! The generic wrappers in [`crate::sql`] parse the user query with sqlparser
//! and print it back. That loses or breaks syntax sqlparser does not model:
//! Db2 isolation/cursor clauses (`WITH UR`, `FOR READ ONLY`,
//! `OPTIMIZE FOR n ROWS`) fail to parse, and Sybase lock hints (`HOLDLOCK`) or
//! `TOP n` are re-printed differently. These wrappers move trailing clauses to
//! the outer query and, for Sybase, never re-print the user SQL.

use crate::errors::ConnectorXError;
use crate::sql::{count_query, get_partition_range_query, single_col_partition_query, CXQuery};
use fehler::{throw, throws};
use log::debug;
use sqlparser::dialect::GenericDialect;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OdbcWrapDialect {
    Db2,
    Sybase,
}

/// A top-level token of the user query, with its byte range.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Token {
    text: String,
    start: usize,
    end: usize,
}

impl Token {
    fn is_word(&self, word: &str) -> bool {
        self.text.eq_ignore_ascii_case(word)
    }
}

/// Returns the tokens at parenthesis depth 0, skipping string literals, quoted
/// identifiers, and comments. Words and numbers are single tokens; other
/// characters are one-character tokens.
fn top_level_tokens(sql: &str) -> Vec<Token> {
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut depth = 0_usize;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b'\'' | b'"' | b'[' => {
                let close = if c == b'[' { b']' } else { c };
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == close {
                        // Doubled quotes are escapes inside the literal.
                        if close != b']' && bytes.get(i + 1) == Some(&close) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i += 2;
            }
            b'(' => {
                if depth == 0 {
                    tokens.push(Token {
                        text: "(".into(),
                        start: i,
                        end: i + 1,
                    });
                }
                depth += 1;
                i += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    tokens.push(Token {
                        text: ")".into(),
                        start: i,
                        end: i + 1,
                    });
                }
                i += 1;
            }
            c if c.is_ascii_whitespace() => i += 1,
            c if c.is_ascii_alphanumeric() || c == b'_' || c == b'@' || c == b'#' || c == b'$' => {
                let start = i;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric()
                        || matches!(bytes[i], b'_' | b'@' | b'#' | b'$'))
                {
                    i += 1;
                }
                if depth == 0 {
                    tokens.push(Token {
                        text: sql[start..i].to_string(),
                        start,
                        end: i,
                    });
                }
            }
            _ => {
                if depth == 0 {
                    let len = sql[i..].chars().next().map(char::len_utf8).unwrap_or(1);
                    tokens.push(Token {
                        text: sql[i..i + len].to_string(),
                        start: i,
                        end: i + len,
                    });
                    i += len;
                } else {
                    i += 1;
                }
            }
        }
    }
    tokens
}

/// Number of trailing tokens forming one recognised clause at the end of
/// `tokens`, or `None`.
fn trailing_clause_len(tokens: &[Token], dialect: OdbcWrapDialect) -> Option<usize> {
    let n = tokens.len();
    let tail = |k: usize| -> Option<&[Token]> { n.checked_sub(k).map(|s| &tokens[s..]) };
    let words = |k: usize, pattern: &[&[&str]]| -> bool {
        tail(k).map_or(false, |t| {
            t.iter().zip(pattern).all(|(tok, options)| {
                options.iter().any(|w| {
                    *w == "#" && tok.text.bytes().all(|b| b.is_ascii_digit()) || tok.is_word(w)
                })
            })
        })
    };

    // FOR READ ONLY / FOR FETCH ONLY (both dialects).
    if words(3, &[&["FOR"], &["READ", "FETCH"], &["ONLY"]]) {
        return Some(3);
    }
    match dialect {
        OdbcWrapDialect::Db2 => {
            if words(2, &[&["WITH"], &["UR", "CS", "RS", "RR"]]) {
                return Some(2);
            }
            if words(4, &[&["OPTIMIZE"], &["FOR"], &["#"], &["ROW", "ROWS"]]) {
                return Some(4);
            }
        }
        OdbcWrapDialect::Sybase => {
            if words(2, &[&["FOR"], &["BROWSE"]]) {
                return Some(2);
            }
            // AT ISOLATION [LEVEL] {0|1|2|3|READ UNCOMMITTED|READ COMMITTED|
            // REPEATABLE READ|SERIALIZABLE}
            for len in [5, 4, 3] {
                let Some(t) = tail(len) else { continue };
                let rest = if t.len() > 2 && t[2].is_word("LEVEL") {
                    &t[3..]
                } else {
                    &t[2..]
                };
                let level_ok = match rest {
                    [one] => {
                        one.is_word("0")
                            || one.is_word("1")
                            || one.is_word("2")
                            || one.is_word("3")
                            || one.is_word("SERIALIZABLE")
                    }
                    [a, b] => {
                        (a.is_word("READ") && (b.is_word("UNCOMMITTED") || b.is_word("COMMITTED")))
                            || (a.is_word("REPEATABLE") && b.is_word("READ"))
                    }
                    _ => false,
                };
                if t[0].is_word("AT") && t[1].is_word("ISOLATION") && level_ok {
                    return Some(len);
                }
            }
        }
    }
    None
}

/// Query text split into the part that can be wrapped in a derived table and
/// the trailing clauses that must stay on the outermost query.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SplitQuery {
    core: String,
    trailing: Option<String>,
}

fn split_trailing_clauses(sql: &str, dialect: OdbcWrapDialect) -> SplitQuery {
    let sql = sql.trim();
    let sql = sql.strip_suffix(';').unwrap_or(sql).trim_end();
    let mut tokens = top_level_tokens(sql);
    let mut cut = sql.len();
    while let Some(len) = trailing_clause_len(&tokens, dialect) {
        cut = tokens[tokens.len() - len].start;
        tokens.truncate(tokens.len() - len);
    }
    let core = sql[..cut].trim_end().to_string();
    let trailing = sql[cut..].trim();
    SplitQuery {
        core,
        trailing: (!trailing.is_empty()).then(|| trailing.to_string()),
    }
}

fn append_trailing(sql: String, trailing: &Option<String>) -> String {
    match trailing {
        Some(trailing) => format!("{sql} {trailing}"),
        None => sql,
    }
}

/// Prepares a Sybase query for use as a derived table.
///
/// ASE rejects `ORDER BY` inside derived tables, so a top-level `ORDER BY` is
/// removed (row order does not matter for count/range/partition wrappers). ASE
/// also ignores `TOP` inside derived tables (a wrapped `TOP 3` returned every
/// row against ASE 16), so top-level `TOP` queries are rejected rather than
/// silently returning different rows.
#[throws(ConnectorXError)]
fn sybase_derived_table_body(core: &str) -> String {
    let tokens = top_level_tokens(core);
    if tokens.iter().any(|t| t.is_word("TOP")) {
        throw!(ConnectorXError::SqlQueryNotSupported(format!(
            "{core} (Sybase ignores TOP inside derived tables, so this query cannot be wrapped for counting or partitioning; read it without partition_on through the Arrow path, or move TOP into an outer query)"
        )));
    }
    let order_by = tokens
        .windows(2)
        .rposition(|w| w[0].is_word("ORDER") && w[1].is_word("BY"));
    match order_by {
        // A later top-level UNION means this ORDER BY belongs to a branch.
        Some(pos) if !tokens[pos..].iter().any(|t| t.is_word("UNION")) => {
            core[..tokens[pos].start].trim_end().to_string()
        }
        _ => core.to_string(),
    }
}

#[throws(ConnectorXError)]
pub fn odbc_count_query(sql: &CXQuery<String>, dialect: OdbcWrapDialect) -> CXQuery<String> {
    let split = split_trailing_clauses(sql.as_str(), dialect);
    let counted = match dialect {
        OdbcWrapDialect::Db2 => {
            let core = sql.map(|_| split.core.clone());
            count_query(&core, &GenericDialect {})?.to_string()
        }
        OdbcWrapDialect::Sybase => format!(
            "SELECT COUNT(*) FROM ({}) AS CXTMPTAB_COUNT",
            sybase_derived_table_body(&split.core)?
        ),
    };
    let tsql = append_trailing(counted, &split.trailing);
    debug!("Transformed {dialect:?} count query: {tsql}");
    CXQuery::Wrapped(tsql)
}

#[throws(ConnectorXError)]
pub fn odbc_partition_range_query(sql: &str, col: &str, dialect: OdbcWrapDialect) -> String {
    let split = split_trailing_clauses(sql, dialect);
    let ranged = match dialect {
        OdbcWrapDialect::Db2 => get_partition_range_query(&split.core, col, &GenericDialect {})?,
        OdbcWrapDialect::Sybase => format!(
            "SELECT MIN(CXTMPTAB_RANGE.{col}), MAX(CXTMPTAB_RANGE.{col}) FROM ({}) AS CXTMPTAB_RANGE",
            sybase_derived_table_body(&split.core)?
        ),
    };
    let tsql = append_trailing(ranged, &split.trailing);
    debug!("Transformed {dialect:?} partition range query: {tsql}");
    tsql
}

#[throws(ConnectorXError)]
pub fn odbc_single_col_partition_query(
    sql: &str,
    col: &str,
    lower: i64,
    upper: i64,
    dialect: OdbcWrapDialect,
) -> String {
    let split = split_trailing_clauses(sql, dialect);
    let part = match dialect {
        OdbcWrapDialect::Db2 => {
            single_col_partition_query(&split.core, col, lower, upper, &GenericDialect {})?
        }
        OdbcWrapDialect::Sybase => {
            format!(
                "SELECT * FROM ({}) AS CXTMPTAB_PART WHERE {lower} <= CXTMPTAB_PART.{col} AND CXTMPTAB_PART.{col} < {upper}",
                sybase_derived_table_body(&split.core)?
            )
        }
    };
    let tsql = append_trailing(part, &split.trailing);
    debug!("Transformed {dialect:?} partition query: {tsql}");
    tsql
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(sql: &str, dialect: OdbcWrapDialect) -> (String, Option<String>) {
        let s = split_trailing_clauses(sql, dialect);
        (s.core, s.trailing)
    }

    #[test]
    fn splits_db2_isolation_and_cursor_clauses() {
        use OdbcWrapDialect::Db2;
        assert_eq!(
            split("select * from t with ur", Db2),
            ("select * from t".into(), Some("with ur".into()))
        );
        assert_eq!(
            split("select * from t FOR READ ONLY WITH CS;", Db2),
            (
                "select * from t".into(),
                Some("FOR READ ONLY WITH CS".into())
            )
        );
        assert_eq!(
            split("select * from t optimize for 10 rows", Db2),
            (
                "select * from t".into(),
                Some("optimize for 10 rows".into())
            )
        );
        // A CTE named "ur" or clauses inside parentheses are not trailing clauses.
        assert_eq!(
            split(
                "with ur as (select 1 x from t with ur) select * from ur",
                Db2
            ),
            (
                "with ur as (select 1 x from t with ur) select * from ur".into(),
                None
            )
        );
        assert_eq!(
            split("select 'with ur' as s from t", Db2),
            ("select 'with ur' as s from t".into(), None)
        );
    }

    #[test]
    fn splits_sybase_isolation_and_browse_clauses() {
        use OdbcWrapDialect::Sybase;
        assert_eq!(
            split("select * from t at isolation read uncommitted", Sybase),
            (
                "select * from t".into(),
                Some("at isolation read uncommitted".into())
            )
        );
        assert_eq!(
            split("select * from t at isolation level 0", Sybase),
            (
                "select * from t".into(),
                Some("at isolation level 0".into())
            )
        );
        assert_eq!(
            split("select * from t for read only", Sybase),
            ("select * from t".into(), Some("for read only".into()))
        );
        // Db2 clauses are not Sybase clauses.
        assert_eq!(
            split("select * from t with ur", Sybase),
            ("select * from t with ur".into(), None)
        );
    }

    #[test]
    fn db2_wrappers_keep_isolation_on_the_outer_query() {
        let part = odbc_single_col_partition_query(
            "select id, name from s.t where flag = 1 with ur",
            "id",
            0,
            10,
            OdbcWrapDialect::Db2,
        )
        .unwrap();
        assert!(part.ends_with(" with ur"), "{}", part);
        assert_eq!(part.matches("with ur").count(), 1, "{}", part);
        assert!(part.contains("0 <= CXTMPTAB_PART.id"), "{}", part);

        let range = odbc_partition_range_query(
            "select id from s.t for read only",
            "id",
            OdbcWrapDialect::Db2,
        )
        .unwrap();
        assert!(
            range.to_ascii_uppercase().starts_with("SELECT MIN("),
            "{}",
            range
        );
        assert!(range.ends_with(" for read only"), "{}", range);

        let count = odbc_count_query(
            &CXQuery::naked("select id from s.t optimize for 5 rows with rs"),
            OdbcWrapDialect::Db2,
        )
        .unwrap()
        .to_string();
        assert!(count.to_ascii_uppercase().contains("COUNT(*)"), "{}", count);
        assert!(count.ends_with(" optimize for 5 rows with rs"), "{}", count);
    }

    #[test]
    fn db2_wrappers_still_hoist_ctes() {
        let part = odbc_single_col_partition_query(
            "with q as (select id from s.t) select * from q with ur",
            "id",
            0,
            10,
            OdbcWrapDialect::Db2,
        )
        .unwrap();
        assert!(
            part.to_ascii_uppercase().starts_with("WITH Q AS"),
            "{}",
            part
        );
        assert!(part.ends_with(" with ur"), "{}", part);
    }

    #[test]
    fn sybase_wrappers_do_not_reprint_user_sql() {
        let sql = "select id, name from dbo.t holdlock where id > 0";
        let part =
            odbc_single_col_partition_query(sql, "id", 1, 5, OdbcWrapDialect::Sybase).unwrap();
        assert_eq!(
            part,
            format!("SELECT * FROM ({sql}) AS CXTMPTAB_PART WHERE 1 <= CXTMPTAB_PART.id AND CXTMPTAB_PART.id < 5")
        );
    }

    #[test]
    fn sybase_wrappers_reject_top_queries() {
        for sql in [
            "select top 10 id from dbo.t",
            "select top 10 id from dbo.t order by id",
        ] {
            let part = odbc_single_col_partition_query(sql, "id", 1, 5, OdbcWrapDialect::Sybase)
                .unwrap_err()
                .to_string();
            assert!(
                part.contains("ignores TOP inside derived tables"),
                "{}",
                part
            );
            let count = odbc_count_query(&CXQuery::naked(sql), OdbcWrapDialect::Sybase)
                .unwrap_err()
                .to_string();
            assert!(
                count.contains("ignores TOP inside derived tables"),
                "{}",
                count
            );
            assert!(odbc_partition_range_query(sql, "id", OdbcWrapDialect::Sybase).is_err());
        }
        // TOP inside a subquery is not wrapped again, so it is fine.
        let nested = "select id from (select top 1 id from t order by id) x";
        assert!(odbc_count_query(&CXQuery::naked(nested), OdbcWrapDialect::Sybase).is_ok());
    }

    #[test]
    fn sybase_wrappers_strip_order_by_and_move_isolation() {
        let sql = "select id from dbo.t holdlock order by id desc at isolation 0";
        let count = odbc_count_query(&CXQuery::naked(sql), OdbcWrapDialect::Sybase)
            .unwrap()
            .to_string();
        assert_eq!(
            count,
            "SELECT COUNT(*) FROM (select id from dbo.t holdlock) AS CXTMPTAB_COUNT at isolation 0"
        );
        let range = odbc_partition_range_query(sql, "id", OdbcWrapDialect::Sybase).unwrap();
        assert_eq!(
            range,
            "SELECT MIN(CXTMPTAB_RANGE.id), MAX(CXTMPTAB_RANGE.id) FROM (select id from dbo.t holdlock) AS CXTMPTAB_RANGE at isolation 0"
        );
    }

    #[test]
    fn sybase_order_by_inside_subqueries_or_union_branches_is_kept() {
        let sql = "select id from (select top 1 id from t order by id) x";
        assert_eq!(sybase_derived_table_body(sql).unwrap(), sql);
        let union = "select id from a order by id union all select id from b";
        assert_eq!(sybase_derived_table_body(union).unwrap(), union);
        assert_eq!(
            sybase_derived_table_body("select a from t union all select a from u order by a")
                .unwrap(),
            "select a from t union all select a from u"
        );
    }
}
