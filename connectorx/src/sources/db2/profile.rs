use anyhow::{anyhow, Result};

use crate::sources::odbc_common::param_value;

pub(crate) const DB2_PROFILE_PARAM: &str = "db2_profile";
pub(crate) const DB2_REPLICATION_KEY_COLUMNS_PARAM: &str = "replication_key_columns";
const DB2_PROFILE_ENV: &str = "DB2_PROFILE";
const DB2_REPLICATION_KEY_COLUMNS_ENV: &str = "DB2_REPLICATION_KEY_COLUMNS";

const DEFAULT_QREP_REPLKEY: &str = "IBMQREP_REPLKEY";
const DEFAULT_QREP_SITE: &str = "IBMQREP_SITE";
const ROW_TRACKING_SYSROWID: &str = "SYSROWID";
const ROW_TRACKING_CREATEXID: &str = "CREATEXID";
const ROW_TRACKING_DELETEXID: &str = "DELETEXID";
const NETEZZA_ROWID: &str = "ROWID";
const NETEZZA_DATASLICEID: &str = "DATASLICEID";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Db2Profile {
    #[default]
    Generic,
    Sailfish,
    QReplication,
    RowModificationTracking,
    NetezzaSpecials,
}

impl Db2Profile {
    pub fn from_name(name: &str) -> Result<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "" | "generic" | "none" => Ok(Self::Generic),
            "sailfish" | "idaa" | "accelerator" => Ok(Self::Sailfish),
            "qrep" | "qreplication" | "q-replication" | "q_replication" => {
                Ok(Self::QReplication)
            }
            "row_tracking"
            | "row-tracking"
            | "rowtracking"
            | "row_modification_tracking"
            | "row-modification-tracking" => Ok(Self::RowModificationTracking),
            "netezza" | "puredata" | "netezza_specials" | "netezza-specials" => {
                Ok(Self::NetezzaSpecials)
            }
            other => Err(anyhow!(
                "{DB2_PROFILE_PARAM} must be one of generic, sailfish, idaa, qrep, qreplication, row_tracking, or netezza; got {other:?}"
            )),
        }
    }

    pub fn is_catalog_profile(self) -> bool {
        !matches!(self, Self::Generic)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Db2ProfileConfig {
    pub profile: Db2Profile,
    pub replication_key_columns: Vec<String>,
}

impl Db2ProfileConfig {
    pub fn from_env_and_params(params: Option<&[(String, String)]>) -> Result<Self> {
        let env_profile = std::env::var(DB2_PROFILE_ENV).ok();
        let env_replication_key_columns = std::env::var(DB2_REPLICATION_KEY_COLUMNS_ENV).ok();

        Self::from_values_and_params(
            params,
            env_profile.as_deref(),
            env_replication_key_columns.as_deref(),
        )
    }

    fn from_values_and_params(
        params: Option<&[(String, String)]>,
        env_profile: Option<&str>,
        env_replication_key_columns: Option<&str>,
    ) -> Result<Self> {
        let mut profile = env_profile
            .filter(|value| !value.trim().is_empty())
            .map(Db2Profile::from_name)
            .transpose()?
            .unwrap_or_default();

        if let Some(value) = params.and_then(|params| param_value(params, DB2_PROFILE_PARAM)) {
            profile = Db2Profile::from_name(value)?;
        }

        let mut replication_key_columns = env_replication_key_columns
            .filter(|value| !value.trim().is_empty())
            .map(parse_replication_key_columns)
            .transpose()?
            .unwrap_or_default();

        if let Some(value) =
            params.and_then(|params| param_value(params, DB2_REPLICATION_KEY_COLUMNS_PARAM))
        {
            replication_key_columns = parse_replication_key_columns(value)?;
        }

        if replication_key_columns.is_empty() && matches!(profile, Db2Profile::QReplication) {
            replication_key_columns = default_qrep_columns();
        }

        Ok(Self {
            profile,
            replication_key_columns,
        })
    }

    pub fn is_active(&self) -> bool {
        self.profile.is_catalog_profile() || !self.replication_key_columns.is_empty()
    }

    pub fn runtime_scope_message(&self) -> Option<String> {
        self.is_active().then(|| {
            format!(
                "Db2 profile {:?} is enabled for catalog helper validation; ConnectorX does not automatically alter extraction queries, project hidden columns, or select partition keys from this profile",
                self.profile
            )
        })
    }
}

pub(crate) fn is_db2_profile_option_key(key: &str) -> bool {
    key.eq_ignore_ascii_case(DB2_PROFILE_PARAM)
        || key.eq_ignore_ascii_case(DB2_REPLICATION_KEY_COLUMNS_PARAM)
}

fn parse_replication_key_columns(value: &str) -> Result<Vec<String>> {
    let columns = value
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();

    if columns.is_empty() {
        return Err(anyhow!(
            "{DB2_REPLICATION_KEY_COLUMNS_PARAM} must contain at least one column name"
        ));
    }

    let mut seen = std::collections::HashSet::new();
    for column in &columns {
        if !seen.insert(column.to_ascii_uppercase()) {
            return Err(anyhow!(
                "{DB2_REPLICATION_KEY_COLUMNS_PARAM} contains duplicate column {column:?}"
            ));
        }
    }

    Ok(columns)
}

fn default_qrep_columns() -> Vec<String> {
    vec![
        DEFAULT_QREP_REPLKEY.to_string(),
        DEFAULT_QREP_SITE.to_string(),
    ]
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Db2CatalogColumn {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    pub hidden: bool,
    pub identity: bool,
    pub generated: bool,
    pub key_sequence: Option<i16>,
}

impl Db2CatalogColumn {
    pub fn new(name: impl Into<String>, type_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            nullable: true,
            hidden: false,
            identity: false,
            generated: false,
            key_sequence: None,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn hidden(mut self) -> Self {
        self.hidden = true;
        self
    }

    pub fn identity(mut self) -> Self {
        self.identity = true;
        self.generated = true;
        self
    }

    pub fn key_sequence(mut self, key_sequence: i16) -> Self {
        self.key_sequence = Some(key_sequence);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Db2UniqueIndex {
    pub name: String,
    pub columns: Vec<String>,
}

impl Db2UniqueIndex {
    pub fn new(name: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            name: name.into(),
            columns,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Db2KeyConstraintKind {
    PrimaryKey,
    Unique,
}

impl Db2KeyConstraintKind {
    pub fn from_catalog_type(catalog_type: &str) -> Option<Self> {
        match catalog_type.trim().to_ascii_uppercase().as_str() {
            "P" => Some(Self::PrimaryKey),
            "U" => Some(Self::Unique),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Db2KeyConstraint {
    pub name: String,
    pub kind: Db2KeyConstraintKind,
    pub enforced: bool,
    pub trusted: bool,
    pub columns: Vec<String>,
}

impl Db2KeyConstraint {
    pub fn new(name: impl Into<String>, kind: Db2KeyConstraintKind, columns: Vec<String>) -> Self {
        Self {
            name: name.into(),
            kind,
            enforced: true,
            trusted: true,
            columns,
        }
    }

    pub fn not_enforced(mut self, trusted: bool) -> Self {
        self.enforced = false;
        self.trusted = trusted;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Db2ReplicationKeyEvidence {
    NotDetected,
    ConfiguredColumns,
    DefaultQReplicationColumns,
    IbmRepKeyNamePattern,
    RowModificationTrackingColumns,
    NetezzaSpecialColumns,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Db2ReplicationKeyUniqueness {
    EnforcedPrimaryKey(String),
    EnforcedUniqueConstraint(String),
    TrustedInformationalConstraint(String),
    NotTrustedInformationalConstraint(String),
    UniqueIndex(String),
    ProfileConventionOnly,
    NotDetected,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Db2PartitionHint {
    UseColumn { column: String, reason: String },
    ManualPartitioning { reason: String },
    NotAvailable { reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Db2ReplicationKeyDiagnostic {
    pub profile: Db2Profile,
    pub evidence: Db2ReplicationKeyEvidence,
    pub candidate_columns: Vec<Db2CatalogColumn>,
    pub uniqueness: Db2ReplicationKeyUniqueness,
    pub partition_hint: Db2PartitionHint,
    pub warnings: Vec<String>,
}

pub fn diagnose_replication_key(
    config: &Db2ProfileConfig,
    columns: &[Db2CatalogColumn],
    key_constraints: &[Db2KeyConstraint],
    unique_indexes: &[Db2UniqueIndex],
) -> Db2ReplicationKeyDiagnostic {
    let (candidate_columns, evidence) = select_replication_key_candidate(config, columns)
        .unwrap_or((Vec::new(), Db2ReplicationKeyEvidence::NotDetected));

    if candidate_columns.is_empty() {
        return Db2ReplicationKeyDiagnostic {
            profile: config.profile,
            evidence: Db2ReplicationKeyEvidence::NotDetected,
            candidate_columns,
            uniqueness: Db2ReplicationKeyUniqueness::NotDetected,
            partition_hint: Db2PartitionHint::NotAvailable {
                reason: "no configured or profile-matching Db2 replication key columns were found"
                    .to_string(),
            },
            warnings: vec![],
        };
    }

    let uniqueness =
        replication_key_uniqueness(&candidate_columns, key_constraints, unique_indexes);
    let partition_hint = replication_key_partition_hint(&candidate_columns);
    let mut warnings = vec![];

    if matches!(
        uniqueness,
        Db2ReplicationKeyUniqueness::ProfileConventionOnly
    ) {
        warnings.push(
            "replication key columns match a Db2 profile convention, but no exact primary-key, unique-constraint, or unique-index catalog evidence was found".to_string(),
        );
    }
    if matches!(
        uniqueness,
        Db2ReplicationKeyUniqueness::TrustedInformationalConstraint(_)
            | Db2ReplicationKeyUniqueness::NotTrustedInformationalConstraint(_)
    ) {
        warnings.push(
            "the matching Db2 constraint is not enforced; ConnectorX treats it as planning metadata, not as guaranteed uniqueness".to_string(),
        );
    }
    if matches!(
        evidence,
        Db2ReplicationKeyEvidence::RowModificationTrackingColumns
    ) {
        warnings.push(
            "row modification tracking columns are hidden Db2 metadata, not Q Replication keys; select them explicitly when a workload needs them".to_string(),
        );
    }
    if matches!(evidence, Db2ReplicationKeyEvidence::NetezzaSpecialColumns) {
        warnings.push(
            "Netezza special columns are system-maintained row metadata; treat them as a platform-specific hint unless catalog constraints also prove uniqueness".to_string(),
        );
    }

    Db2ReplicationKeyDiagnostic {
        profile: config.profile,
        evidence,
        candidate_columns,
        uniqueness,
        partition_hint,
        warnings,
    }
}

fn select_replication_key_candidate(
    config: &Db2ProfileConfig,
    columns: &[Db2CatalogColumn],
) -> Option<(Vec<Db2CatalogColumn>, Db2ReplicationKeyEvidence)> {
    if !config.replication_key_columns.is_empty() {
        if let Some(columns) = find_columns_by_name(columns, &config.replication_key_columns) {
            let evidence = if matches!(
                config.profile,
                Db2Profile::QReplication | Db2Profile::Sailfish
            ) && case_insensitive_eq(
                &config.replication_key_columns,
                &default_qrep_columns(),
            ) {
                Db2ReplicationKeyEvidence::DefaultQReplicationColumns
            } else {
                Db2ReplicationKeyEvidence::ConfiguredColumns
            };
            return Some((columns, evidence));
        }
    }

    if matches!(
        config.profile,
        Db2Profile::QReplication | Db2Profile::Sailfish
    ) {
        if let Some(columns) = find_columns_by_name(columns, &default_qrep_columns()) {
            return Some((
                columns,
                Db2ReplicationKeyEvidence::DefaultQReplicationColumns,
            ));
        }
    }

    if matches!(config.profile, Db2Profile::Sailfish) {
        if let Some(columns) = ibmrepkey_columns(columns) {
            return Some((columns, Db2ReplicationKeyEvidence::IbmRepKeyNamePattern));
        }
        if let Some(columns) = row_tracking_columns(columns) {
            return Some((
                columns,
                Db2ReplicationKeyEvidence::RowModificationTrackingColumns,
            ));
        }
        if let Some(columns) = netezza_special_columns(columns) {
            return Some((columns, Db2ReplicationKeyEvidence::NetezzaSpecialColumns));
        }
    }

    if matches!(config.profile, Db2Profile::RowModificationTracking) {
        if let Some(columns) = row_tracking_columns(columns) {
            return Some((
                columns,
                Db2ReplicationKeyEvidence::RowModificationTrackingColumns,
            ));
        }
    }

    if matches!(config.profile, Db2Profile::NetezzaSpecials) {
        if let Some(columns) = netezza_special_columns(columns) {
            return Some((columns, Db2ReplicationKeyEvidence::NetezzaSpecialColumns));
        }
    }

    None
}

fn ibmrepkey_columns(columns: &[Db2CatalogColumn]) -> Option<Vec<Db2CatalogColumn>> {
    let mut matches = columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.name.to_ascii_uppercase().contains("IBMREPKEY"))
        .map(|(index, column)| (index, column.clone()))
        .collect::<Vec<_>>();
    matches.sort_by(|(left_index, left), (right_index, right)| {
        left.key_sequence
            .unwrap_or(i16::MAX)
            .cmp(&right.key_sequence.unwrap_or(i16::MAX))
            .then_with(|| left_index.cmp(right_index))
    });
    if matches.len() == 2 {
        Some(
            matches
                .into_iter()
                .map(|(_, column)| column)
                .collect::<Vec<_>>(),
        )
    } else {
        None
    }
}

fn row_tracking_columns(columns: &[Db2CatalogColumn]) -> Option<Vec<Db2CatalogColumn>> {
    let sysrowid = find_columns_by_name(columns, &[ROW_TRACKING_SYSROWID.to_string()])?;
    let has_change_columns = find_columns_by_name(
        columns,
        &[
            ROW_TRACKING_CREATEXID.to_string(),
            ROW_TRACKING_DELETEXID.to_string(),
        ],
    )
    .is_some();

    if has_change_columns {
        Some(sysrowid)
    } else {
        None
    }
}

fn netezza_special_columns(columns: &[Db2CatalogColumn]) -> Option<Vec<Db2CatalogColumn>> {
    find_columns_by_name(columns, &[NETEZZA_ROWID.to_string()])
}

fn find_columns_by_name(
    columns: &[Db2CatalogColumn],
    names: &[String],
) -> Option<Vec<Db2CatalogColumn>> {
    let mut found = Vec::with_capacity(names.len());
    for name in names {
        let column = columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(name))?;
        found.push(column.clone());
    }
    Some(found)
}

fn replication_key_uniqueness(
    candidate_columns: &[Db2CatalogColumn],
    key_constraints: &[Db2KeyConstraint],
    unique_indexes: &[Db2UniqueIndex],
) -> Db2ReplicationKeyUniqueness {
    let candidate_names = candidate_columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();

    for constraint in key_constraints {
        if case_insensitive_eq_unordered(&candidate_names, &constraint.columns) {
            return match (constraint.kind, constraint.enforced, constraint.trusted) {
                (Db2KeyConstraintKind::PrimaryKey, true, _) => {
                    Db2ReplicationKeyUniqueness::EnforcedPrimaryKey(constraint.name.clone())
                }
                (Db2KeyConstraintKind::Unique, true, _) => {
                    Db2ReplicationKeyUniqueness::EnforcedUniqueConstraint(constraint.name.clone())
                }
                (_, false, true) => Db2ReplicationKeyUniqueness::TrustedInformationalConstraint(
                    constraint.name.clone(),
                ),
                (_, false, false) => {
                    Db2ReplicationKeyUniqueness::NotTrustedInformationalConstraint(
                        constraint.name.clone(),
                    )
                }
            };
        }
    }

    for index in unique_indexes {
        if case_insensitive_eq_unordered(&candidate_names, &index.columns) {
            return Db2ReplicationKeyUniqueness::UniqueIndex(index.name.clone());
        }
    }

    Db2ReplicationKeyUniqueness::ProfileConventionOnly
}

fn replication_key_partition_hint(candidate_columns: &[Db2CatalogColumn]) -> Db2PartitionHint {
    let Some(first) = candidate_columns.first() else {
        return Db2PartitionHint::NotAvailable {
            reason: "no candidate columns were found".to_string(),
        };
    };

    if looks_like_site_column(&first.name) {
        return Db2PartitionHint::ManualPartitioning {
            reason: format!(
                "{} looks like a site/discriminator column and is not a safe range partition key",
                first.name
            ),
        };
    }

    if is_numeric_db2_type(&first.type_name) {
        return Db2PartitionHint::UseColumn {
            column: first.name.clone(),
            reason: format!(
                "{} is the leading numeric replication-key column; keep the full composite key for row identity",
                first.name
            ),
        };
    }

    Db2PartitionHint::ManualPartitioning {
        reason: format!(
            "{} has Db2 type {} and is not a simple numeric range partition key",
            first.name, first.type_name
        ),
    }
}

fn is_numeric_db2_type(type_name: &str) -> bool {
    [
        "SMALLINT", "INTEGER", "INT", "BIGINT", "DECIMAL", "NUMERIC", "DECFLOAT",
    ]
    .iter()
    .any(|ty| db2_type_name_matches(type_name, ty))
}

fn db2_type_name_matches(type_name: &str, base_type: &str) -> bool {
    if type_name.eq_ignore_ascii_case(base_type) {
        return true;
    }

    type_name
        .get(..base_type.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(base_type))
        && type_name.as_bytes().get(base_type.len()) == Some(&b'(')
}

fn looks_like_site_column(name: &str) -> bool {
    name.to_ascii_uppercase().contains("SITE")
}

fn case_insensitive_eq(left: &[String], right: &[String]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

fn case_insensitive_eq_unordered(left: &[String], right: &[String]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut left = left
        .iter()
        .map(|value| value.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let mut right = right
        .iter()
        .map(|value| value.to_ascii_uppercase())
        .collect::<Vec<_>>();
    left.sort();
    right.sort();
    left == right
}

pub fn replication_key_catalog_query(
    schema: &str,
    table: &str,
    config: &Db2ProfileConfig,
) -> String {
    let mut filters = vec![
        format!("TABSCHEMA = {}", db2_catalog_identifier_literal(schema)),
        format!("TABNAME = {}", db2_catalog_identifier_literal(table)),
    ];

    filters.push(replication_key_catalog_filter(config));

    format!(
        "SELECT COLNAME, TYPENAME, NULLS, HIDDEN, IDENTITY, GENERATED, KEYSEQ \
         FROM SYSCAT.COLUMNS \
         WHERE {} \
         ORDER BY COLNO",
        filters.join(" AND ")
    )
}

fn replication_key_catalog_filter(config: &Db2ProfileConfig) -> String {
    if !config.replication_key_columns.is_empty() {
        return format!(
            "UPPER(COLNAME) IN ({})",
            sql_string_literals_upper(&config.replication_key_columns)
        );
    }

    match config.profile {
        Db2Profile::Generic => "1 = 0".to_string(),
        Db2Profile::QReplication => format!(
            "UPPER(COLNAME) IN ({})",
            sql_string_literals_upper(default_qrep_columns())
        ),
        Db2Profile::Sailfish => {
            let exact_names = [
                DEFAULT_QREP_REPLKEY,
                DEFAULT_QREP_SITE,
                ROW_TRACKING_SYSROWID,
                ROW_TRACKING_CREATEXID,
                ROW_TRACKING_DELETEXID,
                NETEZZA_ROWID,
                NETEZZA_DATASLICEID,
            ];
            format!(
                "(UPPER(COLNAME) IN ({}) OR UPPER(COLNAME) LIKE '%IBMREPKEY%')",
                sql_string_literals_upper(exact_names)
            )
        }
        Db2Profile::RowModificationTracking => format!(
            "UPPER(COLNAME) IN ({})",
            sql_string_literals_upper([
                ROW_TRACKING_SYSROWID,
                ROW_TRACKING_CREATEXID,
                ROW_TRACKING_DELETEXID,
            ])
        ),
        Db2Profile::NetezzaSpecials => format!(
            "UPPER(COLNAME) IN ({})",
            sql_string_literals_upper([
                NETEZZA_ROWID,
                NETEZZA_DATASLICEID,
                ROW_TRACKING_CREATEXID,
                ROW_TRACKING_DELETEXID,
            ])
        ),
    }
}

pub fn key_constraint_catalog_query(schema: &str, table: &str) -> String {
    let schema = db2_catalog_identifier_literal(schema);
    let table = db2_catalog_identifier_literal(table);
    format!(
        "SELECT tc.CONSTNAME, tc.TYPE, tc.ENFORCED, tc.TRUSTED, kc.COLNAME, kc.COLSEQ \
         FROM SYSCAT.TABCONST tc \
         JOIN SYSCAT.KEYCOLUSE kc \
           ON tc.TABSCHEMA = kc.TABSCHEMA \
          AND tc.TABNAME = kc.TABNAME \
          AND tc.CONSTNAME = kc.CONSTNAME \
         WHERE tc.TABSCHEMA = {schema} \
           AND tc.TABNAME = {table} \
           AND tc.TYPE IN ('P', 'U') \
         ORDER BY tc.CONSTNAME, kc.COLSEQ"
    )
}

pub fn table_metadata_catalog_query(schema: &str, table: &str) -> String {
    let schema = db2_catalog_identifier_literal(schema);
    let table = db2_catalog_identifier_literal(table);
    format!(
        "SELECT TABLEORG, TBSPACEID \
         FROM SYSCAT.TABLES \
         WHERE TABSCHEMA = {schema} \
           AND TABNAME = {table}"
    )
}

pub fn unique_index_catalog_query(schema: &str, table: &str) -> String {
    let schema = db2_catalog_identifier_literal(schema);
    let table = db2_catalog_identifier_literal(table);
    format!(
        "SELECT i.INDNAME, c.COLNAME, c.COLSEQ \
         FROM SYSCAT.INDEXES i \
         JOIN SYSCAT.INDEXCOLUSE c \
           ON i.INDSCHEMA = c.INDSCHEMA AND i.INDNAME = c.INDNAME \
         WHERE i.TABSCHEMA = {schema} \
           AND i.TABNAME = {table} \
           AND i.UNIQUERULE IN ('P', 'U') \
           AND COALESCE(c.COLORDER, '') <> 'I' \
         ORDER BY i.INDNAME, c.COLSEQ"
    )
}

fn sql_string_literals_upper(values: impl IntoIterator<Item = impl AsRef<str>>) -> String {
    values
        .into_iter()
        .map(|value| sql_string_literal(&value.as_ref().to_ascii_uppercase()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn db2_catalog_identifier_literal(identifier: &str) -> String {
    let identifier = identifier.trim();
    if let Some(unquoted) = unquote_db2_delimited_identifier(identifier) {
        sql_string_literal(&unquoted)
    } else {
        sql_string_literal(&identifier.to_ascii_uppercase())
    }
}

fn unquote_db2_delimited_identifier(identifier: &str) -> Option<String> {
    if !identifier.starts_with('"') || !identifier.ends_with('"') || identifier.len() < 2 {
        return None;
    }

    let mut unquoted = String::with_capacity(identifier.len().saturating_sub(2));
    let mut chars = identifier[1..identifier.len() - 1].chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '"' {
            if chars.peek() == Some(&'"') {
                chars.next();
                unquoted.push('"');
            } else {
                return None;
            }
        } else {
            unquoted.push(ch);
        }
    }
    Some(unquoted)
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_config_parses_url_options() {
        let params = vec![
            (DB2_PROFILE_PARAM.to_string(), "sailfish".to_string()),
            (
                DB2_REPLICATION_KEY_COLUMNS_PARAM.to_string(),
                "IBMREPKEY1, IBMREPKEY2".to_string(),
            ),
        ];

        let config = Db2ProfileConfig::from_values_and_params(Some(&params), None, None).unwrap();
        assert_eq!(config.profile, Db2Profile::Sailfish);
        assert_eq!(
            config.replication_key_columns,
            vec!["IBMREPKEY1".to_string(), "IBMREPKEY2".to_string()]
        );
        assert!(config.is_active());
        assert!(config
            .runtime_scope_message()
            .unwrap()
            .contains("does not automatically alter extraction queries"));
    }

    #[test]
    fn profile_config_url_options_override_environment_defaults() {
        let params = vec![(DB2_PROFILE_PARAM.to_string(), "sailfish".to_string())];

        let config =
            Db2ProfileConfig::from_values_and_params(Some(&params), Some("qrep"), None).unwrap();

        assert_eq!(config.profile, Db2Profile::Sailfish);
        assert!(config.replication_key_columns.is_empty());
        assert!(config.is_active());
    }

    #[test]
    fn profile_config_url_columns_override_environment_columns() {
        let params = vec![(
            DB2_REPLICATION_KEY_COLUMNS_PARAM.to_string(),
            "IBMREPKEY1, IBMREPKEY2".to_string(),
        )];

        let config = Db2ProfileConfig::from_values_and_params(
            Some(&params),
            Some("sailfish"),
            Some("IBMQREP_REPLKEY,IBMQREP_SITE"),
        )
        .unwrap();

        assert_eq!(config.profile, Db2Profile::Sailfish);
        assert_eq!(
            config.replication_key_columns,
            vec!["IBMREPKEY1".to_string(), "IBMREPKEY2".to_string()]
        );
    }

    #[test]
    fn qrep_profile_defaults_to_public_ibm_replication_key_names() {
        let params = vec![(DB2_PROFILE_PARAM.to_string(), "qrep".to_string())];

        let config = Db2ProfileConfig::from_values_and_params(Some(&params), None, None).unwrap();
        assert_eq!(config.profile, Db2Profile::QReplication);
        assert_eq!(config.replication_key_columns, default_qrep_columns());
    }

    #[test]
    fn numeric_db2_type_detection_handles_case_and_precision() {
        assert!(is_numeric_db2_type("BIGINT"));
        assert!(is_numeric_db2_type("decimal(18,4)"));
        assert!(is_numeric_db2_type("DeCfLoAt"));
        assert!(!is_numeric_db2_type("INTEGERISH"));
        assert!(!is_numeric_db2_type("VARCHAR(32)"));
        assert!(!is_numeric_db2_type("ÅINTEGER("));
    }

    #[test]
    fn sailfish_profile_does_not_default_to_qrep_columns() {
        let params = vec![(DB2_PROFILE_PARAM.to_string(), "sailfish".to_string())];

        let config = Db2ProfileConfig::from_values_and_params(Some(&params), None, None).unwrap();

        assert_eq!(config.profile, Db2Profile::Sailfish);
        assert!(config.replication_key_columns.is_empty());
        assert!(config.is_active());
    }

    #[test]
    fn duplicate_replication_key_columns_are_rejected() {
        let err = parse_replication_key_columns("IBMREPKEY1, ibmrepkey1")
            .unwrap_err()
            .to_string();

        assert!(err.contains("duplicate column"));
    }

    #[test]
    fn diagnostic_detects_default_qrep_columns_and_enforced_unique_constraint() {
        let config = Db2ProfileConfig {
            profile: Db2Profile::QReplication,
            replication_key_columns: default_qrep_columns(),
        };
        let columns = vec![
            Db2CatalogColumn::new("IBMQREP_REPLKEY", "BIGINT")
                .not_null()
                .hidden()
                .identity(),
            Db2CatalogColumn::new("IBMQREP_SITE", "SMALLINT")
                .not_null()
                .hidden(),
        ];
        let constraints = vec![Db2KeyConstraint::new(
            "IBMQREP_UNIQCONST",
            Db2KeyConstraintKind::Unique,
            vec!["IBMQREP_REPLKEY".to_string(), "IBMQREP_SITE".to_string()],
        )];

        let diagnostic = diagnose_replication_key(&config, &columns, &constraints, &[]);

        assert_eq!(
            diagnostic.evidence,
            Db2ReplicationKeyEvidence::DefaultQReplicationColumns
        );
        assert_eq!(
            diagnostic.uniqueness,
            Db2ReplicationKeyUniqueness::EnforcedUniqueConstraint("IBMQREP_UNIQCONST".to_string())
        );
        assert_eq!(
            diagnostic.partition_hint,
            Db2PartitionHint::UseColumn {
                column: "IBMQREP_REPLKEY".to_string(),
                reason: "IBMQREP_REPLKEY is the leading numeric replication-key column; keep the full composite key for row identity".to_string(),
            }
        );
        assert!(diagnostic.warnings.is_empty());
    }

    #[test]
    fn diagnostic_reports_trusted_informational_qrep_constraint() {
        let config = Db2ProfileConfig {
            profile: Db2Profile::QReplication,
            replication_key_columns: default_qrep_columns(),
        };
        let columns = vec![
            Db2CatalogColumn::new("IBMQREP_REPLKEY", "BIGINT").not_null(),
            Db2CatalogColumn::new("IBMQREP_SITE", "SMALLINT").not_null(),
        ];
        let constraints = vec![Db2KeyConstraint::new(
            "IBMQREP_UNIQCONST",
            Db2KeyConstraintKind::Unique,
            vec!["IBMQREP_REPLKEY".to_string(), "IBMQREP_SITE".to_string()],
        )
        .not_enforced(true)];

        let diagnostic = diagnose_replication_key(&config, &columns, &constraints, &[]);

        assert_eq!(
            diagnostic.uniqueness,
            Db2ReplicationKeyUniqueness::TrustedInformationalConstraint(
                "IBMQREP_UNIQCONST".to_string()
            )
        );
        assert_eq!(diagnostic.warnings.len(), 1);
    }

    #[test]
    fn diagnostic_uses_unique_index_after_constraints() {
        let config = Db2ProfileConfig {
            profile: Db2Profile::QReplication,
            replication_key_columns: default_qrep_columns(),
        };
        let columns = vec![
            Db2CatalogColumn::new("IBMQREP_REPLKEY", "BIGINT").not_null(),
            Db2CatalogColumn::new("IBMQREP_SITE", "SMALLINT").not_null(),
        ];
        let indexes = vec![Db2UniqueIndex::new(
            "UX_QREP".to_string(),
            vec!["IBMQREP_REPLKEY".to_string(), "IBMQREP_SITE".to_string()],
        )];

        let diagnostic = diagnose_replication_key(&config, &columns, &[], &indexes);

        assert_eq!(
            diagnostic.uniqueness,
            Db2ReplicationKeyUniqueness::UniqueIndex("UX_QREP".to_string())
        );
        assert!(diagnostic.warnings.is_empty());
    }

    #[test]
    fn diagnostic_does_not_treat_keyseq_alone_as_primary_key() {
        let config = Db2ProfileConfig {
            profile: Db2Profile::QReplication,
            replication_key_columns: default_qrep_columns(),
        };
        let columns = vec![
            Db2CatalogColumn::new("IBMQREP_REPLKEY", "BIGINT")
                .not_null()
                .key_sequence(1),
            Db2CatalogColumn::new("IBMQREP_SITE", "SMALLINT")
                .not_null()
                .key_sequence(2),
        ];

        let diagnostic = diagnose_replication_key(&config, &columns, &[], &[]);

        assert_eq!(
            diagnostic.uniqueness,
            Db2ReplicationKeyUniqueness::ProfileConventionOnly
        );
        assert_eq!(diagnostic.warnings.len(), 1);
    }

    #[test]
    fn diagnostic_detects_sailfish_ibmrepkey_name_pattern_without_unique_evidence() {
        let config = Db2ProfileConfig {
            profile: Db2Profile::Sailfish,
            replication_key_columns: vec![],
        };
        let columns = vec![
            Db2CatalogColumn::new("IBMREPKEY_A", "BIGINT").not_null(),
            Db2CatalogColumn::new("IBMREPKEY_B", "SMALLINT").not_null(),
        ];

        let diagnostic = diagnose_replication_key(&config, &columns, &[], &[]);

        assert_eq!(
            diagnostic.evidence,
            Db2ReplicationKeyEvidence::IbmRepKeyNamePattern
        );
        assert_eq!(
            diagnostic.uniqueness,
            Db2ReplicationKeyUniqueness::ProfileConventionOnly
        );
        assert_eq!(diagnostic.candidate_columns.len(), 2);
        assert_eq!(diagnostic.warnings.len(), 1);
    }

    #[test]
    fn diagnostic_detects_sailfish_row_modification_tracking() {
        let config = Db2ProfileConfig {
            profile: Db2Profile::Sailfish,
            replication_key_columns: vec![],
        };
        let columns = vec![
            Db2CatalogColumn::new("SYSROWID", "BIGINT")
                .not_null()
                .hidden(),
            Db2CatalogColumn::new("CREATEXID", "BIGINT")
                .not_null()
                .hidden(),
            Db2CatalogColumn::new("DELETEXID", "BIGINT")
                .not_null()
                .hidden(),
        ];

        let diagnostic = diagnose_replication_key(&config, &columns, &[], &[]);

        assert_eq!(
            diagnostic.evidence,
            Db2ReplicationKeyEvidence::RowModificationTrackingColumns
        );
        assert_eq!(
            diagnostic.candidate_columns,
            vec![Db2CatalogColumn::new("SYSROWID", "BIGINT")
                .not_null()
                .hidden()]
        );
        assert_eq!(diagnostic.warnings.len(), 2);
    }

    #[test]
    fn diagnostic_detects_netezza_special_rowid() {
        let config = Db2ProfileConfig {
            profile: Db2Profile::NetezzaSpecials,
            replication_key_columns: vec![],
        };
        let columns = vec![
            Db2CatalogColumn::new("ROWID", "BIGINT").not_null(),
            Db2CatalogColumn::new("DATASLICEID", "INTEGER").not_null(),
            Db2CatalogColumn::new("CREATEXID", "BIGINT").not_null(),
            Db2CatalogColumn::new("DELETEXID", "BIGINT").not_null(),
        ];

        let diagnostic = diagnose_replication_key(&config, &columns, &[], &[]);

        assert_eq!(
            diagnostic.evidence,
            Db2ReplicationKeyEvidence::NetezzaSpecialColumns
        );
        assert_eq!(
            diagnostic.candidate_columns,
            vec![Db2CatalogColumn::new("ROWID", "BIGINT").not_null()]
        );
        assert_eq!(diagnostic.warnings.len(), 2);
    }

    #[test]
    fn generic_profile_does_not_infer_replication_keys() {
        let config = Db2ProfileConfig::default();
        let columns = vec![
            Db2CatalogColumn::new("IBMREPKEY_A", "BIGINT").not_null(),
            Db2CatalogColumn::new("IBMREPKEY_B", "SMALLINT").not_null(),
        ];

        let diagnostic = diagnose_replication_key(&config, &columns, &[], &[]);

        assert_eq!(diagnostic.evidence, Db2ReplicationKeyEvidence::NotDetected);
        assert!(diagnostic.candidate_columns.is_empty());
    }

    #[test]
    fn catalog_query_escapes_literals_and_filters_configured_columns() {
        let config = Db2ProfileConfig {
            profile: Db2Profile::Sailfish,
            replication_key_columns: vec!["IBMREPKEY1".to_string(), "IBMREPKEY2".to_string()],
        };

        let query = replication_key_catalog_query("risk'schema", "trade", &config);

        assert!(query.contains("TABSCHEMA = 'RISK''SCHEMA'"));
        assert!(query.contains("TABNAME = 'TRADE'"));
        assert!(query.contains("UPPER(COLNAME) IN ('IBMREPKEY1', 'IBMREPKEY2')"));
        assert!(query.contains("SYSCAT.COLUMNS"));
    }

    #[test]
    fn catalog_queries_preserve_delimited_mixed_case_identifiers() {
        let config = Db2ProfileConfig {
            profile: Db2Profile::Sailfish,
            replication_key_columns: vec![],
        };

        let query = replication_key_catalog_query("\"Risk'Schema\"", "\"Trade\"\"Table\"", &config);

        assert!(query.contains("TABSCHEMA = 'Risk''Schema'"));
        assert!(query.contains("TABNAME = 'Trade\"Table'"));
    }

    #[test]
    fn catalog_query_for_sailfish_searches_all_known_profile_families() {
        let config = Db2ProfileConfig {
            profile: Db2Profile::Sailfish,
            replication_key_columns: vec![],
        };

        let query = replication_key_catalog_query("risk", "trade", &config);

        assert!(query.contains("UPPER(COLNAME) LIKE '%IBMREPKEY%'"));
        assert!(query.contains("'IBMQREP_REPLKEY'"));
        assert!(query.contains("'IBMQREP_SITE'"));
        assert!(query.contains("'SYSROWID'"));
        assert!(query.contains("'CREATEXID'"));
        assert!(query.contains("'DELETEXID'"));
        assert!(query.contains("'ROWID'"));
        assert!(query.contains("'DATASLICEID'"));
    }

    #[test]
    fn generic_catalog_query_does_not_fetch_every_column() {
        let query = replication_key_catalog_query("risk", "trade", &Db2ProfileConfig::default());

        assert!(query.contains("1 = 0"));
    }

    #[test]
    fn key_constraint_query_uses_constraint_catalog_metadata() {
        let query = key_constraint_catalog_query("risk", "trade's");

        assert!(query.contains("SYSCAT.TABCONST"));
        assert!(query.contains("SYSCAT.KEYCOLUSE"));
        assert!(query.contains("tc.TYPE IN ('P', 'U')"));
        assert!(query.contains("tc.TABSCHEMA = 'RISK'"));
        assert!(query.contains("tc.TABNAME = 'TRADE''S'"));
        assert!(query.contains("tc.ENFORCED"));
        assert!(query.contains("tc.TRUSTED"));
    }

    #[test]
    fn table_metadata_query_reads_db2_table_shape() {
        let query = table_metadata_catalog_query("risk", "trade's");

        assert!(query.contains("SYSCAT.TABLES"));
        assert!(query.contains("TABLEORG"));
        assert!(query.contains("TBSPACEID"));
        assert!(query.contains("TABSCHEMA = 'RISK'"));
        assert!(query.contains("TABNAME = 'TRADE''S'"));
    }

    #[test]
    fn unique_index_query_uses_catalog_index_metadata() {
        let query = unique_index_catalog_query("risk", "trade's");

        assert!(query.contains("SYSCAT.INDEXES"));
        assert!(query.contains("SYSCAT.INDEXCOLUSE"));
        assert!(query.contains("i.UNIQUERULE IN ('P', 'U')"));
        assert!(query.contains("COALESCE(c.COLORDER, '') <> 'I'"));
        assert!(query.contains("i.TABSCHEMA = 'RISK'"));
        assert!(query.contains("i.TABNAME = 'TRADE''S'"));
    }
}
