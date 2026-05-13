//! Transport from Db2 Source to ArrowStream Destination.

impl_odbc_family_arrow_transport!(
    module = db2_arrowstream_transport,
    destination = arrowstream,
    transport = Db2ArrowTransport,
    error = Db2ArrowTransportError,
    source_module = db2,
    source = Db2Source,
    source_error = Db2SourceError,
    type_system = Db2TypeSystem
);
