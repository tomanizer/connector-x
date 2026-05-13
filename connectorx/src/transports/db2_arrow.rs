//! Transport from Db2 Source to Arrow Destination.

impl_odbc_family_arrow_transport!(
    module = db2_arrow_transport,
    destination = arrow,
    transport = Db2ArrowTransport,
    error = Db2ArrowTransportError,
    source_module = db2,
    source = Db2Source,
    source_error = Db2SourceError,
    type_system = Db2TypeSystem
);
