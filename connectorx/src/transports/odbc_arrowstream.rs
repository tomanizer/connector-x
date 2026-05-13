//! Transport from Odbc Source to ArrowStream Destination.

impl_odbc_family_arrow_transport!(
    module = odbc_arrowstream_transport,
    destination = arrowstream,
    transport = OdbcArrowTransport,
    error = OdbcArrowTransportError,
    source_module = odbc,
    source = OdbcSource,
    source_error = OdbcSourceError,
    type_system = OdbcTypeSystem
);
