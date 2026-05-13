//! Transport from Odbc Source to Arrow Destination.

impl_odbc_family_arrow_transport!(
    module = odbc_arrow_transport,
    destination = arrow,
    transport = OdbcArrowTransport,
    error = OdbcArrowTransportError,
    source_module = odbc,
    source = OdbcSource,
    source_error = OdbcSourceError,
    type_system = OdbcTypeSystem
);
