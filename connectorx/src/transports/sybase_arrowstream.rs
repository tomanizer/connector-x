//! Transport from Sybase Source to ArrowStream Destination.

impl_odbc_family_arrow_transport!(
    module = sybase_arrowstream_transport,
    destination = arrowstream,
    transport = SybaseArrowTransport,
    error = SybaseArrowTransportError,
    source_module = sybase,
    source = SybaseSource,
    source_error = SybaseSourceError,
    type_system = SybaseTypeSystem
);
