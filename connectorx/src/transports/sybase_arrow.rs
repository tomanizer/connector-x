//! Transport from Sybase Source to Arrow Destination.

impl_odbc_family_arrow_transport!(
    module = sybase_arrow_transport,
    destination = arrow,
    transport = SybaseArrowTransport,
    error = SybaseArrowTransportError,
    source_module = sybase,
    source = SybaseSource,
    source_error = SybaseSourceError,
    type_system = SybaseTypeSystem
);
