//! Transport from Odbc Source to Arrow Destination.

impl_odbc_family_arrow_transport!(
    module = odbc_arrow_transport,
    destination = arrow,
    transport = OdbcArrowTransport,
    error = OdbcArrowTransportError,
    source_module = odbc,
    source = OdbcSource,
    source_error = OdbcSourceError,
    type_system = OdbcTypeSystem,
    extra_mappings = {
        { WChar[String]                => LargeUtf8[String]     | conversion none }
        { WVarchar[String]             => LargeUtf8[String]     | conversion none }
        { WText[String]                => LargeUtf8[String]     | conversion none }
    }
);
