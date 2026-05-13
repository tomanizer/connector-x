# Getting Started

## Installation

### Pip

The easiest way to install ConnectorX is using pip, with the following command:

```bash
pip install connectorx
```

The Python wheel includes ConnectorX itself, but not database-specific ODBC drivers. To use generic ODBC, Sybase, or Db2 sources, install the platform ODBC manager and the target database driver separately:

* Linux needs unixODBC runtime libraries and the target driver registered with unixODBC.
* macOS needs Homebrew `unixodbc` and the target driver installed locally.
* Windows uses the Windows ODBC manager and needs the vendor driver installed and registered.

See the [ODBC database page](./databases/odbc.md) for connection-string forms, driver examples, and live-test setup.
Release and package-index verification now smoke-test the platform ODBC manager on Linux, macOS, and Windows, but they still do not bundle or validate every database-specific driver you may use in production.

### Build from source code

* Step 0: Install tools.
    * Install Rust: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
    * Install [just](https://github.com/casey/just): `cargo install just`
    * Install [Poetry](https://python-poetry.org/docs/): `pip3 install poetry`

* Step 1: Fresh clone of source.
```bash
git clone https://github.com/sfu-db/connector-x.git
```

* Step 2: Install and switch to the correct rust version (please refer [this file](https://github.com/sfu-db/connector-x/blob/main/.github/workflows/release.yml) and search for `rust` for the latest using version).
```bash
rustup install {version}
rustup override set {version}
```

* Step 3: Install system dependencies. Please refer to [release.yml](https://github.com/sfu-db/connector-x/blob/main/.github/workflows/release.yml) for dependencies needed for different operating systems.

    ConnectorX wheels link against the platform ODBC manager so the Python package can expose Sybase, Db2, and generic ODBC sources. Database-specific ODBC drivers are runtime dependencies and are not bundled in wheels.

    * Linux source builds need unixODBC development headers, for example `unixodbc-dev` on Debian/Ubuntu or `unixODBC-devel` on RHEL-compatible distributions.
    * macOS source builds need Homebrew `unixodbc`.
    * Windows source builds use the Windows ODBC manager and do not need unixODBC, but users still need to install the vendor driver for the database they connect to.
    * At runtime, install/register the matching database driver separately, such as FreeTDS or SAP ASE SDK for Sybase, IBM Data Server Driver for ODBC and CLI for Db2, or psqlODBC for PostgreSQL-backed generic ODBC.

* Step 4: Install python dependencies.
```bash
just bootstrap-python
```

* Step 5: Build wheel.
```bash
just build-python-wheel
```

NOTES:
* `OPENSSL_NO_VENDOR=1` might required to compile for windows users.
* Dynamic library is required for the python installation. (e.g. If you are using `pyenv`, use command `PYTHON_CONFIGURE_OPTS=“--enable-shared” pyenv install {version}` to install python since dylib is not enabled by default.)
