FROM icr.io/db2_community/db2:latest

ARG RUST_TOOLCHAIN=stable

RUN test "$(uname -m)" = "x86_64" \
    || (echo "The IBM Db2 ODBC driver used by this image is Linux x86_64 only. Build with --platform linux/amd64." >&2 && exit 1)

RUN dnf install -y \
        ca-certificates \
        clang \
        cmake \
        freetds \
        freetds-devel \
        gcc \
        gcc-c++ \
        git \
        jq \
        libpq-devel \
        make \
        openssl-devel \
        postgresql-odbc \
        pkgconf-pkg-config \
        python3.11 \
        python3.11-devel \
        python3.11-pip \
        rust \
        cargo \
        unixODBC \
        unixODBC-devel \
    && dnf clean all \
    && rm -rf /var/cache/dnf

ENV VIRTUAL_ENV=/opt/connectorx-bench-venv
ENV CARGO_HOME=/usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup
ENV DB2_CLIENT_HOME=/home/db2bench/sqllib
ENV DB2_CLIENT_PROFILE_PATH=/home/db2bench/sqllib/db2profile
ENV DB2_CLI_DRIVER_LIB_DIR=/home/db2bench/sqllib/lib64
ENV CARGO_BUILD_JOBS=1
ENV LD_LIBRARY_PATH=/home/db2bench/sqllib/lib64:/home/db2bench/sqllib/lib64/gskit:/home/db2bench/sqllib/lib64/icc
ENV PATH="/opt/connectorx-bench-venv/bin:/usr/local/cargo/bin:${PATH}"

RUN python3.11 -m venv "$VIRTUAL_ENV" \
    && pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir \
        arrow-odbc \
        ibm_db \
        ibm_db_sa \
        maturin \
        pandas \
        polars \
        psutil \
        psycopg2-binary \
        pyarrow \
        pyodbc \
        sqlalchemy \
    && mkdir -p "$CARGO_HOME/registry" "$CARGO_HOME/git"

RUN groupadd -g 2000 db2iadm1 \
    && useradd -m -u 2000 -g db2iadm1 db2bench \
    && /opt/ibm/db2/V12.1/instance/db2icrt -s client db2bench \
    && test -f /home/db2bench/sqllib/lib64/libdb2o.so \
    && test -f /usr/lib64/libtdsodbc.so \
    && test -f /usr/lib64/psqlodbcw.so \
    && if ! odbcinst -q -d -n "FreeTDS" >/dev/null 2>&1; then { \
        echo ""; \
        echo "[FreeTDS]"; \
        echo "Description=FreeTDS unixODBC Driver"; \
        echo "Driver=/usr/lib64/libtdsodbc.so"; \
        echo "Setup=/usr/lib64/libtdsS.so"; \
        echo "FileUsage=1"; \
        echo "UsageCount=1"; \
    } >> /etc/odbcinst.ini; fi \
    && if ! odbcinst -q -d -n "PostgreSQL Unicode" >/dev/null 2>&1; then { \
        echo ""; \
        echo "[PostgreSQL Unicode]"; \
        echo "Description=PostgreSQL ODBC Unicode Driver"; \
        echo "Driver=/usr/lib64/psqlodbcw.so"; \
        echo "Setup=/usr/lib64/libodbcpsqlS.so"; \
        echo "FileUsage=1"; \
        echo "UsageCount=1"; \
    } >> /etc/odbcinst.ini; fi \
    && if ! odbcinst -q -d -n "IBM DB2 ODBC DRIVER" >/dev/null 2>&1; then { \
        echo ""; \
        echo "[IBM DB2 ODBC DRIVER]"; \
        echo "Description=IBM Db2 ODBC Driver"; \
        echo "Driver=/home/db2bench/sqllib/lib64/libdb2o.so"; \
        echo "FileUsage=1"; \
        echo "DontDLClose=1"; \
    } >> /etc/odbcinst.ini; fi

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --profile minimal --default-toolchain "$RUST_TOOLCHAIN" \
    && rustc --version \
    && cargo --version

RUN dnf install -y krb5-devel \
    && dnf clean all \
    && rm -rf /var/cache/dnf

ENV OPENSSL_NO_VENDOR=1

WORKDIR /workspace

COPY . /workspace
COPY scripts/odbc_driver_comparison_container_entrypoint.sh /usr/local/bin/connectorx-odbc-benchmark
RUN chmod +x /usr/local/bin/connectorx-odbc-benchmark

ENTRYPOINT ["connectorx-odbc-benchmark"]
CMD ["benchmark"]
