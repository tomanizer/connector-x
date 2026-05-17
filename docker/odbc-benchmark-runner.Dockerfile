FROM rust:1-bookworm

ENV DEBIAN_FRONTEND=noninteractive

RUN test "$(dpkg --print-architecture)" = "amd64" \
    || (echo "The IBM Db2 ODBC/CLI driver used by this image is Linux x86_64 only. Build with --platform linux/amd64." >&2 && exit 1)

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        cmake \
        curl \
        freetds-bin \
        git \
        jq \
        libclang-dev \
        libkrb5-dev \
        libpq-dev \
        libssl-dev \
        odbc-postgresql \
        pkg-config \
        python3-dev \
        python3-pip \
        python3-venv \
        tdsodbc \
        unixodbc \
        unixodbc-dev \
    && (apt-get install -y --no-install-recommends libaio1 || apt-get install -y --no-install-recommends libaio1t64) \
    && rm -rf /var/lib/apt/lists/*

ENV VIRTUAL_ENV=/opt/connectorx-bench-venv
ENV PATH="/opt/connectorx-bench-venv/bin:${PATH}"
ENV DB2_CLI_DRIVER_LIB_DIR=/opt/ibm/clidriver/lib
ENV LD_LIBRARY_PATH=/opt/ibm/clidriver/lib

RUN python3 -m venv "$VIRTUAL_ENV" \
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
    && mkdir -p /opt/ibm \
    && python - <<'PY' > /tmp/db2-clidriver-dir
import pathlib
import sys

import ibm_db

module_path = pathlib.Path(ibm_db.__file__).resolve()
for parent in (module_path.parent, *module_path.parents):
    candidate = parent / "clidriver"
    if (candidate / "lib" / "libdb2.so").exists():
        print(candidate)
        sys.exit(0)

raise SystemExit("ibm_db clidriver/lib/libdb2.so not found")
PY
RUN ln -s "$(cat /tmp/db2-clidriver-dir)" /opt/ibm/clidriver \
    && test -f /opt/ibm/clidriver/lib/libdb2.so \
    && { \
        echo ""; \
        echo "[IBM DB2 ODBC DRIVER]"; \
        echo "Description=IBM Db2 ODBC CLI Driver"; \
        echo "Driver=/opt/ibm/clidriver/lib/libdb2.so"; \
        echo "FileUsage=1"; \
    } >> /etc/odbcinst.ini

WORKDIR /workspace

COPY . /workspace
COPY scripts/odbc_driver_comparison_container_entrypoint.sh /usr/local/bin/connectorx-odbc-benchmark
RUN chmod +x /usr/local/bin/connectorx-odbc-benchmark

ENTRYPOINT ["connectorx-odbc-benchmark"]
CMD ["benchmark"]
