set dotenv-load := true

build-release:
    cargo build --release --features all

build-debug:
    cargo build --features all

build-cpp +ARGS="":
    cd connectorx-cpp && cargo build {{ARGS}}

build-cpp-release +ARGS="":
    cd connectorx-cpp && cargo build --release {{ARGS}}

test +ARGS="": 
    cargo test --features all {{ARGS}} -- --nocapture

test-ci: 
    cargo test --features src_postgres --features dst_arrow --test test_postgres
    cargo test --features src_postgres --features src_dummy --features dst_polars --test test_polars

test-feature-gate:
    cargo c --features src_postgres
    cargo c --features src_mysql
    cargo c --features src_mssql
    cargo c --features src_sybase
    cargo c --features src_db2
    cargo c --features src_sqlite
    cargo c --features src_oracle
    cargo c --features src_trino
    cargo c --features src_clickhouse
    cargo c --features dst_arrow

bench-sybase-odbc:
    cargo bench -p connectorx --features "src_sybase dst_arrow" --bench sybase_odbc

bench-db2-odbc:
    cargo bench -p connectorx --features "src_db2 dst_arrow" --bench db2_odbc

bench-odbc:
    cargo bench -p connectorx --no-default-features --features "src_odbc dst_arrow fptr" --bench odbc

test-odbc-live target="all":
    #!/usr/bin/env bash
    set -euo pipefail
    run_cargo_test() {
        local features="$1"
        local test_name="$2"
        local coverage_msg="$3"
        shift 3
        echo "ODBC_COVERAGE: $coverage_msg"
        cargo test -p connectorx --no-default-features --features "$features" --test "$test_name" -- --nocapture "$@"
    }
    run_postgres() {
        CONNECTORX_ODBC_TESTCONTAINER=1 \
            run_cargo_test "src_odbc dst_arrow fptr" "test_odbc" "running generic ODBC PostgreSQL testcontainer coverage" --test-threads=1
    }
    run_sybase() {
        if [ -z "${SYBASE_ODBC_CONN:-}" ] && [ -z "${SYBASE_URL:-}" ]; then
            echo "CONNECTORX_SKIP: skipping Sybase live tests: set SYBASE_ODBC_CONN and/or SYBASE_URL"
            return
        fi
        run_cargo_test "src_sybase dst_arrow fptr" "test_sybase" "running secret/local Sybase ODBC coverage"
    }
    run_db2() {
        if [ -z "${DB2_ODBC_CONN:-}" ] && [ -z "${DB2_URL:-}" ]; then
            echo "CONNECTORX_SKIP: skipping Db2 live tests: set DB2_ODBC_CONN and/or DB2_URL"
            return
        fi
        run_cargo_test "src_db2 dst_arrow fptr" "test_db2" "running secret/local Db2 ODBC coverage"
    }
    run_odbc() {
        if [ -z "${ODBC_TEST_QUERY:-}" ] || { [ -z "${ODBC_CONN:-}" ] && [ -z "${ODBC_URL:-}" ]; }; then
            echo "CONNECTORX_SKIP: skipping generic ODBC live tests: set ODBC_TEST_QUERY plus ODBC_CONN and/or ODBC_URL"
            return
        fi
        run_cargo_test "src_odbc dst_arrow fptr" "test_odbc" "running secret/local generic ODBC coverage"
    }
    case "{{target}}" in
        all)
            run_postgres
            run_sybase
            run_db2
            run_odbc
            ;;
        postgres) run_postgres ;;
        sybase) run_sybase ;;
        db2) run_db2 ;;
        odbc) run_odbc ;;
        *)
            echo "unknown ODBC live target '{{target}}'; use all, postgres, sybase, db2, or odbc" >&2
            exit 2
            ;;
    esac

start-db2-docker:
    #!/usr/bin/env bash
    set -euo pipefail
    container="${DB2_CONTAINER:-connectorx-db2}"
    port="${DB2_PORT:-50000}"
    db="${DB2_DB:-testdb}"
    password="${DB2_PASSWORD:-connectorx1}"
    data_volume="${DB2_DATA_VOLUME:-connectorx-db2-data}"
    image="${DB2_IMAGE:-icr.io/db2_community/db2:latest}"
    platform="${DB2_DOCKER_PLATFORM:-linux/amd64}"
    if [ -n "${DB2_DATA_DIR:-}" ]; then
        mkdir -p "$DB2_DATA_DIR"
        data_mount="$DB2_DATA_DIR:/database"
    else
        docker volume create "$data_volume" >/dev/null
        data_mount="$data_volume:/database"
    fi
    if docker ps -a --format '{{ "{{" }}.Names{{ "}}" }}' | grep -qx "$container"; then
        docker start "$container"
    else
        docker run -d \
            --name "$container" \
            --platform "$platform" \
            --privileged=true \
            -p "$port:50000" \
            -e LICENSE=accept \
            -e DB2INST1_PASSWORD="$password" \
            -e DBNAME="$db" \
            -v "$data_mount" \
            "$image"
    fi
    echo "DB2_URL=db2://db2inst1:$password@127.0.0.1:$port/$db?driver=IBM%20DB2%20ODBC%20DRIVER"
    echo "DB2_ODBC_CONN=Driver={IBM DB2 ODBC DRIVER};Hostname=127.0.0.1;Port=$port;Protocol=TCPIP;Database=$db;UID=db2inst1;PWD=$password;"

logs-db2-docker:
    docker logs -f ${DB2_CONTAINER:-connectorx-db2}

seed-db2-docker:
    #!/usr/bin/env bash
    set -euo pipefail
    container="${DB2_CONTAINER:-connectorx-db2}"
    db="${DB2_DB:-testdb}"
    docker cp scripts/db2.sql "$container:/tmp/connectorx-db2.sql"
    docker exec "$container" bash -lc "su - db2inst1 -c 'db2 connect to $db && db2 -td@ -vf /tmp/connectorx-db2.sql'"

check-db2-linux-odbc:
    #!/usr/bin/env bash
    set -euo pipefail
    host="${DB2_HOST:-host.docker.internal}"
    port="${DB2_PORT:-50000}"
    db="${DB2_DB:-testdb}"
    password="${DB2_PASSWORD:-connectorx1}"
    docker run --rm --platform linux/amd64 -e DEBIAN_FRONTEND=noninteractive debian:bookworm-slim bash -lc "
        set -euo pipefail
        apt-get update >/dev/null
        apt-get install -y --no-install-recommends ca-certificates curl tar gzip unixodbc libxml2 libstdc++6 libaio1 >/dev/null
        mkdir -p /opt/ibm/db2
        curl -fsSL https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/linuxx64_odbc_cli.tar.gz -o /tmp/linuxx64_odbc_cli.tar.gz
        tar -xzf /tmp/linuxx64_odbc_cli.tar.gz -C /opt/ibm/db2
        export LD_LIBRARY_PATH=/opt/ibm/db2/clidriver/lib:\${LD_LIBRARY_PATH:-}
        printf 'select count(*) from cx_db2_test;\n' | isql -v -k \"Driver=/opt/ibm/db2/clidriver/lib/libdb2.so;Hostname=$host;Port=$port;Protocol=TCPIP;Database=$db;UID=db2inst1;PWD=$password;\"
    "

test-db2-docker:
    #!/usr/bin/env bash
    set -euo pipefail
    container="${DB2_CONTAINER:-connectorx-db2}"
    db="${DB2_DB:-testdb}"
    password="${DB2_PASSWORD:-connectorx1}"
    src_dir="${DB2_TEST_SRC_DIR:-/tmp/connectorx-src}"
    git ls-files -z --cached --others --exclude-standard \
        | COPYFILE_DISABLE=1 tar --no-xattrs --no-mac-metadata --format=ustar --no-recursion --null -T - -cf - \
        | docker exec -i "$container" bash -lc "rm -rf '$src_dir' && mkdir -p '$src_dir' && tar -xf - -C '$src_dir'"
    docker exec "$container" bash -lc "
        set -euo pipefail
        dnf install -y gcc gcc-c++ make pkgconf-pkg-config unixODBC unixODBC-devel openssl-devel ca-certificates perl >/tmp/connectorx-dnf.log 2>&1
        if [ ! -x /root/.cargo/bin/cargo ]; then
            curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal
        fi
        if [ -f /database/config/db2inst1/sqllib/db2profile ]; then
            . /database/config/db2inst1/sqllib/db2profile
        else
            . /opt/ibm/db2/V12.1/cfg/db2profile
        fi
        export PATH=/root/.cargo/bin:\$PATH
        export LD_LIBRARY_PATH=/opt/ibm/db2/V12.1/lib64:\${LD_LIBRARY_PATH:-}
        export CARGO_TARGET_DIR=/tmp/connectorx-db2-target
        cd '$src_dir'
        DB2_ODBC_CONN=\"Driver=/opt/ibm/db2/V12.1/lib64/libdb2o.so;Hostname=127.0.0.1;Port=50000;Protocol=TCPIP;Database=$db;UID=db2inst1;PWD=$password;\" \
        DB2_URL=\"db2://db2inst1:$password@127.0.0.1:50000/$db?driver=%2Fopt%2Fibm%2Fdb2%2FV12.1%2Flib64%2Flibdb2o.so\" \
        cargo test -p connectorx --features 'src_db2 dst_arrow' --test test_db2 -- --nocapture
    "

cleanup:
    cargo clean
    cd connectorx-python && cargo clean
    rm connectorx-python/connectorx/connectorx*.so

bootstrap-python:
    cd connectorx-python && poetry install

setup-java:
    cd $ACCIO_PATH/rewriter && mvn package -Dmaven.test.skip=true
    cp -f $ACCIO_PATH/rewriter/target/accio-rewriter-1.0-SNAPSHOT-jar-with-dependencies.jar connectorx-python/connectorx/dependencies/federated-rewriter.jar

setup-python:
    cd connectorx-python && poetry run maturin develop --release
    
test-python +opts="": setup-python
    cd connectorx-python && poetry run pytest connectorx/tests -v -s {{opts}}

test-python-s +opts="":
    cd connectorx-python && poetry run pytest connectorx/tests -v -s {{opts}}

seed-db:
    #!/bin/bash
    psql $POSTGRES_URL -f scripts/postgres.sql
    sqlite3 ${SQLITE_URL#sqlite://} < scripts/sqlite.sql
    mysql --protocol tcp -h$MYSQL_HOST -P$MYSQL_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DB < scripts/mysql.sql

# dbs not included in ci
seed-db-more:
    mssql-cli -S$MSSQL_HOST -U$MSSQL_USER -P$MSSQL_PASSWORD -d$MSSQL_DB -i scripts/mssql.sql
    psql $REDSHIFT_URL -f scripts/redshift.sql
    ORACLE_URL_SCRIPT=`echo ${ORACLE_URL#oracle://} | sed "s/:/\//"`
    cat scripts/oracle.sql | sqlplus $ORACLE_URL_SCRIPT
    mysql --protocol tcp -h$MARIADB_HOST -P$MARIADB_PORT -u$MARIADB_USER -p$MARIADB_PASSWORD $MARIADB_DB < scripts/mysql.sql
    trino $TRINO_URL --catalog=$TRINO_CATALOG < scripts/trino.sql
    clickhouse-client -h $CLICKHOUSE_HOST --port $CLICKHOUSE_PORT -u $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD -d $CLICKHOUSE_DB < scripts/clickhouse.sql

# benches 
flame-tpch conn="POSTGRES_URL":
    cd connectorx-python && PYO3_PYTHON=$HOME/.pyenv/versions/3.8.6/bin/python3.8 PYTHONPATH=$HOME/.pyenv/versions/conn/lib/python3.8/site-packages LD_LIBRARY_PATH=$HOME/.pyenv/versions/3.8.6/lib/ cargo run --no-default-features --features executable --features fptr --features nbstr --features dsts --features srcs --release --example flame_tpch {{conn}}

build-tpch:
    cd connectorx-python && cargo build --no-default-features --features executable --features fptr --release --example tpch

cachegrind-tpch: build-tpch
    valgrind --tool=cachegrind target/release/examples/tpch

python-tpch name +ARGS="": setup-python
    #!/bin/bash
    export PYTHONPATH=$PWD/connectorx-python
    cd connectorx-python && \
    poetry run python ../benchmarks/tpch-{{name}}.py {{ARGS}}

python-tpch-ext name +ARGS="":
    cd connectorx-python && poetry run python ../benchmarks/tpch-{{name}}.py {{ARGS}}

python-ddos name +ARGS="": setup-python
    #!/bin/bash
    export PYTHONPATH=$PWD/connectorx-python
    cd connectorx-python && \
    poetry run python ../benchmarks/ddos-{{name}}.py {{ARGS}}

python-ddos-ext name +ARGS="":
    cd connectorx-python && poetry run python ../benchmarks/ddos-{{name}}.py {{ARGS}}


python-shell:
    cd connectorx-python && \
    poetry run ipython

benchmark-report: setup-python
    cd connectorx-python && \
    poetry run pytest connectorx/tests/benchmarks.py --benchmark-json ../benchmark.json

odbc-driver-comparison +ARGS="":
    python3 scripts/odbc_driver_comparison.py {{ARGS}}

odbc-driver-comparison-container +ARGS="":
    #!/usr/bin/env bash
    set -euo pipefail
    HOST_UID="$(id -u)" HOST_GID="$(id -g)" \
        docker compose -f docker-compose.odbc-driver-comparison.yml run --rm --build benchmark benchmark {{ARGS}}

odbc-driver-comparison-container-smoke:
    #!/usr/bin/env bash
    set -euo pipefail
    HOST_UID="$(id -u)" HOST_GID="$(id -g)" \
        docker compose -f docker-compose.odbc-driver-comparison.yml run --rm --build benchmark smoke
    
# releases
build-python-wheel:
    cd connectorx-python && maturin build --release -i python

# release with federation enabled
build-python-wheel-fed:
    # need to get the j4rs dependency first
    cd connectorx-python && maturin build --release -i python
    # copy files
    cp -rf connectorx-python/target/release/jassets connectorx-python/connectorx/dependencies
    # build final wheel
    cd connectorx-python && maturin build --release -i python
