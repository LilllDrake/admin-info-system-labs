#!/usr/bin/env bash
set -euo pipefail

# Подготавливаем каталог данных и его владельца.
mkdir -p "$PGDATA"
chown postgres:postgres "$PGDATA"
chmod 700 "$PGDATA"

# При первом запуске получаем начальную копию primary.
if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "Creating a base backup from primary..."

    gosu postgres pg_basebackup \
        -h postgres \
        -p 5432 \
        -U repl_user \
        -D "$PGDATA" \
        -R \
        -X stream \
        -P \
        --checkpoint=fast
fi

# Проверяем, что каталог подготовлен именно для standby.
if [ ! -f "$PGDATA/standby.signal" ]; then
    echo "Error: standby.signal is missing." >&2
    exit 1
fi

# Запускаем PostgreSQL с возможностью чтения на standby.
exec gosu postgres postgres \
    -c hba_file=/etc/postgresql/pg_hba.conf \
    -c hot_standby=on