#!/bin/bash
set -e

# # 1. Create the needed databases (main_db is created by default, so we only need to create indexed_db)
# psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
#     CREATE DATABASE indexed_db;
# EOSQL

# 2. Import simple_db_structure into the default 'main_db'
# Note the updated path: /opt/sql_dumps/...
echo "Importing simple_db_structure.sql into $POSTGRES_DB..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /opt/sql_dumps/simple_db_structure.sql

# 3. Create db indexed_db
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE indexed_db;
EOSQL

# 4. Import indexed_db_structure into the new 'indexed_db'
echo "Importing indexed_db_structure.sql into indexed_db..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "indexed_db" -f /opt/sql_dumps/indexed_db_structure.sql