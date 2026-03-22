from __future__ import annotations

import importlib
import subprocess
from pathlib import Path
from typing import Any

import config
from connectors.base import BaseConnector
from constants import DBMSType


class PostgresConnector(BaseConnector):
    def __init__(self, dbms_type: DBMSType, host: str, port: int, user: str, password: str) -> None:
        super().__init__(dbms_type=dbms_type, name=dbms_type.name)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        if dbms_type == DBMSType.PostgreSQL_LTS:
            self.database = config.POSTGRES_LTS_DB
        else:
            self.database = config.POSTGRES_11_DB

    def connect(self) -> None:
        psycopg2 = importlib.import_module("psycopg2")
        print(f"DEBUG: Connecting to {self.dbms_type.name} at {self.host}:{self.port} to database {self.database}")
        self.client = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.database,
        )
        self.client.autocommit = True

    def close(self) -> None:
        if self.client:
            self.client.close()
            self.client = None
    
    # restoring db with given sizes, for simplicity, we assume the backup files are created with pg_dump and can be restored with pg_restore

    def restore_data(self, size_label: str) -> None:
        print(f"DEBUG: Restoring {self.dbms_type.name} to size {size_label} using presets...")
        backup_path = self._resolve_backup_path(size_label)
        if not backup_path.exists():
            print(f"WARNING: Backup file not found for {self.dbms_type.name} and size {size_label}: {backup_path}")
            return

        if self.client:
            self.close()

        container_name = self._container_name()
        container_backup_path = f"/tmp/{backup_path.name}"

        # copy backup file into postgres container
        subprocess.run(
            ["docker", "cp", str(backup_path), f"{container_name}:{container_backup_path}"],
            check=True,
            capture_output=True,
            text=True,
        )

        restore_cmd = (
            "pg_restore "
            f"-U {self.user} "
            f"-d {self.database} "
            "--clean --if-exists --no-owner --no-privileges "
            f"{container_backup_path}"
        )

        # run restore with matching pg tools in container
        subprocess.run(
            [
                "docker",
                "exec",
                "-e",
                f"PGPASSWORD={self.password}",
                container_name,
                "sh",
                "-lc",
                restore_cmd,
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        self.connect()

    def _container_name(self) -> str:
        if self.dbms_type == DBMSType.PostgreSQL_LTS:
            return "postgres_lts"
        return "postgres_11_22"

    def _resolve_backup_path(self, size_label: str) -> Path:
        db_prefix = "postgresql_lts" if self.dbms_type == DBMSType.PostgreSQL_LTS else "postgresql_11"
        return Path(f"./data/db_backups/{db_prefix}_{size_label}.backup")
    
    # CRUD helper methods for test cases

    def insert_row(self, query: str, params: tuple[Any, ...] | None = None) -> None:
        with self.client.cursor() as cursor:
            cursor.execute(query, params)

    def read_row(self, query: str, params: tuple[Any, ...] | None = None) -> dict[str, Any] | None:
        extras = importlib.import_module("psycopg2.extras")
        with self.client.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()
            return dict(row) if row else None

    def update_rows(self, query: str, params: tuple[Any, ...] | None = None) -> int:
        with self.client.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.rowcount

    def delete_rows(self, query: str, params: tuple[Any, ...] | None = None) -> int:
        with self.client.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.rowcount
