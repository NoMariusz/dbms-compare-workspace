from __future__ import annotations

import importlib
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
        print(f"DEBUG: Connecting to {self.dbms_type.name} at {self.host}:{self.port} as {self.user} with password {self.password} and database {self.database}")
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

    def restore_data(self, size_label: str) -> None:
        print(f"DEBUG: Restoring {self.dbms_type.name} to size {size_label} using presets...")

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
