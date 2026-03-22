from __future__ import annotations

import importlib
from typing import Any

import config
from connectors.base import BaseConnector
from constants import DBMSType


class CouchConnector(BaseConnector):
    def __init__(self, host: str, port: int, user: str, password: str) -> None:
        super().__init__(dbms_type=DBMSType.CouchDB, name=DBMSType.CouchDB.name)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database_name = config.COUCHDB_DATABASE
        self.database = None
        self._couchdb_module: Any = None

    def connect(self) -> None:
        self._couchdb_module = importlib.import_module("couchdb")
        uri = f"http://{self.user}:{self.password}@{self.host}:{self.port}/"
        self.client = self._couchdb_module.Server(uri)
        self.database = self.client[self.database_name]

    def close(self) -> None:
        self.client = None
        self.database = None

    def restore_data(self, size_label: str) -> None:
        print(f"DEBUG: Restoring {self.dbms_type.name} to size {size_label} using presets...")

