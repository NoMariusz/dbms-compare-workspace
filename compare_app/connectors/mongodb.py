from __future__ import annotations

import importlib

import config
from connectors.base import BaseConnector
from constants import DBMSType


class MongoConnector(BaseConnector):
    def __init__(self, host: str, port: int, user: str, password: str) -> None:
        super().__init__(dbms_type=DBMSType.MongoDB, name=DBMSType.MongoDB.name)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database_name = config.MONGO_DATABASE
        self.database = None

    def connect(self) -> None:
        pymongo = importlib.import_module("pymongo")
        uri = f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/?authSource=admin"
        self.client = pymongo.MongoClient(uri)
        self.database = self.client[self.database_name]

    def close(self) -> None:
        if self.client:
            self.client.close()
            self.client = None
        self.database = None

    def restore_data(self, size_label: str) -> None:
        print(f"DEBUG: Restoring {self.dbms_type.name} to size {size_label} using presets...")
