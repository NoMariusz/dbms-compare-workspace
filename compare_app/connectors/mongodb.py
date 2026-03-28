from __future__ import annotations

import importlib
import subprocess
from pathlib import Path
from typing import Any

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
        self._pymongo = None

    def connect(self) -> None:
        self._pymongo = importlib.import_module("pymongo")
        uri = f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/?authSource=admin"
        print(
            f"DEBUG: Connecting to {self.dbms_type.name} "
            f"at {self.host}:{self.port} to database {self.database_name}"
        )
        self.client = self._pymongo.MongoClient(uri)
        self.client.admin.command("ping")
        self.database = self.client[self.database_name]

    def close(self) -> None:
        if self.client:
            self.client.close()
            self.client = None
        self.database = None

    def restore_data(self, size_label: str) -> None:
        print(f"DEBUG: Restoring {self.dbms_type.name} to size {size_label} using presets...")

        backup_path = self._resolve_backup_path(size_label)
        if not backup_path.exists():
            print(
                f"WARNING: Backup file not found for {self.dbms_type.name} "
                f"and size {size_label}: {backup_path}"
            )
            return

        if self.client:
            self.close()

        container_name = self._container_name()
        container_backup_path = f"/tmp/{backup_path.name}"

        subprocess.run(
            ["docker", "cp", str(backup_path), f"{container_name}:{container_backup_path}"],
            check=True,
            capture_output=True,
            text=True,
        )

        restore_cmd = (
            "mongorestore "
            f"--username {self.user} "
            f"--password {self.password} "
            "--authenticationDatabase admin "
            "--drop "
            f"--nsInclude {self.database_name}.* "
            f"--archive={container_backup_path}"
        )

        subprocess.run(
            ["docker", "exec", container_name, "sh", "-lc", restore_cmd],
            check=True,
            capture_output=True,
            text=True,
        )

        self.connect()

    def _container_name(self) -> str:
        return "mongodb_lts"

    def _resolve_backup_path(self, size_label: str) -> Path:
        return Path(f"./data/db_backups/mongodb_{size_label}.archive")

    def _get_collection(self, collection_name: str):
        if self.database is None:
            raise RuntimeError("MongoDB connection is not initialized. Call connect() first.")
        return self.database[collection_name]

    def insert_one_ignore_duplicates(self, collection_name: str, document: dict[str, Any]) -> bool:
        if self._pymongo is None:
            raise RuntimeError("PyMongo module is not initialized. Call connect() first.")

        try:
            self._get_collection(collection_name).insert_one(dict(document))
            return True
        except self._pymongo.errors.DuplicateKeyError:
            return False

    def insert_one(self, collection_name: str, document: dict[str, Any]) -> Any:
        result = self._get_collection(collection_name).insert_one(dict(document))
        return result.inserted_id

    def read_one(
        self,
        collection_name: str,
        filter_query: dict[str, Any],
        projection: dict[str, int] | None = None,
        sort: list[tuple[str, int]] | None = None,
    ) -> dict[str, Any] | None:
        document = self._get_collection(collection_name).find_one(
            filter=filter_query,
            projection=projection,
            sort=sort,
        )
        if document is None:
            return None

        result = dict(document)
        result.pop("_id", None)
        return result

    def update_many(
        self,
        collection_name: str,
        filter_query: dict[str, Any],
        update_query: dict[str, Any],
        upsert: bool = False,
    ) -> int:
        result = self._get_collection(collection_name).update_many(
            filter_query,
            update_query,
            upsert=upsert,
        )
        return int(result.modified_count)

    def delete_many(self, collection_name: str, filter_query: dict[str, Any]) -> int:
        result = self._get_collection(collection_name).delete_many(filter_query)
        return int(result.deleted_count)