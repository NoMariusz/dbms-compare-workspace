from __future__ import annotations

import importlib
import subprocess
from pathlib import Path
from typing import Any

import config
from connectors.base import BaseConnector
from constants import DBMSType


class MongoConnector(BaseConnector):
    _ID_FIELD_BY_COLLECTION = {
        "user_roles": "id_role",
        "users": "id_user",
        "manufacturers": "id_manufacturer",
        "product_types": "id_type",
        "models": "id_model",
        "models_to_product_types": None,
        "gear_specifications": "id_specification",
        "product": "id_product",
        "order_status": "id_status",
        "orders": "id_order",
        "order_items": "id_order_item",
    }

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
        return Path(f"./data/db_backups/{self.database_name}/mongodb_{size_label}.archive")

    def _get_collection(self, collection_name: str):
        if self.database is None:
            raise RuntimeError("MongoDB connection is not initialized. Call connect() first.")
        return self.database[collection_name]

    def _normalize_document(self, document: dict[str, Any]) -> dict[str, Any]:
        result = dict(document)
        result.pop("_id", None)
        return result
        
    def _chunk_values(self, values: list[Any], chunk_size: int) -> list[list[Any]]:
        return [values[i:i + chunk_size] for i in range(0, len(values), chunk_size)]


    def read_many_in_batches(
        self,
        collection_name: str,
        field_name: str,
        values: list[Any],
        projection: dict[str, int] | None = None,
        sort: list[tuple[str, int]] | None = None,
        chunk_size: int = 5000,
    ) -> list[dict[str, Any]]:
        if not values:
            return []

        results: list[dict[str, Any]] = []
        for chunk in self._chunk_values(values, chunk_size):
            results.extend(
                self.read_many(
                    collection_name=collection_name,
                    filter_query={field_name: {"$in": chunk}},
                    projection=projection,
                    sort=sort,
                )
            )
        return results


    def delete_many_in_batches(
        self,
        collection_name: str,
        field_name: str,
        values: list[Any],
        chunk_size: int = 5000,
    ) -> int:
        if not values:
            return 0

        deleted_total = 0
        for chunk in self._chunk_values(values, chunk_size):
            deleted_total += self.delete_many(
                collection_name=collection_name,
                filter_query={field_name: {"$in": chunk}},
            )
        return deleted_total

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

    def insert_many(self, collection_name: str, documents: list[dict[str, Any]], ordered: bool = True) -> list[Any]:
        if not documents:
            return []
        result = self._get_collection(collection_name).insert_many(
            [dict(document) for document in documents],
            ordered=ordered,
        )
        return list(result.inserted_ids)

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
        return self._normalize_document(document)

    def read_many(
        self,
        collection_name: str,
        filter_query: dict[str, Any],
        projection: dict[str, int] | None = None,
        sort: list[tuple[str, int]] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        cursor = self._get_collection(collection_name).find(
            filter=filter_query,
            projection=projection,
        )
        if sort is not None:
            cursor = cursor.sort(sort)
        if limit is not None:
            cursor = cursor.limit(limit)

        return [self._normalize_document(document) for document in cursor]

    def read_latest(
        self,
        collection_name: str,
        id_field: str | None = None,
        projection: dict[str, int] | None = None,
    ) -> dict[str, Any] | None:
        if id_field is None:
            id_field = self._ID_FIELD_BY_COLLECTION.get(collection_name)
        if id_field is None:
            raise ValueError(f"No business id field configured for collection: {collection_name}")

        return self.read_one(
            collection_name=collection_name,
            filter_query={},
            projection=projection,
            sort=[(id_field, -1)],
        )

    def update_one(
        self,
        collection_name: str,
        filter_query: dict[str, Any],
        update_query: dict[str, Any],
        upsert: bool = False,
    ) -> int:
        result = self._get_collection(collection_name).update_one(
            filter_query,
            update_query,
            upsert=upsert,
        )
        return int(result.modified_count)

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

    def delete_one(self, collection_name: str, filter_query: dict[str, Any]) -> int:
        result = self._get_collection(collection_name).delete_one(filter_query)
        return int(result.deleted_count)

    def delete_many(self, collection_name: str, filter_query: dict[str, Any]) -> int:
        result = self._get_collection(collection_name).delete_many(filter_query)
        return int(result.deleted_count)

    def get_next_business_id(self, collection_name: str, id_field: str | None = None) -> int:
        if id_field is None:
            id_field = self._ID_FIELD_BY_COLLECTION.get(collection_name)
        if id_field is None:
            raise ValueError(f"No business id field configured for collection: {collection_name}")

        latest_document = self.read_one(
            collection_name=collection_name,
            filter_query={},
            projection={id_field: 1, "_id": 0},
            sort=[(id_field, -1)],
        )
        if latest_document is None:
            return 1

        current_value = latest_document.get(id_field)
        if not isinstance(current_value, int):
            raise ValueError(f"Field {id_field} in collection {collection_name} is not an integer.")

        return current_value + 1
