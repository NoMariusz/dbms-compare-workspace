from __future__ import annotations

import importlib
import os
import subprocess
from pathlib import Path
from typing import Any

import config
from bson import UuidRepresentation
from bson.codec_options import CodecOptions
from connectors.base import BaseConnector
from constants import DBMSType
from pymongo.encryption import Algorithm, ClientEncryption


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
        self.admin_user = user
        self.admin_password = password
        self.user = user
        self.password = password
        self.database_name = config.MONGO_DATABASE
        self.auth_source = "admin"
        if self.database_name == "skates_shop_roles":
            self.user = os.getenv("MONGO_ROLES_DB_USER", "moderator_user")
            self.password = os.getenv("MONGO_ROLES_DB_PASSWORD", "moderator_password123")
            self.auth_source = self.database_name
        self.database = None
        self._pymongo = None
        self._client_encryption: ClientEncryption | None = None
        self._encrypted_fields_by_collection: dict[str, dict[str, Any]] = {}

    def connect(self) -> None:
        self._pymongo = importlib.import_module("pymongo")
        query_params = [f"authSource={self.auth_source}"]
        replica_set = os.getenv("MONGO_REPLICA_SET", "rs0").strip()
        if replica_set:
            query_params.append(f"replicaSet={replica_set}")

        direct_connection = os.getenv("MONGO_DIRECT_CONNECTION", "true").strip().lower()
        if direct_connection in {"1", "true", "yes", "on"}:
            query_params.append("directConnection=true")

        uri = (
            f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/"
            f"?{'&'.join(query_params)}"
        )
        print(
            f"DEBUG: Connecting to {self.dbms_type.name} "
            f"at {self.host}:{self.port} to database {self.database_name}"
        )
        self.client = self._pymongo.MongoClient(uri)
        self.client.admin.command("ping")
        self.database = self.client[self.database_name]
        self._init_field_encryption()

    def close(self) -> None:
        if self._client_encryption is not None:
            self._client_encryption.close()
            self._client_encryption = None
        if self.client:
            self.client.close()
            self.client = None
        self.database = None
        self._encrypted_fields_by_collection = {}

    def _load_or_create_local_master_key(self) -> bytes:
        key_path = Path(
            os.getenv(
                "MONGO_QE_MASTER_KEY_PATH",
                str((Path(__file__).resolve().parents[1] / ".mongo_qe_master_key.bin")),
            )
        )
        if key_path.exists():
            key_data = key_path.read_bytes()
            if len(key_data) != 96:
                raise ValueError(
                    f"Invalid master key length in {key_path}. Expected 96 bytes, got {len(key_data)}"
                )
            return key_data

        key_path.parent.mkdir(parents=True, exist_ok=True)
        key_data = os.urandom(96)
        key_path.write_bytes(key_data)
        return key_data

    def _init_field_encryption(self) -> None:
        if self.client is None or self.database is None:
            return

        self._encrypted_fields_by_collection = {}
        for collection_name in ("users", "orders"):
            collection_info = next(
                self.database.list_collections(filter={"name": collection_name}),
                None,
            )
            if collection_info is None:
                continue
            encrypted_fields = collection_info.get("options", {}).get("encryptedFields")
            if encrypted_fields is None:
                continue
            fields = encrypted_fields.get("fields", [])
            self._encrypted_fields_by_collection[collection_name] = {
                field["path"]: field["keyId"]
                for field in fields
                if isinstance(field, dict) and "path" in field and "keyId" in field
            }

        if not self._encrypted_fields_by_collection:
            return

        kms_providers = {"local": {"key": self._load_or_create_local_master_key()}}
        key_vault_namespace = os.getenv("MONGO_QE_KEY_VAULT_NAMESPACE", "encryption.__keyVault")
        codec_options = CodecOptions(
            uuid_representation=UuidRepresentation.STANDARD,
            tz_aware=False,
        )
        self._client_encryption = ClientEncryption(
            kms_providers=kms_providers,
            key_vault_namespace=key_vault_namespace,
            key_vault_client=self.client,
            codec_options=codec_options,
        )

    def _encrypt_write_value(self, collection_name: str, field_name: str, value: Any) -> Any:
        if self._client_encryption is None:
            return value
        key_map = self._encrypted_fields_by_collection.get(collection_name)
        if not key_map or field_name not in key_map:
            return value
        if value is None:
            return value
        return self._client_encryption.encrypt(
            value,
            algorithm=Algorithm.UNINDEXED,
            key_id=key_map[field_name],
        )

    def _encrypt_document_for_insert(self, collection_name: str, document: dict[str, Any]) -> dict[str, Any]:
        encrypted_document = dict(document)
        for field_name in list(encrypted_document.keys()):
            encrypted_document[field_name] = self._encrypt_write_value(
                collection_name=collection_name,
                field_name=field_name,
                value=encrypted_document[field_name],
            )
        return encrypted_document

    def _encrypt_update_query(self, collection_name: str, update_query: dict[str, Any]) -> dict[str, Any]:
        encrypted_update = dict(update_query)
        for operator in ("$set", "$setOnInsert"):
            payload = encrypted_update.get(operator)
            if not isinstance(payload, dict):
                continue
            updated_payload = dict(payload)
            for field_name, value in updated_payload.items():
                updated_payload[field_name] = self._encrypt_write_value(
                    collection_name=collection_name,
                    field_name=field_name,
                    value=value,
                )
            encrypted_update[operator] = updated_payload
        return encrypted_update

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
            f"--username {self.admin_user} "
            f"--password {self.admin_password} "
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
            self._get_collection(collection_name).insert_one(
                self._encrypt_document_for_insert(collection_name, dict(document))
            )
            return True
        except self._pymongo.errors.DuplicateKeyError:
            return False

    def insert_one(self, collection_name: str, document: dict[str, Any]) -> Any:
        result = self._get_collection(collection_name).insert_one(
            self._encrypt_document_for_insert(collection_name, dict(document))
        )
        return result.inserted_id

    def insert_many(self, collection_name: str, documents: list[dict[str, Any]], ordered: bool = True) -> list[Any]:
        if not documents:
            return []
        result = self._get_collection(collection_name).insert_many(
            [self._encrypt_document_for_insert(collection_name, dict(document)) for document in documents],
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
            self._encrypt_update_query(collection_name, update_query),
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
            self._encrypt_update_query(collection_name, update_query),
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
