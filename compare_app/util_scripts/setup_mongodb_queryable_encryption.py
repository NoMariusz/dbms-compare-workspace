from __future__ import annotations

import argparse
import os
from pathlib import Path

import pymongo
from bson import UuidRepresentation
from bson.binary import Binary
from bson.codec_options import CodecOptions
from pymongo.encryption import ClientEncryption


def _load_env(path: Path) -> None:
    if not path.exists():
        return

    with path.open("r", encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _connect_admin() -> pymongo.MongoClient:
    query_params = ["authSource=admin"]
    replica_set = _env("MONGO_REPLICA_SET", "rs0").strip()
    if replica_set:
        query_params.append(f"replicaSet={replica_set}")

    direct_connection = _env("MONGO_DIRECT_CONNECTION", "true").strip().lower()
    if direct_connection in {"1", "true", "yes", "on"}:
        query_params.append("directConnection=true")

    uri = (
        f"mongodb://{_env('MONGO_USERNAME', 'admin')}:"
        f"{_env('MONGO_PASSWORD', 'password123')}@"
        f"{_env('MONGO_HOST', 'localhost')}:"
        f"{_env('MONGO_PORT', '27017')}/?{'&'.join(query_params)}"
    )
    client = pymongo.MongoClient(uri)
    client.admin.command("ping")
    return client


def _load_or_create_local_master_key(path: Path) -> bytes:
    if path.exists():
        data = path.read_bytes()
        if len(data) != 96:
            raise ValueError(f"Invalid master key length in {path}. Expected 96 bytes, got {len(data)}")
        return data

    path.parent.mkdir(parents=True, exist_ok=True)
    master_key = os.urandom(96)
    path.write_bytes(master_key)
    return master_key


def _ensure_base_collections(target_db) -> None:
    required = [
        "user_roles",
        "users",
        "manufacturers",
        "product_types",
        "models",
        "models_to_product_types",
        "gear_specifications",
        "product",
        "order_status",
        "orders",
        "order_items",
    ]
    existing = set(target_db.list_collection_names())
    for collection_name in required:
        if collection_name not in existing:
            target_db.create_collection(collection_name)


def _ensure_key_vault_index(key_vault_collection) -> None:
    key_vault_collection.create_index(
        "keyAltNames",
        unique=True,
        partialFilterExpression={"keyAltNames": {"$exists": True}},
    )


def _get_or_create_data_key(client_encryption: ClientEncryption, key_vault_collection, key_alt_name: str) -> Binary:
    existing = key_vault_collection.find_one({"keyAltNames": key_alt_name}, {"_id": 1})
    if existing is not None:
        return existing["_id"]
    return client_encryption.create_data_key("local", key_alt_names=[key_alt_name])


def _create_or_replace_encrypted_collection(
    client_encryption: ClientEncryption,
    target_db,
    collection_name: str,
    encrypted_fields: dict,
    recreate: bool,
) -> None:
    exists = collection_name in target_db.list_collection_names()
    if exists and not recreate:
        collection_info = next(
            target_db.list_collections(filter={"name": collection_name}),
            None,
        )
        options = collection_info.get("options", {}) if collection_info else {}
        if options.get("encryptedFields") is not None:
            print(f"Collection {collection_name} already has Queryable Encryption in {target_db.name}; skipping.")
            return

        document_count = target_db[collection_name].estimated_document_count()
        if document_count > 0:
            raise RuntimeError(
                f"Collection {target_db.name}.{collection_name} has {document_count} documents and is not encrypted. "
                "Refusing automatic recreation. Use --recreate-collections only when data loss is acceptable."
            )

        print(
            f"Collection {target_db.name}.{collection_name} exists without Queryable Encryption and is empty; "
            "recreating it automatically."
        )
        recreate = True

    if exists and recreate:
        target_db.drop_collection(collection_name)

    client_encryption.create_encrypted_collection(
        database=target_db,
        name=collection_name,
        encrypted_fields=encrypted_fields,
    )
    print(f"Queryable Encryption configured for collection {target_db.name}.{collection_name}")


def setup_queryable_encryption(database_name: str, recreate_collections: bool) -> None:
    client = _connect_admin()
    try:
        target_db = client[database_name]
        _ensure_base_collections(target_db)

        key_vault_namespace = _env("MONGO_QE_KEY_VAULT_NAMESPACE", "encryption.__keyVault")
        key_vault_db_name, key_vault_collection_name = key_vault_namespace.split(".", 1)
        key_vault_collection = client[key_vault_db_name][key_vault_collection_name]
        _ensure_key_vault_index(key_vault_collection)

        master_key_path = Path(
            _env(
                "MONGO_QE_MASTER_KEY_PATH",
                str((Path(__file__).resolve().parents[1] / ".mongo_qe_master_key.bin")),
            )
        )
        local_master_key = _load_or_create_local_master_key(master_key_path)
        kms_providers = {"local": {"key": local_master_key}}

        codec_options = CodecOptions(
            uuid_representation=UuidRepresentation.STANDARD,
            tz_aware=False,
        )
        client_encryption = ClientEncryption(
            kms_providers=kms_providers,
            key_vault_namespace=key_vault_namespace,
            key_vault_client=client,
            codec_options=codec_options,
        )
        try:
            # Create separate data keys for each encrypted field to avoid "Duplicate key ids" error
            email_key_id = _get_or_create_data_key(
                client_encryption=client_encryption,
                key_vault_collection=key_vault_collection,
                key_alt_name=_env("MONGO_QE_EMAIL_KEY_ALT_NAME", "skates_shop_email_key"),
            )
            password_key_id = _get_or_create_data_key(
                client_encryption=client_encryption,
                key_vault_collection=key_vault_collection,
                key_alt_name=_env("MONGO_QE_PASSWORD_KEY_ALT_NAME", "skates_shop_password_key"),
            )
            phone_key_id = _get_or_create_data_key(
                client_encryption=client_encryption,
                key_vault_collection=key_vault_collection,
                key_alt_name=_env("MONGO_QE_PHONE_KEY_ALT_NAME", "skates_shop_phone_key"),
            )
            shipping_address_key_id = _get_or_create_data_key(
                client_encryption=client_encryption,
                key_vault_collection=key_vault_collection,
                key_alt_name=_env("MONGO_QE_SHIPPING_ADDRESS_KEY_ALT_NAME", "skates_shop_shipping_address_key"),
            )

            users_encrypted_fields = {
                "fields": [
                    {
                        "path": "email",
                        "bsonType": "string",
                        "keyId": email_key_id,
                        "queries": {"queryType": "equality"},
                    },
                    {
                        "path": "password",
                        "bsonType": "string",
                        "keyId": password_key_id,
                        "queries": {"queryType": "equality"},
                    },
                    {
                        "path": "phone",
                        "bsonType": "string",
                        "keyId": phone_key_id,
                        "queries": {"queryType": "equality"},
                    },
                ]
            }
            orders_encrypted_fields = {
                "fields": [
                    {
                        "path": "shipping_address",
                        "bsonType": "string",
                        "keyId": shipping_address_key_id,
                        "queries": {"queryType": "equality"},
                    }
                ]
            }

            print(f"Creating encrypted collection 'users' with fields: {[f['path'] for f in users_encrypted_fields['fields']]}")
            _create_or_replace_encrypted_collection(
                client_encryption=client_encryption,
                target_db=target_db,
                collection_name="users",
                encrypted_fields=users_encrypted_fields,
                recreate=recreate_collections,
            )
            print(f"Creating encrypted collection 'orders' with fields: {[f['path'] for f in orders_encrypted_fields['fields']]}")
            _create_or_replace_encrypted_collection(
                client_encryption=client_encryption,
                target_db=target_db,
                collection_name="orders",
                encrypted_fields=orders_encrypted_fields,
                recreate=recreate_collections,
            )

            print(f"Queryable Encryption setup finished for database: {database_name}")
            print(f"Local master key path: {master_key_path}")
        finally:
            client_encryption.close()
    finally:
        client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Setup MongoDB Queryable Encryption for benchmark database")
    parser.add_argument("--db", type=str, default="skates_shop_encrypted", help="target encrypted MongoDB database")
    parser.add_argument(
        "--recreate-collections",
        action="store_true",
        help="drop and recreate encrypted collections (users, orders)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    env_path = Path(__file__).resolve().parents[1] / ".env"
    _load_env(env_path)

    setup_queryable_encryption(
        database_name=args.db,
        recreate_collections=args.recreate_collections,
    )


if __name__ == "__main__":
    main()
