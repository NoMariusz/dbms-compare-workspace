from __future__ import annotations

import argparse
import os
import random
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pymongo
from bson import UuidRepresentation
from bson.codec_options import CodecOptions
from pymongo.encryption import Algorithm, ClientEncryption


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


def _connect():
    uri = _build_mongo_uri()
    client = pymongo.MongoClient(uri)
    client.admin.command("ping")
    return client


def _build_mongo_uri() -> str:
    query_params = ["authSource=admin"]
    replica_set = _env("MONGO_REPLICA_SET", "rs0").strip()
    if replica_set:
        query_params.append(f"replicaSet={replica_set}")

    direct_connection = _env("MONGO_DIRECT_CONNECTION", "true").strip().lower()
    if direct_connection in {"1", "true", "yes", "on"}:
        query_params.append("directConnection=true")

    return (
        f"mongodb://{_env('MONGO_USERNAME', 'admin')}:"
        f"{_env('MONGO_PASSWORD', 'password123')}@"
        f"{_env('MONGO_HOST', 'localhost')}:"
        f"{_env('MONGO_PORT', '27017')}/?{'&'.join(query_params)}"
    )


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


def _chunks(total: int, batch_size: int):
    start = 0
    while start < total:
        end = min(start + batch_size, total)
        yield start, end
        start = end


def _ensure_collections(db) -> None:
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
    existing = set(db.list_collection_names())
    for collection_name in required:
        if collection_name not in existing:
            db.create_collection(collection_name)


def _is_encrypted_field(db, collection_name: str, field_path: str) -> bool:
    collection_info = next(
        db.list_collections(filter={"name": collection_name}),
        None,
    )
    if collection_info is None:
        return False

    encrypted_fields = collection_info.get("options", {}).get("encryptedFields", {})
    fields = encrypted_fields.get("fields", [])
    return any(field.get("path") == field_path for field in fields)


def _ensure_indexes(db) -> None:
    asc = pymongo.ASCENDING
    desc = pymongo.DESCENDING

    db.user_roles.create_index([("id_role", asc)], unique=True)
    db.users.create_index([("id_user", asc)], unique=True)
    db.manufacturers.create_index([("id_manufacturer", asc)], unique=True)
    db.product_types.create_index([("id_type", asc)], unique=True)
    db.models.create_index([("id_model", asc)], unique=True)
    db.gear_specifications.create_index([("id_specification", asc)], unique=True)
    db.product.create_index([("id_product", asc)], unique=True)
    db.order_status.create_index([("id_status", asc)], unique=True)
    db.orders.create_index([("id_order", asc)], unique=True)
    db.order_items.create_index([("id_order_item", asc)], unique=True)

    if _is_encrypted_field(db, "users", "email"):
        print("Skipping index users.email because field is encrypted (Queryable Encryption).")
    else:
        db.users.create_index([("email", asc)], unique=True)
    db.users.create_index([("id_role", asc)])

    db.models.create_index([("id_manufacturer", asc)])
    db.models.create_index([("release_date", asc)])

    db.models_to_product_types.create_index([("id_model", asc), ("id_type", asc)], unique=True)
    db.models_to_product_types.create_index([("id_type", asc)])

    db.product.create_index([("id_model", asc)])
    db.product.create_index([("id_specification", asc)])
    db.product.create_index([("stock_quantity", asc)])
    db.product.create_index([("id_model", asc), ("color_name", asc)])
    db.product.create_index([("id_model", asc), ("color_name", asc), ("size_value", asc)])

    db.orders.create_index([("id_user", asc), ("order_date", desc)])
    db.orders.create_index([("id_status", asc), ("order_date", desc)])
    db.orders.create_index([("order_date", asc)])

    db.order_items.create_index([("id_order", asc)])
    db.order_items.create_index([("id_product", asc)])
    db.order_items.create_index([("id_order", asc), ("id_product", asc)])


def _is_without_indexes_database(db_name: str) -> bool:
    return db_name.endswith("_without_indexes")


def _has_non_default_indexes(db) -> bool:
    collections = [
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
    for collection_name in collections:
        if collection_name not in db.list_collection_names():
            continue
        index_names = db[collection_name].index_information().keys()
        if any(index_name != "_id_" for index_name in index_names):
            return True
    return False


def _ensure_reference_data(db) -> None:
    db.user_roles.replace_one(
        {"id_role": 1},
        {"id_role": 1, "role_name": "Admin"},
        upsert=True,
    )
    db.user_roles.replace_one(
        {"id_role": 2},
        {"id_role": 2, "role_name": "User"},
        upsert=True,
    )

    order_status_rows = [
        {"id_status": 1, "status_name": "Draft"},
        {"id_status": 2, "status_name": "Waiting for a payment"},
        {"id_status": 3, "status_name": "Shipment in progress"},
        {"id_status": 4, "status_name": "Completed"},
        {"id_status": 5, "status_name": "Cancelled"},
    ]
    for row in order_status_rows:
        db.order_status.replace_one({"id_status": row["id_status"]}, row, upsert=True)

    product_type_rows = [
        {"id_type": 1, "type_name": "Man"},
        {"id_type": 2, "type_name": "Woman"},
        {"id_type": 3, "type_name": "Unisex"},
        {"id_type": 4, "type_name": "In-line Skates"},
        {"id_type": 5, "type_name": "Accessories"},
        {"id_type": 6, "type_name": "Ice Skates"},
    ]
    for row in product_type_rows:
        db.product_types.replace_one({"id_type": row["id_type"]}, row, upsert=True)


def _truncate_generated_collections(db) -> None:
    db.models_to_product_types.delete_many({})
    db.order_items.delete_many({})
    db.orders.delete_many({})
    db.product.delete_many({})
    db.models.delete_many({})
    db.gear_specifications.delete_many({})
    db.manufacturers.delete_many({})
    db.users.delete_many({})


def _split_entity_sizes(size: int) -> dict[str, int]:
    if size <= 0:
        raise ValueError("size must be greater than zero")

    entities = [
        "manufacturers",
        "models",
        "mappings",
        "specs",
        "products",
        "users",
        "orders",
        "order_items",
    ]

    if size < len(entities):
        return {
            "manufacturers": 0,
            "models": 0,
            "mappings": 0,
            "specs": 0,
            "products": 0,
            "users": size,
            "orders": 0,
            "order_items": 0,
        }

    weights = {
        "manufacturers": 0.05,
        "models": 0.10,
        "mappings": 0.05,
        "specs": 0.10,
        "products": 0.20,
        "users": 0.20,
        "orders": 0.15,
        "order_items": 0.15,
    }

    result = {entity: 1 for entity in entities}
    remaining = size - len(entities)

    raw_allocations = {entity: remaining * weights[entity] for entity in entities}
    base_allocations = {entity: int(raw_allocations[entity]) for entity in entities}

    for entity in entities:
        result[entity] += base_allocations[entity]

    assigned = sum(base_allocations.values())
    leftovers = remaining - assigned
    remainders = sorted(
        entities,
        key=lambda entity: raw_allocations[entity] - base_allocations[entity],
        reverse=True,
    )

    for entity in remainders[:leftovers]:
        result[entity] += 1

    return result


def _insert_many(collection, documents: list[dict], batch_size: int) -> None:
    if not documents:
        return

    for start, end in _chunks(len(documents), batch_size):
        collection.insert_many(documents[start:end], ordered=False)


def _build_kms_providers() -> dict:
    master_key_path = Path(
        _env(
            "MONGO_QE_MASTER_KEY_PATH",
            str((Path(__file__).resolve().parents[1] / ".mongo_qe_master_key.bin")),
        )
    )
    local_master_key = _load_or_create_local_master_key(master_key_path)
    return {"local": {"key": local_master_key}}

def _build_client_encryption(client: pymongo.MongoClient) -> ClientEncryption:
    return ClientEncryption(
        kms_providers=_build_kms_providers(),
        key_vault_namespace=_env("MONGO_QE_KEY_VAULT_NAMESPACE", "encryption.__keyVault"),
        key_vault_client=client,
        codec_options=CodecOptions(
            uuid_representation=UuidRepresentation.STANDARD,
            tz_aware=False,
        ),
    )


def _get_key_id_from_alt_name(client: pymongo.MongoClient, key_alt_name: str):
    key_vault_namespace = _env("MONGO_QE_KEY_VAULT_NAMESPACE", "encryption.__keyVault")
    key_vault_db_name, key_vault_collection_name = key_vault_namespace.split(".", 1)
    key_vault_collection = client[key_vault_db_name][key_vault_collection_name]
    key_doc = key_vault_collection.find_one({"keyAltNames": key_alt_name}, {"_id": 1})
    if key_doc is None:
        raise ValueError(
            f"Data key with alt name '{key_alt_name}' was not found in key vault '{key_vault_namespace}'. "
            "Run setup_mongodb_queryable_encryption.py first."
        )
    return key_doc["_id"]


def _encrypt_unindexed_value(client_encryption: ClientEncryption, value, key_id):
    return client_encryption.encrypt(
        value,
        algorithm=Algorithm.UNINDEXED,
        key_id=key_id,
    )


def populate_database(size: int, batch_size: int, reset: bool, db_name: str | None = None) -> None:
    client = _connect()
    client_encryption: ClientEncryption | None = None
    try:
        selected_db_name = db_name or _env("MONGO_DATABASE", "skates_shop")
        db = client[selected_db_name]

        users_email_encrypted = _is_encrypted_field(db, "users", "email")
        users_password_encrypted = _is_encrypted_field(db, "users", "password")
        users_phone_encrypted = _is_encrypted_field(db, "users", "phone")
        orders_shipping_address_encrypted = _is_encrypted_field(db, "orders", "shipping_address")

        if users_email_encrypted or users_password_encrypted or users_phone_encrypted or orders_shipping_address_encrypted:
            client_encryption = _build_client_encryption(client)
            print(f"Using explicit encryption for encrypted fields in database: {selected_db_name}")

        email_key_id = None
        password_key_id = None
        phone_key_id = None
        shipping_address_key_id = None
        if client_encryption is not None:
            if users_email_encrypted:
                email_key_id = _get_key_id_from_alt_name(client, _env("MONGO_QE_EMAIL_KEY_ALT_NAME", "skates_shop_email_key"))
            if users_password_encrypted:
                password_key_id = _get_key_id_from_alt_name(client, _env("MONGO_QE_PASSWORD_KEY_ALT_NAME", "skates_shop_password_key"))
            if users_phone_encrypted:
                phone_key_id = _get_key_id_from_alt_name(client, _env("MONGO_QE_PHONE_KEY_ALT_NAME", "skates_shop_phone_key"))
            if orders_shipping_address_encrypted:
                shipping_address_key_id = _get_key_id_from_alt_name(client, _env("MONGO_QE_SHIPPING_ADDRESS_KEY_ALT_NAME", "skates_shop_shipping_address_key"))

        _ensure_collections(db)
        if _is_without_indexes_database(selected_db_name):
            print(f"Skipping index creation for non-indexed database: {selected_db_name}")
        else:
            _ensure_indexes(db)

        if _has_non_default_indexes(db):
            print(f"Database {selected_db_name} has secondary indexes")
        else:
            print(f"Database {selected_db_name} has no secondary indexes (only default _id index)")

        if reset:
            _truncate_generated_collections(db)

        _ensure_reference_data(db)

        sizes = _split_entity_sizes(size)
        run_tag = datetime.now().strftime("%Y%m%d%H%M%S")

        manufacturers_count = sizes["manufacturers"]
        models_count = sizes["models"]
        mappings_count = sizes["mappings"]
        specs_count = sizes["specs"]
        products_count = sizes["products"]
        users_count = sizes["users"]
        orders_count = sizes["orders"]
        order_items_count = sizes["order_items"]

        manufacturer_ids = list(range(1, manufacturers_count + 1))
        model_ids = list(range(1, models_count + 1))
        product_type_ids = [1, 2, 3, 4, 5, 6]
        specification_ids = list(range(1, specs_count + 1))
        product_ids = list(range(1, products_count + 1))
        user_ids = list(range(1, users_count + 1))
        order_ids = list(range(1, orders_count + 1))

        manufacturer_docs: list[dict] = []
        for manufacturer_id in manufacturer_ids:
            manufacturer_docs.append(
                {
                    "id_manufacturer": manufacturer_id,
                    "name": f"Manufacturer {run_tag}-{manufacturer_id}",
                }
            )
        _insert_many(db.manufacturers, manufacturer_docs, batch_size)

        model_docs: list[dict] = []
        for model_id in model_ids:
            release_date = date.today() - timedelta(days=random.randint(0, 4000))
            model_docs.append(
                {
                    "id_model": model_id,
                    "id_manufacturer": random.choice(manufacturer_ids) if manufacturer_ids else None,
                    "model_name": f"Model {run_tag}-{model_id}",
                    "description": f"Generated model description {model_id}",
                    "release_date": datetime.combine(release_date, time.min),
                }
            )
        _insert_many(db.models, model_docs, batch_size)

        mapping_docs: list[dict] = []
        if model_ids and product_type_ids and mappings_count > 0:
            max_pairs = len(model_ids) * len(product_type_ids)
            target_pairs = min(mappings_count, max_pairs)
            seen_pairs: set[tuple[int, int]] = set()

            while len(mapping_docs) < target_pairs:
                pair = (random.choice(model_ids), random.choice(product_type_ids))
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                mapping_docs.append(
                    {
                        "id_model": pair[0],
                        "id_type": pair[1],
                    }
                )
        _insert_many(db.models_to_product_types, mapping_docs, batch_size)

        specification_docs: list[dict] = []
        for specification_id in specification_ids:
            specification_docs.append(
                {
                    "id_specification": specification_id,
                    "wheel_size": random.choice([72, 76, 80, 84, 90, 100]),
                    "number_of_wheels": random.choice([3, 4, 5]),
                    "blade_material": random.choice(["steel", "aluminium", "carbon"]),
                    "boot_material": random.choice(["plastic", "carbon", "composite"]),
                    "bearing_type": random.choice(["ABEC-5", "ABEC-7", "ILQ-9"]),
                }
            )
        _insert_many(db.gear_specifications, specification_docs, batch_size)

        product_docs: list[dict] = []
        for product_id in product_ids:
            product_docs.append(
                {
                    "id_product": product_id,
                    "id_model": random.choice(model_ids) if model_ids else None,
                    "id_specification": random.choice(specification_ids) if specification_ids else None,
                    "color_name": random.choice(["black", "white", "red", "blue", "green"]),
                    "size_value": random.choice(["36", "38", "40", "42", "44", "46"]),
                    "stock_quantity": random.randint(0, 250),
                    "price": round(random.uniform(149.0, 999.0), 2),
                    "description": f"Generated product {run_tag}-{product_id}",
                }
            )
        _insert_many(db.product, product_docs, batch_size)

        user_docs: list[dict] = []
        for user_id in user_ids:
            user_doc = {
                "id_user": user_id,
                "username": f"user_{run_tag}_{user_id}",
                "email": f"{run_tag}-u-{user_id}@bench.local",
                "password": "hashed_password",
                "phone": f"+48{random.randint(500000000, 999999999)}",
                "id_role": random.choice([1, 2]),
            }
            if client_encryption is not None:
                if users_email_encrypted:
                    user_doc["email"] = _encrypt_unindexed_value(client_encryption, user_doc["email"], email_key_id)
                if users_password_encrypted:
                    user_doc["password"] = _encrypt_unindexed_value(client_encryption, user_doc["password"], password_key_id)
                if users_phone_encrypted:
                    user_doc["phone"] = _encrypt_unindexed_value(client_encryption, user_doc["phone"], phone_key_id)
            user_docs.append(user_doc)
        _insert_many(db.users, user_docs, batch_size)

        order_docs: list[dict] = []
        for order_id in order_ids:
            order_doc = {
                "id_order": order_id,
                "id_user": random.choice(user_ids) if user_ids else None,
                "id_status": random.randint(1, 5),
                "order_date": datetime.utcnow() - timedelta(days=random.randint(0, 3650)),
                "total_amount": round(random.uniform(50.0, 2500.0), 2),
                "shipping_address": f"{run_tag}-addr-{order_id}",
            }
            if client_encryption is not None and orders_shipping_address_encrypted:
                order_doc["shipping_address"] = _encrypt_unindexed_value(
                    client_encryption,
                    order_doc["shipping_address"],
                    shipping_address_key_id,
                )
            order_docs.append(order_doc)
        _insert_many(db.orders, order_docs, batch_size)

        order_item_docs: list[dict] = []
        for order_item_id in range(1, order_items_count + 1):
            order_item_docs.append(
                {
                    "id_order_item": order_item_id,
                    "id_order": random.choice(order_ids) if order_ids else None,
                    "id_product": random.choice(product_ids) if product_ids else None,
                    "quantity": random.randint(1, 4),
                    "unit_price": round(random.uniform(40.0, 1000.0), 2),
                }
            )
        _insert_many(db.order_items, order_item_docs, batch_size)

    finally:
        if client_encryption is not None:
            client_encryption.close()
        client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate random MongoDB data for benchmark tests")
    parser.add_argument("--db", type=str, default=None, help="target MongoDB database name")
    parser.add_argument("--size", type=int, required=True, help="total generated row budget split across entities")
    parser.add_argument("--batch-size", type=int, default=1000, help="batch size for insert operations")
    parser.add_argument("--seed", type=int, default=None, help="optional random seed")
    parser.add_argument("--reset", action="store_true", help="delete generated collections before inserts")
    return parser.parse_args()


"""
Populate db:
py util_scripts\\generate_random_data_mongodb.py --db skates_shop --size 500000 --batch-size 1000 --reset

Populate non-indexed db:
py util_scripts\\generate_random_data_mongodb.py --db skates_shop_without_indexes --size 500000 --batch-size 1000 --reset

Create backup inside mongodb_lts container:
docker exec mongodb_lts sh -lc 'mongodump --uri "mongodb://admin:password123@localhost:27017/skates_shop?authSource=admin" --archive=/tmp/mongodb_500k.archive'

Copy file to host:
docker cp mongodb_lts:/tmp/mongodb_500k.archive ./data/db_backups/mongodb_500k.archive

Similarly:
--size 1000000 --size-label 1m --batch-size 1000 --reset / backup and copy for: mongodb_1m.archive
--size 10000000 --size-label 10m --batch-size 1000 --reset / backup and copy for: mongodb_10m.archive
"""


def main() -> None:
    args = parse_args()
    if args.size <= 0:
        raise ValueError("--size must be greater than zero")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be greater than zero")

    if args.seed is not None:
        random.seed(args.seed)

    env_path = Path(__file__).resolve().parents[1] / ".env"
    _load_env(env_path)

    populate_database(
        size=args.size,
        batch_size=args.batch_size,
        reset=args.reset,
        db_name=args.db,
    )
    target_db = args.db or _env("MONGO_DATABASE", "skates_shop")
    print(f"MongoDB data generation finished for db={target_db}, size={args.size}")


if __name__ == "__main__":
    main()