from __future__ import annotations

import argparse
import base64
import json
import os
import random
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from urllib import error, request

INDEX_DEFINITIONS = [
    {"name": "idx_user_roles_id_role", "fields": ["type", "id_role"]},
    {"name": "idx_users_id_user", "fields": ["type", "id_user"]},
    {"name": "idx_users_email", "fields": ["type", "email"]},
    {"name": "idx_users_id_role", "fields": ["type", "id_role"]},
    {"name": "idx_manufacturers_id_manufacturer", "fields": ["type", "id_manufacturer"]},
    {"name": "idx_product_types_id_type", "fields": ["type", "id_type"]},
    {"name": "idx_models_id_model", "fields": ["type", "id_model"]},
    {"name": "idx_models_id_manufacturer", "fields": ["type", "id_manufacturer"]},
    {"name": "idx_models_release_date", "fields": ["type", "release_date"]},
    {"name": "idx_models_to_product_types_id_model_id_type", "fields": ["type", "id_model", "id_type"]},
    {"name": "idx_models_to_product_types_id_type", "fields": ["type", "id_type"]},
    {"name": "idx_gear_specifications_id_specification", "fields": ["type", "id_specification"]},
    {"name": "idx_product_id_product", "fields": ["type", "id_product"]},
    {"name": "idx_product_id_model", "fields": ["type", "id_model"]},
    {"name": "idx_product_id_specification", "fields": ["type", "id_specification"]},
    {"name": "idx_product_stock_quantity", "fields": ["type", "stock_quantity"]},
    {"name": "idx_product_model_color", "fields": ["type", "id_model", "color_name"]},
    {"name": "idx_product_model_color_size", "fields": ["type", "id_model", "color_name", "size_value"]},
    {"name": "idx_order_status_id_status", "fields": ["type", "id_status"]},
    {"name": "idx_orders_id_order", "fields": ["type", "id_order"]},
    {"name": "idx_orders_id_user_order_date", "fields": ["type", "id_user", "order_date"]},
    {"name": "idx_orders_id_status_order_date", "fields": ["type", "id_status", "order_date"]},
    {"name": "idx_orders_order_date", "fields": ["type", "order_date"]},
    {"name": "idx_order_items_id_order_item", "fields": ["type", "id_order_item"]},
    {"name": "idx_order_items_id_order", "fields": ["type", "id_order"]},
    {"name": "idx_order_items_id_product", "fields": ["type", "id_product"]},
    {"name": "idx_order_items_id_order_id_product", "fields": ["type", "id_order", "id_product"]},
]


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


def _base_url() -> str:
    host = _env("COUCHDB_HOST", "localhost")
    port = _env("COUCHDB_PORT", "5984")
    return f"http://{host}:{port}"


def _auth_header() -> str:
    user = _env("COUCHDB_USER", "admin")
    password = _env("COUCHDB_PASSWORD", "password123")
    raw = f"{user}:{password}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def _request_json(method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
    url = f"{_base_url()}{path}"
    body = None
    headers = {
        "Authorization": _auth_header(),
        "Accept": "application/json",
    }

    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = request.Request(url=url, data=body, headers=headers, method=method)

    try:
        with request.urlopen(req) as response:
            raw = response.read()
            if not raw:
                return None
            return json.loads(raw.decode("utf-8"))
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"CouchDB request failed: {method} {path} -> {exc.code} {details}") from exc


def _ensure_database_exists(database_name: str) -> None:
    try:
        _request_json("PUT", f"/{database_name}")
    except RuntimeError as exc:
        if "412" not in str(exc) and "file_exists" not in str(exc):
            raise


def _create_index(database_name: str, name: str, fields: list[str]) -> None:
    _request_json(
        method="POST",
        path=f"/{database_name}/_index",
        payload={
            "index": {"fields": fields},
            "name": name,
            "type": "json",
        },
    )


def _ensure_indexes(database_name: str) -> None:
    for index_definition in INDEX_DEFINITIONS:
        _create_index(
            database_name=database_name,
            name=index_definition["name"],
            fields=index_definition["fields"],
        )


def _is_without_indexes_database(database_name: str) -> bool:
    return database_name.endswith("_without_indexes")


def _has_secondary_indexes(database_name: str) -> bool:
    result = _request_json(method="GET", path=f"/{database_name}/_index")
    indexes = result.get("indexes", [])
    for index_item in indexes:
        if index_item.get("type") != "special":
            return True
    return False


def _chunks(items: list[dict[str, Any]], chunk_size: int):
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def _bulk_docs(database_name: str, documents: list[dict[str, Any]], chunk_size: int) -> None:
    if not documents:
        return

    for chunk in _chunks(documents, chunk_size):
        _request_json(
            method="POST",
            path=f"/{database_name}/_bulk_docs",
            payload={"docs": chunk},
        )


def _find_docs(
    database_name: str,
    selector: dict[str, Any],
    fields: list[str] | None = None,
    limit: int = 1_000_000,
) -> list[dict[str, Any]]:
    payload: dict[str, Any] = {
        "selector": selector,
        "limit": limit,
    }
    if fields is not None:
        payload["fields"] = fields

    result = _request_json(
        method="POST",
        path=f"/{database_name}/_find",
        payload=payload,
    )
    return result.get("docs", [])


def _ensure_reference_data(database_name: str) -> None:
    reference_docs = [
        {"_id": "user_roles:1", "type": "user_roles", "id_role": 1, "role_name": "Admin"},
        {"_id": "user_roles:2", "type": "user_roles", "id_role": 2, "role_name": "User"},
        {"_id": "order_status:1", "type": "order_status", "id_status": 1, "status_name": "Draft"},
        {"_id": "order_status:2", "type": "order_status", "id_status": 2, "status_name": "Waiting for a payment"},
        {"_id": "order_status:3", "type": "order_status", "id_status": 3, "status_name": "Shipment in progress"},
        {"_id": "order_status:4", "type": "order_status", "id_status": 4, "status_name": "Completed"},
        {"_id": "order_status:5", "type": "order_status", "id_status": 5, "status_name": "Cancelled"},
        {"_id": "product_types:1", "type": "product_types", "id_type": 1, "type_name": "Man"},
        {"_id": "product_types:2", "type": "product_types", "id_type": 2, "type_name": "Woman"},
        {"_id": "product_types:3", "type": "product_types", "id_type": 3, "type_name": "Unisex"},
        {"_id": "product_types:4", "type": "product_types", "id_type": 4, "type_name": "In-line Skates"},
        {"_id": "product_types:5", "type": "product_types", "id_type": 5, "type_name": "Accessories"},
        {"_id": "product_types:6", "type": "product_types", "id_type": 6, "type_name": "Ice Skates"},
    ]
    _bulk_docs(database_name, reference_docs, chunk_size=200)


def _truncate_generated_documents(database_name: str) -> None:
    docs = _find_docs(
        database_name=database_name,
        selector={
            "type": {
                "$in": [
                    "models_to_product_types",
                    "order_items",
                    "orders",
                    "product",
                    "models",
                    "gear_specifications",
                    "manufacturers",
                    "users",
                ]
            }
        },
        fields=["_id", "_rev"],
    )

    if not docs:
        return

    deleted_docs = []
    for doc in docs:
        deleted_docs.append(
            {
                "_id": doc["_id"],
                "_rev": doc["_rev"],
                "_deleted": True,
            }
        )

    _bulk_docs(database_name, deleted_docs, chunk_size=500)


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


def _build_backup_payload(database_name: str) -> dict[str, Any]:
    all_docs_result = _request_json(
        method="POST",
        path=f"/{database_name}/_find",
        payload={
            "selector": {},
            "limit": 10_000_000,
        },
    )

    documents = all_docs_result.get("docs", [])
    existing_indexes_result = _request_json(method="GET", path=f"/{database_name}/_index")
    existing_indexes = []
    for index_item in existing_indexes_result.get("indexes", []):
        if index_item.get("type") == "special":
            continue
        definition = index_item.get("def", {}).get("fields", [])
        fields = [field for item in definition for field in item.keys()]
        existing_indexes.append(
            {
                "name": index_item.get("name"),
                "fields": fields,
            }
        )

    return {
        "documents": documents,
        "indexes": existing_indexes,
    }


def populate_database(size: int, batch_size: int, reset: bool, database_name: str | None = None) -> None:
    selected_database_name = database_name or _env("COUCHDB_DB", "skates_shop")

    _ensure_database_exists(selected_database_name)
    if _is_without_indexes_database(selected_database_name):
        print(f"Skipping index creation for non-indexed database: {selected_database_name}")
    else:
        _ensure_indexes(selected_database_name)

    if _has_secondary_indexes(selected_database_name):
        print(f"Database {selected_database_name} has secondary indexes")
    else:
        print(f"Database {selected_database_name} has no secondary indexes")

    if reset:
        _truncate_generated_documents(selected_database_name)

    _ensure_reference_data(selected_database_name)

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

    manufacturer_docs: list[dict[str, Any]] = []
    for manufacturer_id in manufacturer_ids:
        manufacturer_docs.append(
            {
                "_id": f"manufacturers:{manufacturer_id}",
                "type": "manufacturers",
                "id_manufacturer": manufacturer_id,
                "name": f"Manufacturer {run_tag}-{manufacturer_id}",
            }
        )
    _bulk_docs(selected_database_name, manufacturer_docs, chunk_size=batch_size)

    model_docs: list[dict[str, Any]] = []
    for model_id in model_ids:
        release_date = date.today() - timedelta(days=random.randint(0, 4000))
        model_docs.append(
            {
                "_id": f"models:{model_id}",
                "type": "models",
                "id_model": model_id,
                "id_manufacturer": random.choice(manufacturer_ids) if manufacturer_ids else None,
                "model_name": f"Model {run_tag}-{model_id}",
                "description": f"Generated model description {model_id}",
                "release_date": datetime.combine(release_date, time.min).isoformat(),
            }
        )
    _bulk_docs(selected_database_name, model_docs, chunk_size=batch_size)

    mapping_docs: list[dict[str, Any]] = []
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
                    "_id": f"models_to_product_types:{pair[0]}:{pair[1]}",
                    "type": "models_to_product_types",
                    "id_model": pair[0],
                    "id_type": pair[1],
                }
            )
    _bulk_docs(selected_database_name, mapping_docs, chunk_size=batch_size)

    specification_docs: list[dict[str, Any]] = []
    for specification_id in specification_ids:
        specification_docs.append(
            {
                "_id": f"gear_specifications:{specification_id}",
                "type": "gear_specifications",
                "id_specification": specification_id,
                "wheel_size": random.choice([72, 76, 80, 84, 90, 100]),
                "number_of_wheels": random.choice([3, 4, 5]),
                "blade_material": random.choice(["steel", "aluminium", "carbon"]),
                "boot_material": random.choice(["plastic", "carbon", "composite"]),
                "bearing_type": random.choice(["ABEC-5", "ABEC-7", "ILQ-9"]),
            }
        )
    _bulk_docs(selected_database_name, specification_docs, chunk_size=batch_size)

    product_docs: list[dict[str, Any]] = []
    for product_id in product_ids:
        product_docs.append(
            {
                "_id": f"product:{product_id}",
                "type": "product",
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
    _bulk_docs(selected_database_name, product_docs, chunk_size=batch_size)

    user_docs: list[dict[str, Any]] = []
    for user_id in user_ids:
        user_docs.append(
            {
                "_id": f"users:{user_id}",
                "type": "users",
                "id_user": user_id,
                "username": f"user_{run_tag}_{user_id}",
                "email": f"{run_tag}-u-{user_id}@bench.local",
                "password": "hashed_password",
                "phone": f"+48{random.randint(500000000, 999999999)}",
                "id_role": random.choice([1, 2]),
            }
        )
    _bulk_docs(selected_database_name, user_docs, chunk_size=batch_size)

    order_docs: list[dict[str, Any]] = []
    for order_id in order_ids:
        order_docs.append(
            {
                "_id": f"orders:{order_id}",
                "type": "orders",
                "id_order": order_id,
                "id_user": random.choice(user_ids) if user_ids else None,
                "id_status": random.randint(1, 5),
                "order_date": (datetime.utcnow() - timedelta(days=random.randint(0, 3650))).isoformat(),
                "total_amount": round(random.uniform(50.0, 2500.0), 2),
                "shipping_address": f"{run_tag}-addr-{order_id}",
            }
        )
    _bulk_docs(selected_database_name, order_docs, chunk_size=batch_size)

    order_item_docs: list[dict[str, Any]] = []
    for order_item_id in range(1, order_items_count + 1):
        order_item_docs.append(
            {
                "_id": f"order_items:{order_item_id}",
                "type": "order_items",
                "id_order_item": order_item_id,
                "id_order": random.choice(order_ids) if order_ids else None,
                "id_product": random.choice(product_ids) if product_ids else None,
                "quantity": random.randint(1, 4),
                "unit_price": round(random.uniform(40.0, 1000.0), 2),
            }
        )
    _bulk_docs(selected_database_name, order_item_docs, chunk_size=batch_size)


def export_backup(size_label: str, output_dir: Path, database_name: str | None = None) -> Path:
    selected_database_name = database_name or _env("COUCHDB_DB", "skates_shop")
    output_dir.mkdir(parents=True, exist_ok=True)
    backup_path = output_dir / f"couchdb_{size_label}.json"

    payload = _build_backup_payload(selected_database_name)
    with backup_path.open("w", encoding="utf-8") as backup_file:
        json.dump(payload, backup_file, ensure_ascii=False)

    return backup_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate random CouchDB data for benchmark tests")
    parser.add_argument("--db", type=str, default=None, help="target CouchDB database name")
    parser.add_argument("--size", type=int, required=True, help="total generated row budget split across entities")
    parser.add_argument("--size-label", type=str, required=True, help="dataset label, e.g. 500k / 1m / 10m")
    parser.add_argument("--batch-size", type=int, default=1000, help="batch size for bulk document operations")
    parser.add_argument("--seed", type=int, default=None, help="optional random seed")
    parser.add_argument("--reset", action="store_true", help="delete generated documents before inserts")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./data/db_backups",
        help="directory for generated CouchDB backup JSON files",
    )
    return parser.parse_args()


"""
Populate db and export backup:
py util_scripts/generate_random_data_couchdb.py --db skates_shop --size 500000 --size-label 500k --batch-size 1000 --reset

Populate non-indexed db and export backup:
py util_scripts/generate_random_data_couchdb.py --db skates_shop_without_indexes --size 500000 --size-label 500k --batch-size 1000 --reset

Similarly for:
--size 1000000 --size-label 1m
--size 10000000 --size-label 10m

It creates:
./data/db_backups/couchdb_skates_shop_500k.json
./data/db_backups/couchdb_skates_shop_1m.json
./data/db_backups/couchdb_skates_shop_10m.json
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
        database_name=args.db,
    )
    backup_path = export_backup(
        size_label=args.size_label,
        output_dir=Path(args.output_dir),
        database_name=args.db,
    )
    target_db = args.db or _env("COUCHDB_DB", "skates_shop")
    print(f"CouchDB data generation finished for db={target_db}, size={args.size}")
    print(f"Backup exported to: {backup_path}")


if __name__ == "__main__":
    main()