#!/bin/sh
set -eu

COUCH_URL="http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@couchdb:5984"
DB="${COUCHDB_DB}"

until curl -fsS "${COUCH_URL}/" >/dev/null; do
  sleep 2
done

# Create business database if it does not exist
curl -fsS -X PUT "${COUCH_URL}/${DB}" >/dev/null 2>&1 || true

create_index() {
  NAME="$1"
  FIELDS="$2"

  curl -fsS -X POST "${COUCH_URL}/${DB}/_index" \
    -H "Content-Type: application/json" \
    -d "{\"index\":{\"fields\":${FIELDS}},\"name\":\"${NAME}\",\"type\":\"json\"}" \
    >/dev/null
}

# --- ids / basic lookups ---
create_index "idx_user_roles_id_role" '["type","id_role"]'
create_index "idx_users_id_user" '["type","id_user"]'
create_index "idx_manufacturers_id_manufacturer" '["type","id_manufacturer"]'
create_index "idx_product_types_id_type" '["type","id_type"]'
create_index "idx_models_id_model" '["type","id_model"]'
create_index "idx_models_to_product_types_model_type" '["type","id_model","id_type"]'
create_index "idx_gear_specifications_id_specification" '["type","id_specification"]'
create_index "idx_product_id_product" '["type","id_product"]'
create_index "idx_order_status_id_status" '["type","id_status"]'
create_index "idx_orders_id_order" '["type","id_order"]'
create_index "idx_order_items_id_order_item" '["type","id_order_item"]'

# --- CRUD scenario indexes ---
create_index "idx_users_email" '["type","email"]'
create_index "idx_users_role" '["type","id_role"]'

create_index "idx_models_manufacturer" '["type","id_manufacturer"]'
create_index "idx_models_release_date" '["type","release_date"]'

create_index "idx_models_to_product_types_type" '["type","id_type"]'

create_index "idx_product_model" '["type","id_model"]'
create_index "idx_product_specification" '["type","id_specification"]'
create_index "idx_product_stock" '["type","stock_quantity"]'
create_index "idx_product_model_color" '["type","id_model","color_name"]'
create_index "idx_product_model_color_size" '["type","id_model","color_name","size_value"]'

create_index "idx_orders_user_date" '["type","id_user","order_date"]'
create_index "idx_orders_status_date" '["type","id_status","order_date"]'
create_index "idx_orders_date" '["type","order_date"]'

create_index "idx_order_items_order" '["type","id_order"]'
create_index "idx_order_items_product" '["type","id_product"]'
create_index "idx_order_items_order_product" '["type","id_order","id_product"]'