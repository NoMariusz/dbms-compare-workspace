#!/bin/sh
set -eu

COUCH_URL="http://${COUCHDB_USER}:${COUCHDB_PASSWORD}@couchdb:5984"
DB="${COUCHDB_DB}"
DB_WITHOUT_INDEXES="${COUCHDB_DB_WITHOUT_INDEXES:-skates_shop_without_indexes}"
DB_ROLES="${COUCHDB_DB_ROLES:-skates_shop_roles}"

COUCHDB_ROLES_DB_USER="${COUCHDB_ROLES_DB_USER:-moderator_user}"
COUCHDB_ROLES_DB_PASSWORD="${COUCHDB_ROLES_DB_PASSWORD:-moderator_password123}"
COUCHDB_STANDARD_DB_USER="${COUCHDB_STANDARD_DB_USER:-standard_user}"
COUCHDB_STANDARD_DB_PASSWORD="${COUCHDB_STANDARD_DB_PASSWORD:-standard_password123}"

until curl -fsS "${COUCH_URL}/" >/dev/null; do
  sleep 2
done

# Create business databases if they do not exist
curl -fsS -X PUT "${COUCH_URL}/${DB}" >/dev/null 2>&1 || true
curl -fsS -X PUT "${COUCH_URL}/${DB_WITHOUT_INDEXES}" >/dev/null 2>&1 || true
curl -fsS -X PUT "${COUCH_URL}/${DB_ROLES}" >/dev/null 2>&1 || true

create_index_for_db() {
  TARGET_DB="$1"
  NAME="$2"
  FIELDS="$3"

  curl -fsS -X POST "${COUCH_URL}/${TARGET_DB}/_index" \
    -H "Content-Type: application/json" \
    -d "{\"index\":{\"fields\":${FIELDS}},\"name\":\"${NAME}\",\"type\":\"json\"}" \
    >/dev/null
}

create_index() {
  NAME="$1"
  FIELDS="$2"

  create_index_for_db "${DB}" "${NAME}" "${FIELDS}"
  create_index_for_db "${DB_ROLES}" "${NAME}" "${FIELDS}"
}

create_user_if_not_exists() {
  USERNAME="$1"
  PASSWORD="$2"
  ROLE_NAME="$3"

  curl -fsS -X PUT "${COUCH_URL}/_users/org.couchdb.user:${USERNAME}" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"${USERNAME}\",\"password\":\"${PASSWORD}\",\"roles\":[\"${ROLE_NAME}\"],\"type\":\"user\"}" \
    >/dev/null 2>&1 || true
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

# --- roles db security and access control ---
create_user_if_not_exists "${COUCHDB_ROLES_DB_USER}" "${COUCHDB_ROLES_DB_PASSWORD}" "moderators"
create_user_if_not_exists "${COUCHDB_STANDARD_DB_USER}" "${COUCHDB_STANDARD_DB_PASSWORD}" "users"

curl -fsS -X PUT "${COUCH_URL}/${DB_ROLES}/_security" \
  -H "Content-Type: application/json" \
  -d '{"admins":{"names":[],"roles":[]},"members":{"names":[],"roles":["users","moderators"]}}' \
  >/dev/null

curl -fsS -X PUT "${COUCH_URL}/${DB_ROLES}/_design/access_control" \
  -H "Content-Type: application/json" \
  -d '{"_id":"_design/access_control","validate_doc_update":"function(newDoc, oldDoc, userCtx) { if (userCtx.roles.indexOf(\"_admin\") !== -1) { return; } var isModerator = userCtx.roles.indexOf(\"moderators\") !== -1; var isUser = userCtx.roles.indexOf(\"users\") !== -1; if (!isModerator && !isUser) { throw({unauthorized: \"Authentication required.\"}); } var docType = (newDoc && newDoc.type) || (oldDoc && oldDoc.type); if (!docType) { throw({forbidden: \"Document type is required.\"}); } if (isModerator) { if (docType === \"user_roles\") { throw({forbidden: \"moderators role: user_roles is read-only.\"}); } return; } if (isUser) { var allowedWrites = { users: true, orders: true, order_items: true, order_status: true }; if (!allowedWrites[docType]) { throw({forbidden: \"users role cannot modify this document type.\"}); } return; } throw({forbidden: \"Operation is not allowed.\"}); }"}' \
  >/dev/null