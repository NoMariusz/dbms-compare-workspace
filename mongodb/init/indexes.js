db = db.getSiblingDB('skates_shop');

// --- primary/business ids ---
db.user_roles.createIndex({ id_role: 1 }, { unique: true });
db.users.createIndex({ id_user: 1 }, { unique: true });
db.manufacturers.createIndex({ id_manufacturer: 1 }, { unique: true });
db.product_types.createIndex({ id_type: 1 }, { unique: true });
db.models.createIndex({ id_model: 1 }, { unique: true });
db.gear_specifications.createIndex({ id_specification: 1 }, { unique: true });
db.product.createIndex({ id_product: 1 }, { unique: true });
db.order_status.createIndex({ id_status: 1 }, { unique: true });
db.orders.createIndex({ id_order: 1 }, { unique: true });
db.order_items.createIndex({ id_order_item: 1 }, { unique: true });

// --- logical constraints / lookup indexes ---
db.users.createIndex({ email: 1 }, { unique: true });
db.users.createIndex({ id_role: 1 });

db.models.createIndex({ id_manufacturer: 1 });
db.models.createIndex({ release_date: 1 });

// join table equivalent
db.models_to_product_types.createIndex({ id_model: 1, id_type: 1 }, { unique: true });
db.models_to_product_types.createIndex({ id_type: 1 });

// products
db.product.createIndex({ id_model: 1 });
db.product.createIndex({ id_specification: 1 });
db.product.createIndex({ stock_quantity: 1 });
db.product.createIndex({ id_model: 1, color_name: 1 });
db.product.createIndex({ id_model: 1, color_name: 1, size_value: 1 });

// orders
db.orders.createIndex({ id_user: 1, order_date: -1 });
db.orders.createIndex({ id_status: 1, order_date: -1 });
db.orders.createIndex({ order_date: 1 });

// order items
db.order_items.createIndex({ id_order: 1 });
db.order_items.createIndex({ id_product: 1 });
db.order_items.createIndex({ id_order: 1, id_product: 1 });
