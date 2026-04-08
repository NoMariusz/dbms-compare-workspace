const databasesWithIndexes = [
	'skates_shop',
	'skates_shop_encrypted',
	'skates_shop_roles',
];

function applyIndexes(targetDb) {
	// --- primary/business ids ---
	targetDb.user_roles.createIndex({ id_role: 1 }, { unique: true });
	targetDb.users.createIndex({ id_user: 1 }, { unique: true });
	targetDb.manufacturers.createIndex(
		{ id_manufacturer: 1 },
		{ unique: true },
	);
	targetDb.product_types.createIndex({ id_type: 1 }, { unique: true });
	targetDb.models.createIndex({ id_model: 1 }, { unique: true });
	targetDb.gear_specifications.createIndex(
		{ id_specification: 1 },
		{ unique: true },
	);
	targetDb.product.createIndex({ id_product: 1 }, { unique: true });
	targetDb.order_status.createIndex({ id_status: 1 }, { unique: true });
	targetDb.orders.createIndex({ id_order: 1 }, { unique: true });
	targetDb.order_items.createIndex({ id_order_item: 1 }, { unique: true });

	// --- logical constraints / lookup indexes ---
	targetDb.users.createIndex({ email: 1 }, { unique: true });
	targetDb.users.createIndex({ id_role: 1 });

	targetDb.models.createIndex({ id_manufacturer: 1 });
	targetDb.models.createIndex({ release_date: 1 });

	// join table equivalent
	targetDb.models_to_product_types.createIndex(
		{ id_model: 1, id_type: 1 },
		{ unique: true },
	);
	targetDb.models_to_product_types.createIndex({ id_type: 1 });

	// products
	targetDb.product.createIndex({ id_model: 1 });
	targetDb.product.createIndex({ id_specification: 1 });
	targetDb.product.createIndex({ stock_quantity: 1 });
	targetDb.product.createIndex({ id_model: 1, color_name: 1 });
	targetDb.product.createIndex({ id_model: 1, color_name: 1, size_value: 1 });

	// orders
	targetDb.orders.createIndex({ id_user: 1, order_date: -1 });
	targetDb.orders.createIndex({ id_status: 1, order_date: -1 });
	targetDb.orders.createIndex({ order_date: 1 });

	// order items
	targetDb.order_items.createIndex({ id_order: 1 });
	targetDb.order_items.createIndex({ id_product: 1 });
	targetDb.order_items.createIndex({ id_order: 1, id_product: 1 });
}

databasesWithIndexes.forEach((dbName) => {
	const targetDb = db.getSiblingDB(dbName);
	applyIndexes(targetDb);
});
