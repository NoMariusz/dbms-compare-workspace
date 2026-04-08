const collections = [
	'user_roles',
	'users',
	'manufacturers',
	'product_types',
	'models',
	'models_to_product_types',
	'gear_specifications',
	'product',
	'order_status',
	'orders',
	'order_items',
];

const databasesToInitialize = [
	'skates_shop',
	'skates_shop_encrypted',
	'skates_shop_without_indexes',
	'skates_shop_roles',
];

databasesToInitialize.forEach((dbName) => {
	const targetDb = db.getSiblingDB(dbName);
	const existing = targetDb.getCollectionNames();

	collections.forEach((name) => {
		if (!existing.includes(name)) {
			targetDb.createCollection(name);
		}
	});
});
