db = db.getSiblingDB('skates_shop');

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

const existing = db.getCollectionNames();

collections.forEach((name) => {
  if (!existing.includes(name)) {
    db.createCollection(name);
  }
});
