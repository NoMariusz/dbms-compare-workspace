db = db.getSiblingDB('skates_shop');

const collections = [
  'user_roles',
  'users',
  'manufacturers',
  'models',
  'product_types',
  'products',
  'product_variants',
  'gear_specifications',
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
