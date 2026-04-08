const rolesDb = db.getSiblingDB('skates_shop_roles');

const allCollections = [
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

const standardWriteCollections = [
	'users',
	'orders',
	'order_items',
	'order_status',
];
const moderatorWriteCollections = allCollections.filter(
	(collection) => collection !== 'user_roles',
);

function ensureRole(roleName, privileges) {
	const existing = rolesDb.getRole(roleName);
	if (existing) {
		return;
	}

	rolesDb.createRole({
		role: roleName,
		privileges,
		roles: [],
	});
}

function collectionPrivileges(collections, actions) {
	return collections.map((collection) => ({
		resource: { db: 'skates_shop_roles', collection },
		actions,
	}));
}

const usersRolePrivileges = [
	{
		resource: { db: 'skates_shop_roles', collection: '' },
		actions: ['find'],
	},
	...collectionPrivileges(standardWriteCollections, [
		'find',
		'insert',
		'update',
		'remove',
	]),
];

const moderatorsRolePrivileges = [
	{
		resource: { db: 'skates_shop_roles', collection: 'user_roles' },
		actions: ['find'],
	},
	...collectionPrivileges(moderatorWriteCollections, [
		'find',
		'insert',
		'update',
		'remove',
	]),
];

ensureRole('users', usersRolePrivileges);
ensureRole('moderators', moderatorsRolePrivileges);

const moderatorUsername = process.env.MONGO_ROLES_DB_USER || 'moderator_user';
const moderatorPassword =
	process.env.MONGO_ROLES_DB_PASSWORD || 'moderator_password123';
const standardUsername = process.env.MONGO_STANDARD_DB_USER || 'standard_user';
const standardPassword =
	process.env.MONGO_STANDARD_DB_PASSWORD || 'standard_password123';

if (!rolesDb.getUser(moderatorUsername)) {
	rolesDb.createUser({
		user: moderatorUsername,
		pwd: moderatorPassword,
		roles: [{ role: 'moderators', db: 'skates_shop_roles' }],
	});
}

if (!rolesDb.getUser(standardUsername)) {
	rolesDb.createUser({
		user: standardUsername,
		pwd: standardPassword,
		roles: [{ role: 'users', db: 'skates_shop_roles' }],
	});
}
