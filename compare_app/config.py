import argparse

from constants import DBDataSize, DBMSType

# Default values
_DEFAULT_POSTGRES_LTS_DB = 'main_db'
_DEFAULT_POSTGRES_11_DB = 'main_db'
_DEFAULT_MONGO_DATABASE = 'skates_shop'
_DEFAULT_COUCHDB_DATABASE = 'skates_shop'
_DEFAULT_OUTPUT_FILE_PATH = './data/results/base_results.csv'
_DEFAULT_TESTED_SIZES = [DBDataSize.SMALL, DBDataSize.MEDIUM, DBDataSize.LARGE]
_DEFAULT_TESTED_DBMS = [DBMSType.PostgreSQL_LTS, DBMSType.PostgreSQL_11]
_DEFAULT_NUMBER_OF_TEST_RUNS = 3
_DEFAULT_TEST_CASES: list[str] | None = None

# Configuration variables (set by init_config)
POSTGRES_LTS_DB = _DEFAULT_POSTGRES_LTS_DB
POSTGRES_11_DB = _DEFAULT_POSTGRES_11_DB
MONGO_DATABASE = _DEFAULT_MONGO_DATABASE
COUCHDB_DATABASE = _DEFAULT_COUCHDB_DATABASE
OUTPUT_FILE_PATH = _DEFAULT_OUTPUT_FILE_PATH
TESTED_SIZES = _DEFAULT_TESTED_SIZES
TESTED_DBMS = _DEFAULT_TESTED_DBMS
NUMER_OF_TEST_RUNS = _DEFAULT_NUMBER_OF_TEST_RUNS
TESTED_TEST_CASE_NAMES = _DEFAULT_TEST_CASES

""" Example run command:
python main.py --postgres-lts-db main_db --output-path ./data/results/base_results_alt.csv --sizes 500k 1m --dbms PostgreSQL_LTS --test-runs 5 --test-cases r1_read_user_by_email r2_list_products_by_type
python main.py --output-path ./data/results/simple_results_non_indexed.csv --sizes 500k --dbms PostgreSQL_LTS PostgreSQL_11 --test-runs 5
python main.py --postgres-lts-db indexed_db --output-path ./data/results/results_format_v2.csv --sizes 500k 1m --dbms PostgreSQL_LTS --test-runs 5
"""

def _parse_arguments(argv=None):
	"""Parse command-line arguments."""
	parser = argparse.ArgumentParser(
		description='Database comparison benchmark configuration',
		add_help=True
	)

	parser.add_argument(
		'--postgres-lts-db',
		default=_DEFAULT_POSTGRES_LTS_DB,
		help=f'PostgreSQL LTS database name (default: {_DEFAULT_POSTGRES_LTS_DB})'
	)
	parser.add_argument(
		'--postgres-11-db',
		default=_DEFAULT_POSTGRES_11_DB,
		help=f'PostgreSQL 11 database name (default: {_DEFAULT_POSTGRES_11_DB})'
	)
	parser.add_argument(
		'--mongo-database',
		default=_DEFAULT_MONGO_DATABASE,
		help=f'MongoDB database name (default: {_DEFAULT_MONGO_DATABASE})'
	)
	parser.add_argument(
		'--couchdb-database',
		default=_DEFAULT_COUCHDB_DATABASE,
		help=f'CouchDB database name (default: {_DEFAULT_COUCHDB_DATABASE})'
	)
	parser.add_argument(
		'--output-path',
		default=_DEFAULT_OUTPUT_FILE_PATH,
		help=f'Output CSV file path (default: {_DEFAULT_OUTPUT_FILE_PATH})'
	)
	parser.add_argument(
		'--sizes',
		nargs='+',
		default=[s.label for s in _DEFAULT_TESTED_SIZES],
		metavar='SIZE',
		help=f'Data sizes to test, e.g., "500k 1m 10m" (default: {" ".join(s.label for s in _DEFAULT_TESTED_SIZES)})'
	)
	parser.add_argument(
		'--dbms',
		nargs='+',
		default=[d.name for d in _DEFAULT_TESTED_DBMS],
		metavar='DBMS',
		help=f'DBMS to test, e.g., "PostgreSQL_LTS PostgreSQL_11" (default: {" ".join(d.name for d in _DEFAULT_TESTED_DBMS)})'
	)
	parser.add_argument(
		'--test-runs',
		type=int,
		default=_DEFAULT_NUMBER_OF_TEST_RUNS,
		help=f'Number of test runs per configuration (default: {_DEFAULT_NUMBER_OF_TEST_RUNS})'
	)
	parser.add_argument(
		'--test-cases',
		nargs='+',
		default=_DEFAULT_TEST_CASES,
		metavar='TEST_CASE',
		help='Optional list of test case names to run, e.g., "r1_read_user_by_email u1_update_user_contact"'
	)

	return parser.parse_args(argv)


def init_config(argv=None):
	"""Initialize configuration from command-line arguments.
	
	Args:
		argv: List of arguments to parse. If None, uses sys.argv.
	
	Example:
		init_config(['--postgres-lts-db', 'test_db', '--sizes', '500k', '1m'])
	"""
	global POSTGRES_LTS_DB, POSTGRES_11_DB, MONGO_DATABASE, COUCHDB_DATABASE
	global OUTPUT_FILE_PATH, TESTED_SIZES, TESTED_DBMS, NUMER_OF_TEST_RUNS, TESTED_TEST_CASE_NAMES

	args = _parse_arguments(argv)

	POSTGRES_LTS_DB = args.postgres_lts_db
	POSTGRES_11_DB = args.postgres_11_db
	MONGO_DATABASE = args.mongo_database
	COUCHDB_DATABASE = args.couchdb_database
	OUTPUT_FILE_PATH = args.output_path
	NUMER_OF_TEST_RUNS = args.test_runs
	TESTED_TEST_CASE_NAMES = args.test_cases

	# Parse and validate sizes
	TESTED_SIZES = []
	for size_label in args.sizes:
		matching_sizes = [s for s in DBDataSize if s.label == size_label]
		if matching_sizes:
			TESTED_SIZES.append(matching_sizes[0])
		else:
			available = [s.label for s in DBDataSize]
			raise ValueError(f"Unknown size: {size_label}. Available: {available}")

	# Parse and validate DBMS types
	TESTED_DBMS = []
	for dbms_name in args.dbms:
		matching_dbms = [d for d in DBMSType if d.name == dbms_name]
		if matching_dbms:
			TESTED_DBMS.append(matching_dbms[0])
		else:
			available = [d.name for d in DBMSType]
			raise ValueError(f"Unknown DBMS: {dbms_name}. Available: {available}")


# Initialize configuration with command-line arguments at module load time
init_config()