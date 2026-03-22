from constants import DBDataSize, DBMSType

# selected databases for each dbms to perform the comparison
POSTGRES_LTS_DB='main_db'
POSTGRES_11_DB='main_db'
MONGO_DATABASE='skates_shop'
COUCHDB_DATABASE='skates_shop'

OUTPUT_FILE_PATH = './data/base_results.csv'

# sizes of data to perform the test cases for each dbms
TESTED_SIZES = [DBDataSize.SMALL, DBDataSize.MEDIUM, DBDataSize.LARGE]

# dbms to perform the test cases for
# TESTED_DBMS = [DBMSType.PostgreSQL_LTS, DBMSType.PostgreSQL_11, DBMSType.MongoDB, DBMSType.CouchDB]
TESTED_DBMS = [DBMSType.PostgreSQL_LTS, DBMSType.PostgreSQL_11]
