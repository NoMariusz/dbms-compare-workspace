from enum import Enum


class DBDataSize(Enum):
    SMALL = 1
    MEDIUM = 2
    LARGE = 3

    @property
    def row_count(self) -> int:
        if self == DBDataSize.SMALL:
            return 500000
        if self == DBDataSize.MEDIUM:
            return 1000000
        return 10000000

    @property
    def label(self) -> str:
        if self == DBDataSize.SMALL:
            return "500k"
        if self == DBDataSize.MEDIUM:
            return "1m"
        return "10m"

class DBMSType(Enum):
    PostgreSQL_LTS = 1
    PostgreSQL_11 = 2
    MongoDB = 3
    CouchDB = 4