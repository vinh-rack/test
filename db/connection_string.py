from typing import Callable, Union

from sqlalchemy import URL

from db.mariadb import create_connection_string_mariadb
from db.mssql import create_connection_string_mssql
from db.mysql import create_connection_string_mysql
from db.oracle import create_connection_string_oracle
from db.postgresql import create_connection_string_postgresql
from db.sqlite import create_connection_string_sqlite

DB_CONNECTION_BUILDERS: dict[str, Callable[..., str]] = {
    "mariadb": create_connection_string_mariadb,
    "mssql": create_connection_string_mssql,
    "mysql": create_connection_string_mysql,
    "oracle": create_connection_string_oracle,
    "postgresql": create_connection_string_postgresql,
    "sqlite": create_connection_string_sqlite,
}

def create_connection_string(
    db_type: str,
    database: str,
    host: str,
    port: int,
    username: str,
    password: str
) -> Union[str, URL]:
    
    builder = DB_CONNECTION_BUILDERS.get(db_type.lower())
    
    if not builder:
        raise ValueError(f"Unsupported database type: {db_type}")

    return builder(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password
    )