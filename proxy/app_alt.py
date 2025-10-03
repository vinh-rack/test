import uuid
from typing import Callable, Union

from sqlalchemy import URL, create_engine, text

from db.db_connection_alt import DbConnection
from db.mariadb import create_connection_string_mariadb
from db.mssql import create_connection_string_mssql
from db.mysql import create_connection_string_mysql
from db.oracle import create_connection_string_oracle
from db.postgresql import create_connection_string_postgresql
from db.sqlite import create_connection_string_sqlite
from utils.logger import setup_logger

LOGGER = setup_logger("Proxy Logs", "proxy.log")

DB_CONNECTION_BUILDERS: dict[str, Callable[..., str]] = {
    "mariadb": create_connection_string_mariadb,
    "mssql": create_connection_string_mssql,
    "mysql": create_connection_string_mysql,
    "oracle": create_connection_string_oracle,
    "postgresql": create_connection_string_postgresql,
    "sqlite": create_connection_string_sqlite,
}


def create_connection_string(db_type: str, database: str, host: str, port: int, user: str, password: str) -> Union[str, URL]:
    try:
        builder = DB_CONNECTION_BUILDERS.get(db_type.lower())
    except KeyError:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    return builder(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

def create_connection(conn_string: str) -> DbConnection:
    try:
        return DbConnection(conn_string)
    except Exception as e:
        LOGGER.error(f"Error creating connection: {e}")
        raise

# TODO: Wait until vault is ready
def store_in_vault(conn_string: str) -> dict:
    uuid = str(uuid.uuid4())
    name = ""
    return {"name": name, "uuid": uuid}

def get_connection_string(uuid: str) -> str:
    return ""

def list_vaults() -> list: 
    

    return []

def set_current_connection(uuid: str) -> DbConnection:
    try:
        conn_string = get_connection_string(uuid)
        db_connection = create_connection(conn_string)
        
        return db_connection
    except Exception as e:
        LOGGER.error(f"Error setting current connection: {e}")
        raise

def check_health(db_connection: DbConnection) -> list:
    try:

        query = text(open("../queries/health_check.sql").read())

        result = db_connection.conn.execute(query)

        LOGGER.info("Health check query executed successfully")

        return [row for row in result]
    except Exception as e:
        LOGGER.error(f"Error checking health: {e}")

def check_db_size(db_connection: DbConnection, db_name: str):
    try:
        
        query = text(open("../queries/db_size.sql").read())

        result = db_connection.conn.execute(query, {"db_name": db_name})

        LOGGER.info("DB Size query executed successfully")

        return [row for row in result]
    except Exception as e:
        LOGGER.error(f"Error checking database size: {e}")

def check_log_space(db_connection: DbConnection):
    try:
        query = text(open("../queries/log_space.sql").read())

        result = db_connection.conn.execute(query)

        LOGGER.info("Log Space query executed successfully")

        return [row for row in result]
    except Exception as e:
        LOGGER.error(f"Error checking log space: {e}")

def check_blocking_sessions(db_connection: DbConnection):
    try:
        query = text(open("../queries/blocking_sessions.sql").read())

        result = db_connection.conn.execute(query)

        LOGGER.info("Blocking Sessions query executed successfully")

        return [row for row in result]
    except Exception as e:
        LOGGER.error(f"Error checking blocking sessions: {e}")

def check_index_fragmentation(db_connection: DbConnection, db_name: str):

    try:
        query = text(open("../queries/index_frag.sql").read())

        result = db_connection.conn.execute(query, {"db_name": db_name})

        LOGGER.info("Index Fragmentation query executed successfully")

        return [row for row in result]
    except Exception as e:
        LOGGER.error(f"Error checking index frag: {e}")

def change_password(db_connection: DbConnection, login_name: str, new_password: str):

    try:
        query = text(open("../queries/change_pwd.sql").read())

        result = db_connection.conn.execute(query, {"login_name": login_name, "password": new_password})

        LOGGER.info("Change Password query executed successfully")

        return [row for row in result]
    except Exception as e:
        LOGGER.error(f"Error changing password: {e}")