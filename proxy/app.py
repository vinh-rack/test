import json
from pathlib import Path
from uuid import uuid4

import aiohttp

from db.connection_string import create_connection_string
from db.db_connection import DbConnection
from utils.config import settings
from utils.logger import setup_logger

# ===================================================
# Setup
# ===================================================

def _load_queries(directory: str) -> dict[str, str]:
    queries = {}

    for file in Path(directory).glob("*.sql"):
        with open(file, encoding="utf-8") as f:
            queries[file.stem] = f.read()
    
    return queries

QUERIES = _load_queries("../queries")

LOGGER = setup_logger("Proxy Logs", "proxy.log")

# ===================================================
# Credentials and DB Connection Functions
# ===================================================

def create_connection(conn_string: str) -> DbConnection:
    try:
        return DbConnection(conn_string)
    except Exception as e:
        LOGGER.error(f"Error creating connection: {e}")
        raise

# TODO: Wait until vault is ready
def store_db_credentials(
    name: str,
    db_type: str,
    database: str,
    host: str,
    port: int,
    username: str,
    password: str, 
    vault: bool = False
) -> str:

    uuid = str(uuid4())
    key = f"{db_type}_{host}_{database}"
    value = {
        "db_type": db_type,
        "database": database,
        "host": host,
        "port": port,
        "username": username,
        "password": password
    }

    if vault:
        return NotImplementedError("Vault storage is not implemented yet.")
    else:
        data = {
            "uuid": uuid,
            "name": name,
            "type": "db",
            "key": key,
            "value": value
        }

        with open(settings.db_credentials_path, "a", encoding="utf-8") as f:
            json.dump(data, f)
            f.write("\n")

        settings.reload()

        return uuid

def store_sn_credentials(
    name: str,
    instance_url: str,
    username: str,
    password: str,
    vault: bool = False
) -> dict:
    
    uuid = str(uuid4())
    key = f"sn_{username}"
    value = {
        "instance_url": instance_url,
        "username": username,
        "password": password
    }

    if vault:
        return NotImplementedError("Vault storage is not implemented yet.")
    else:
        data = {
            "uuid": uuid,
            "name": name,
            "type": "servicenow",
            "key": key,
            "value": value
        }

        with open(settings.sn_credentials_path, "a", encoding="utf-8") as f:
            json.dump(data, f)
            f.write("\n")
    
        settings.reload()

        return uuid

def retrieve_credentials(uuid: str, cred_type: str, vault: bool = False) -> dict:
    try:
        if vault:
            # Retrieve from vault
            return {}
        
        cred_file = settings.db_credentials_path if cred_type == "db" else settings.sn_credentials_path

        with open(cred_file, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue

                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    LOGGER.warning(f"Skipping invalid line in {cred_file}: {line}")
                    continue

                if obj.get("uuid") == uuid and obj.get("type") == cred_type:
                    return obj.get("value")

        # If no match found
        LOGGER.warning(f"Credential with uuid={uuid} and type={cred_type} not found.")
        return {}
    except Exception as e:
        LOGGER.error(f"Error retrieving credentials: {e}")
        raise

def delete_credentials(uuid: str, cred_type: str, vault: bool = False) -> bool:
    try:
        if vault:
            # Retrieve from vault
            return {}
        
        out = []
        cred_file = settings.db_credentials_path if cred_type == "db" else settings.sn_credentials_path

        # Read and filter
        with open(cred_file, "r") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    if obj.get("uuid") != uuid:
                        out.append(line)  # keep original line
                except json.JSONDecodeError:
                    # If line isn't valid JSON, keep it or log error
                    out.append(line)

        # Rewrite file without the deleted entry
        with open(cred_file, "w") as f:
            f.writelines(out)
        
        settings.reload()
    except Exception as e:
        LOGGER.error(f"Error deleting credentials: {e}")
        raise

def get_connection_string(uuid: str, vault: bool = False) -> str:
    try:
        if vault:
            # Retrieve from vault
            return NotImplementedError("Vault retrieval is not implemented yet.")

        cred = retrieve_credentials(
            uuid=uuid,
            cred_type="db",
            vault=False
        )

        return create_connection_string(
            db_type=cred.get("db_type"),
            database=cred.get("database"),
            host=cred.get("host"),
            port=cred.get("port"),
            username=cred.get("username"),
            password=cred.get("password")
        )
    except Exception as e:
        LOGGER.error(f"Error getting connection string: {e}")
    

def set_current_connection(uuid: str) -> DbConnection:
    try:
        conn_string = get_connection_string(uuid)
        db_connection = create_connection(conn_string)
        
        return db_connection
    except Exception as e:
        LOGGER.error(f"Error setting current connection: {e}")
        raise

    
# TODO: Wait until vault is ready
def list_vaults() -> list: 
    
    raise NotImplementedError("Vault functionality is not implemented yet.")

# ===================================================
# Database Tools
# ===================================================

async def check_health(db_connection: DbConnection) -> list:
    try:
        # ko nên dính file, nên in-memory
        # hơi dài dòng, nên để trong memory
        # nếu mở file thì mở 1 file lúc khởi tạo
        result = await db_connection.execute(QUERIES["health_check"])

        LOGGER.info("Health check query executed successfully")

        return result
    except Exception as e:
        LOGGER.error(f"Error checking health: {e}")

async def check_log_space(db_connection: DbConnection):
    try:
        result = await db_connection.execute(QUERIES["log_space"])

        LOGGER.info("Log Space query executed successfully")

        return result
    except Exception as e:
        LOGGER.error(f"Error checking log space: {e}")

async def check_blocking_sessions(db_connection: DbConnection):
    try:
        result = await db_connection.execute(QUERIES["blocking_sessions"])

        LOGGER.info("Blocking Sessions query executed successfully")

        return result
    except Exception as e:
        LOGGER.error(f"Error checking blocking sessions: {e}")

async def check_index_fragmentation(db_connection: DbConnection, db_name: str):

    try:
        params = {"db_name": db_name}
        result = await db_connection.execute(QUERIES["index_frag"], **params)

        LOGGER.info("Index Fragmentation query executed successfully")

        return result
    except Exception as e:
        LOGGER.error(f"Error checking index frag: {e}")

async def check_db_size(db_connection: DbConnection, db_name: str):
    try:
        params = {"db_name": db_name}
        result = await db_connection.execute(QUERIES["db_size"], **params)

        LOGGER.info("DB Size query executed successfully")

        return result
    except Exception as e:
        LOGGER.error(f"Error checking database size: {e}")

async def change_password(db_connection: DbConnection, login_name: str, new_password: str):

    try:
        params = {"login_name": login_name, "new_password": new_password}
        result = await db_connection.execute(QUERIES["change_pwd"], **params)

        LOGGER.info("Change Password query executed successfully")

        return result
    except Exception as e:
        LOGGER.error(f"Error changing password: {e}")

# ===================================================
# ServiceNow Tools
# ===================================================

async def get_sn_users(instance_url: str, username: str, password: str):
    try:
        url_users = f"{instance_url}/api/now/table/sys_user"

        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(username, password)) as session:
            async with session.get(
                url_users,
                headers={"Accept": "application/json"},
                params={"sysparm_limit": "5"}
            ) as response:

                data = await response.json()

                return data
    except Exception as e:
        LOGGER.error(f"Error getting SN users: {e}")

async def get_sn_roles(instance_url: str, username: str, password: str):
    try:
        url_roles = f"{instance_url}/api/now/table/sys_user_role"

        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(username, password)) as session:
            async with session.get(
                url_roles,
                headers={"Accept": "application/json"},
                params={"sysparm_limit": "5"}
            ) as response:

                data = await response.json()

                return data
    except Exception as e:
        LOGGER.error(f"Error getting SN roles: {e}")

async def get_sn_incidents(instance_url: str, username: str, password: str):
    try:
        url_incidents = f"{instance_url}/api/now/table/incident"

        async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(username, password)) as session:
            async with session.get(
                url_incidents,
                headers={"Accept": "application/json"},
                params={"sysparm_limit": "5"}
            ) as response:

                data = await response.json()

                return data
    except Exception as e:
        LOGGER.error(f"Error getting SN incidents: {e}")  