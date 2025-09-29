import os
import urllib

from sqlalchemy import URL, create_engine, text

from fastmcp import tool
from utils.config import settings
from utils.logger import setup_logger

LOGGER = setup_logger("db_tools")

@tool(
    "health_check",
    "Perform a health check using the connection string"
)
async def health_check(conn_string: str, db_name: str):
    try:
        engine = create_engine(conn_string)
        with engine.connect() as conn:
            query = text(open("../queries/health_check.sql").read())

            result = conn.execute(query)

            if result.returns_rows:
                LOGGER.info("Query executed successfully")
                rows = [row for row in result if row.DatabaseName == db_name]

                return rows
            else:
                return "Query executed successfully with no result rows"
    except Exception as e:
        LOGGER.error(f"Error checking log space: {e}")

@tool(
    "check_db_size",
    "Check the size of an MSSQL database"
)
async def check_db_size(conn_string: str, db_name: str):
    try:
        engine = create_engine(conn_string)
        with engine.connect() as conn:
            query = text(text(open("../queries/db_size.sql").read()))

            result = conn.execute(query, {"db_name": db_name})

            if result.returns_rows:
                LOGGER.info("Query executed successfully")
                rows = [dict(row._mapping) for row in result.fetchall()]

                return rows
            else:
                return "Query executed successfully with no result rows"
    except Exception as e:
        LOGGER.error(f"Error checking database size: {e}")

@tool(
    "check_log_space",
    "Check the log space usage of an MSSQL database"
)
async def check_log_space(conn_string: str, db_name: str):
    try:
        engine = create_engine(conn_string)
        with engine.connect() as conn:
            query = text(open("../queries/log_space.sql").read())

            result = conn.execute(query)

            if result.returns_rows:
                LOGGER.info("Query executed successfully")
                rows = [dict(row._mapping) for row in result.fetchall() if row.DatabaseName == db_name]

                return rows
            else:
                return "Query executed successfully with no result rows"
    except Exception as e:
        LOGGER.error(f"Error checking log space: {e}")

@tool(
    "check_blocking_sessions",
    "Check for blocking sessions in an MSSQL database"
)
async def check_blocking_sessions(conn_string: str):
    try:
        engine = create_engine(conn_string)
        with engine.connect() as conn:
            query = text(open("../queries/blocking_sessions.sql").read())

            result = conn.execute(query)

            if result.returns_rows:
                LOGGER.info("Query executed successfully")
                rows = [dict(row._mapping) for row in result.fetchall()]

                return rows
            else:
                return "Query executed successfully with no result rows"
    except Exception as e:
        LOGGER.error(f"Error checking blocking sessions: {e}")

@tool(
    "check_index_fragmentation",
    "Check for index fragmentations in a MSSQL Database"
)
def check_index_fragmentation(conn_string: str, db_name: str):

    try:
        engine = create_engine(conn_string)

        with engine.connect() as conn:
            query = text(open("../queries/index_frag.sql").read())

            result = conn.execute(query, {"db_name": db_name})

            if result.returns_rows:
                LOGGER.info("Query executed successfully")
                rows = [dict(row._mapping) for row in result.fetchall()]

                return rows
            else:
                return "Query executed successfully with no result rows"
    except Exception as e:
        LOGGER.error(f"Error checking index frag: {e}")