import os

import aioodbc
from sqlalchemy import create_engine, text

from fastmcp import tool
from utils.logger import setup_logger

LOGGER = setup_logger("db_tools")
DB_DSN = os.getenv("DB_DSN", "http://3.109.246.243")

@tool(
    "check_db_size",
    "Check the size of an MSSQL database"
)
async def check_db_size(conn_string: str, db_name: str):
    try:
        engine = create_engine(conn_string)
        with engine.connect() as conn:
            query = text("""
                SELECT
                    DB_NAME(database_id) AS DatabaseName,
                    Name AS FileName,
                    type_desc AS FileType,
                    size * 8 / 1024 AS SizeMB
                FROM sys.master_files
                WHERE database_id = DB_ID(:db_name);
            """)

            result = conn.execute(query, {"db_name": db_name})
            
            if result.returns_rows:
                rows = [dict(row._mapping) for row in result.fetchall()]

                return rows
            else:
                return "Query executed successfully"
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
            query = text("""
                DBCC SQLPERF(LOGSPACE);
            """)

            result = conn.execute(query)
            
            if result.returns_rows:
                rows = [dict(row._mapping) for row in result.fetchall() if row.DatabaseName == db_name]

                return rows
            else:
                return "Query executed successfully"
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
            query = text("""
                SELECT
                    blocking_session_id AS BlockingSessionID,
                    session_id AS BlockedSessionID,
                    wait_type,
                    wait_time,
                    wait_resource
                FROM sys.dm_exec_requests
                WHERE blocking_session_id <> 0;
            """)

            result = conn.execute(query)
            
            if result.returns_rows:
                rows = [dict(row._mapping) for row in result.fetchall()]

                return rows
            else:
                return "Query executed successfully"
    except Exception as e:
        LOGGER.error(f"Error checking blocking sessions: {e}")