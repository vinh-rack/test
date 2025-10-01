from sqlalchemy import URL


def create_connection_string_mssql(user: str, password: str, host: str, port: str, database: str) -> URL:
    return URL.create(
        "mssql+pyodbc",
        username=user,
        password=password,
        host=host,
        port=port,
        database=database,
        query={
            "driver": "ODBC Driver 18 for SQL Server",
            "TrustServerCertificate": "yes",
        },
    )