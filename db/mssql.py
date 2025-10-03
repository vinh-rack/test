from sqlalchemy import URL


def create_connection_string_mssql(username: str, password: str, host: str, port: str, database: str) -> URL:
    return URL.create(
        # "mssql+pyodbc",
        # "mssql+pymssql",
        "mssql+aioodbc",
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
        query={
            "driver": "ODBC Driver 18 for SQL Server",
            "TrustServerCertificate": "yes",
        },
    )