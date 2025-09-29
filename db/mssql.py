from sqlalchemy import URL


def create_connection_string(user: str, password: str, host: str, port: str, database: str, driver: str) -> URL:
    return URL.create(
        "mssql+pyodbc",
        username=user,
        password=password,
        host=host,
        port=port,
        database=database,
        query={
            "driver": {driver},
            "TrustServerCertificate": "yes",
        },
    )