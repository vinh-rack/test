from sqlalchemy import URL


def create_connection_string_postgresql(username: str, password: str, host: str, port: str, database: str) -> URL:
    return URL.create(
        "postgresql+psycopg2",
        username=username,
        password=password,
        host=host,
        port=port,
        database=database
    )