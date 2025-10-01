from sqlalchemy import URL


def create_connection_string_postgresql(user: str, password: str, host: str, port: str, database: str) -> URL:
    return URL.create(
        "postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=port,
        database=database
    )