from sqlalchemy import URL


def create_connection_string_mysql(username: str, password: str, host: str, port: str, database: str) -> URL:
    return URL.create(
        "mysql+mysqldb",
        username=username,
        password=password,
        host=host,
        port=port,
        database=database
    )