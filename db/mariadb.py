from sqlalchemy import URL


def create_connection_string_mariadb(username: str, password: str, host: str, port: str, database: str) -> URL:
    return URL.create(
        "mariadb+mysqldb",
        username=username,
        password=password,
        host=host,
        port=port,
        database=database
    )