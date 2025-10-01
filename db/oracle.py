from sqlalchemy import URL


def create_connection_string_oracle(user: str, password: str, host: str, port: str, database: str) -> URL:
    return URL.create(
        "oracle+oracledb",
        username=user,
        password=password,
        host=host,
        port=port,
        database=database
    )