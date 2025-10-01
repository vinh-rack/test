import platform


def create_connection_string_sqlite(db: str, **kwargs) -> str:
    if platform.system() == "Windows":
        return f"sqlite:///{db}"
    else:
        return f"sqlite:////{db}"
        