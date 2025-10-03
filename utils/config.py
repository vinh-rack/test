import json
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    log_level: str
    log_dir: str
    
    host: str
    user: str

    mssql_port: int
    mysql_port: int
    postgre_port: int

    mysql_password: str
    mssql_password: str
    postgre_password: str

    mssql_db: str
    mysql_db: str
    postgre_db: str
    
    servicenow_instance_url: str
    servicenow_username: str
    servicenow_password: str

    db_credentials_path: str = "../config/db_credentials.jsonl"
    sn_credentials_path: str = "../config/sn_credentials.jsonl"

    _db_cache: list[dict] | None = None
    _sn_cache: list[dict] | None = None
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    def load_json(self, path: str) -> list[dict]:
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        
        objects = []
        with open(path, encoding="utf-8", mode="r") as f:
            for line in f:
                line = line.strip()
                if line:  # Skip empty lines
                    try:
                        objects.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return objects

    @property
    def db_credentials(self) -> dict:
        if self._db_cache is None:
            self._db_cache = self.load_json(self.db_credentials_path)

        return self._db_cache

    @property
    def sn_credentials(self) -> dict:
        if self._sn_cache is None:
            self._sn_cache = self.load_json(self.sn_credentials_path)

        return self._sn_cache
    
    def reload(self):
        self._db_cache = None
        self._sn_cache = None

settings = Settings()