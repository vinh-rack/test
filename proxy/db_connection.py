import threading

from sqlalchemy import create_engine, text

from utils.logger import setup_logger


class DbConnection:
    def __init__(self, conn_string: str, timeout: int = 60):
        self.engine = create_engine(conn_string)
        self.conn = self.engine.connect()
        self.timeout = timeout
        self._timer = None
        self._reset_timer()
        self.logger = setup_logger("DB Connection", "db_connection.log")

    def _reset_timer(self):
        """Cancel old timer and start a new one."""
        if self._timer:
            self._timer.cancel()
        self._timer = threading.Timer(self.timeout, self.close)
        self._timer.daemon = True
        self._timer.start()

    def execute(self, query: str, **params):
        """Execute query and reset silence timer."""
        self._reset_timer()
        return self.conn.execute(text(query), params)

    def close(self):
        """Close connection and engine."""
        if self.conn and not self.conn.closed:
            self.conn.close()
        if self.engine:
            self.engine.dispose()
        if self._timer:
            self._timer.cancel()
            
        self.logger.info("Connection closed due to inactivity")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()