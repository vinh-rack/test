import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from utils.logger import setup_logger


class DbConnection:
    def __init__(self, conn_string: str, timeout: int = 60):
        self.conn_string = conn_string
        self.engine = create_async_engine(
            conn_string, 
            pool_pre_ping=True
        )

        self.conn: AsyncConnection | None = None
        self.timeout = timeout
        
        self._timeout_task: asyncio.Task | None = None
        self.logger = setup_logger("DB Connection Async", "db_connection_async.log")

    def _reset_timer(self):
        """Cancel old timer and start a new async task for inactivity timeout."""
        
        if self._timeout_task and not self._timeout_task.done():
            self._timeout_task.cancel()

        self._timeout_task = asyncio.create_task(self._close_after_timeout())

    async def connect(self):
        """Establish a new async connection and start timeout watchdog."""
        
        if not self.conn or self.conn.closed:
            self.conn = await self.engine.connect()
            self.logger.info("Connected to database successfully")

        # Start or refresh timeout timer
        self._reset_timer()

    async def _close_after_timeout(self):
        try:
            await asyncio.sleep(self.timeout)
            await self.close()
            self.logger.info("Connection closed due to inactivity")
        except asyncio.CancelledError:
            # Timer was reset before timeout expired
            pass

    async def reconnect(self):
        """Dispose old engine and reconnect."""
        
        await self.close()
        self.engine = create_async_engine(self.conn_string, pool_pre_ping=True)
        self.conn = await self.engine.connect()
        self.logger.info("Reconnected to database successfully")

    async def close(self):
        """Close connection and engine."""
        
        if self.conn and not self.conn.closed:
            await self.conn.close()
        if self.engine:
            await self.engine.dispose()
        if self._timeout_task and not self._timeout_task.done():
            self._timeout_task.cancel()

        self.logger.info("Connection closed due to inactivity")

    async def execute(self, query: str, **params):
        """Execute query and reset silence timer."""
        
        self._reset_timer()

        try:
            if not self.conn or self.conn.closed:
                self.logger.info("No active connection. Connecting...")
                self.conn = await self.engine.connect()
                
            return await self.conn.execute(text(query), params)
        except Exception as e:
            self.logger.warning(f"Connection dropped, reconnecting... ({e})")
            await self.reconnect()
            return await self.conn.execute(text(query), params)

    async def __aenter__(self):
        if not self.conn or self.conn.closed:
            await self.connect()

        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()