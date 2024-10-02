from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .get_secrets import get_secret

DB_URL = get_secret()

DB: AsyncEngine = create_async_engine(DB_URL)
