import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
import logging

logger = logging.getLogger(__name__)

POSTGRES_URL = os.getenv(
    "POSTGRES_URL", 
    "postgresql+asyncpg://port_admin:PortSecure2024!@localhost:5432/port_iot_db"
)

try:
    engine = create_async_engine(POSTGRES_URL, echo=False)
    AsyncSessionLocal = async_sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )
except Exception as e:
    logger.error(f"Error creating database engine: {e}")
    raise e

Base = declarative_base()

async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
