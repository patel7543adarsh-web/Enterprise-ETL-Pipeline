"""
Enterprise Data Warehouse SQLAlchemy Connection & Session Management.
Supports PostgreSQL, Snowflake, and SQLite (local dev/testing) with connection pooling and health checks.
"""

import logging
from contextlib import contextmanager
from typing import Generator, Optional
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import NullPool, QueuePool, StaticPool

from config.settings import WarehouseSettings, get_settings

logger = logging.getLogger("warehouse.connection")

Base = declarative_base()


class WarehouseConnectionManager:
    """Manages SQLAlchemy 2.0 Engine and Session lifecycles for the Data Warehouse."""

    def __init__(
        self,
        settings: Optional[WarehouseSettings] = None,
        custom_url: Optional[str] = None,
    ):
        self.settings = settings or get_settings().warehouse
        self.custom_url = custom_url
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None

    @property
    def engine(self) -> Engine:
        """Lazily instantiates and returns the configured SQLAlchemy Engine."""
        if self._engine is None:
            self._engine = self.create_engine_instance()
            self._session_factory = sessionmaker(bind=self._engine, autoflush=False, autocommit=False)
        return self._engine

    def create_engine_instance(self) -> Engine:
        """Creates an optimized SQLAlchemy Engine based on dialect and settings."""
        url = self.custom_url or self.settings.get_connection_url()
        logger.info(f"Initializing Data Warehouse Engine with dialect: {self.settings.db_type}")

        if url.startswith("sqlite"):
            # SQLite configuration (in-memory or file-based)
            is_memory = ":memory:" in url or url == "sqlite://"
            if is_memory:
                engine = create_engine(
                    url,
                    connect_args={"check_same_thread": False},
                    poolclass=StaticPool,
                    echo=self.settings.echo_sql,
                )
            else:
                engine = create_engine(
                    url,
                    connect_args={"check_same_thread": False},
                    pool_pre_ping=True,
                    echo=self.settings.echo_sql,
                )
        elif url.startswith("postgresql"):
            engine = create_engine(
                url,
                pool_size=self.settings.pool_size,
                max_overflow=self.settings.max_overflow,
                pool_timeout=self.settings.pool_timeout,
                pool_pre_ping=True,
                echo=self.settings.echo_sql,
            )
        elif url.startswith("snowflake"):
            engine = create_engine(
                url,
                poolclass=NullPool,
                echo=self.settings.echo_sql,
            )
        else:
            engine = create_engine(
                url,
                pool_pre_ping=True,
                echo=self.settings.echo_sql,
            )

        return engine

    def ping(self) -> bool:
        """Verifies database connectivity with a health check query."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database health check ping succeeded.")
            return True
        except Exception as exc:
            logger.error(f"Database health check failed: {exc}")
            return False

    @contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """Transactional scope context manager that commits or rolls back automatically."""
        if self._session_factory is None:
            _ = self.engine
        session: Session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception as exc:
            session.rollback()
            logger.error(f"Transaction rolled back due to error: {exc}")
            raise
        finally:
            session.close()

    def get_dialect_name(self) -> str:
        """Returns the normalized dialect name ('postgresql', 'sqlite', 'snowflake')."""
        return self.engine.dialect.name.lower()


def get_warehouse_engine(
    settings: Optional[WarehouseSettings] = None,
    custom_url: Optional[str] = None,
) -> Engine:
    """Convenience factory function to get a configured SQLAlchemy Engine."""
    manager = WarehouseConnectionManager(settings=settings, custom_url=custom_url)
    return manager.engine
