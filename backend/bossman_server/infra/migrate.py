"""
infra.migrate
─────────────
Programmatic Alembic: `bossmand migrate` and server auto-migration both call
`upgrade()`. No alembic.ini is needed — the script location is the
`bossman_server/migrations` package directory.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"


def _config(url: str):  # noqa: ANN202
    from alembic.config import Config

    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS_DIR))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


def upgrade(url: str, revision: str = "head") -> None:
    from alembic import command

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        command.upgrade(_config(url), revision)
        return
    # Called from inside an event loop (notebook, async test): env.py needs its own loop.
    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(command.upgrade, _config(url), revision).result()


async def current_async(url: str) -> str | None:
    """Revision the database is at (None = never migrated)."""
    from alembic.runtime.migration import MigrationContext

    from bossman_server.infra.db import make_engine

    engine = make_engine(url)
    try:
        async with engine.connect() as conn:
            return await conn.run_sync(lambda c: MigrationContext.configure(c).get_current_revision())
    finally:
        await engine.dispose()


async def upgrade_async(url: str, revision: str = "head") -> None:
    # env.py drives an async engine with asyncio.run(); run it off-loop.
    await asyncio.to_thread(upgrade, url, revision)
