import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from helpers.config_loader import set_config_path, get_config

import asyncpg


async def main():
    config_path = sys.argv[1]
    set_config_path(config_path)
    config = get_config()

    from sonolus_converters import __version__ as converter_version

    pool = await asyncpg.create_pool(
        host=config["psql"]["host"],
        user=config["psql"]["user"],
        database=config["psql"]["database"],
        password=config["psql"]["password"],
        port=config["psql"]["port"],
    )

    async with pool.acquire() as conn:
        await conn.execute(
            "ALTER TABLE chart_data ADD COLUMN IF NOT EXISTS converter_version TEXT NOT NULL DEFAULT ''"
        )
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM chart_data WHERE converter_version != $1",
            converter_version,
        )
        result = await conn.execute(
            "UPDATE chart_data SET bundle_hash = '' WHERE converter_version != $1",
            converter_version,
        )
        print(
            f"marked {count} charts for regeneration (current converter: {converter_version})"
        )
        print(f"db: {result}")

    await pool.close()
    print("done. restart sss data worker to regenerate charts in-place.")


asyncio.run(main())
