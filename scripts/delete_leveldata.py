import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from helpers.config_loader import set_config_path, get_config

import asyncpg
import aioboto3


async def main():
    config_path = sys.argv[1]
    set_config_path(config_path)
    config = get_config()

    pool = await asyncpg.create_pool(
        host=config["psql"]["host"],
        user=config["psql"]["user"],
        database=config["psql"]["database"],
        password=config["psql"]["password"],
        port=config["psql"]["port"],
    )

    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM chart_data")
        print(f"db: {result}")

    await pool.close()

    s3_config = config["s3"]
    session = aioboto3.Session(
        aws_access_key_id=s3_config["access-key-id"],
        aws_secret_access_key=s3_config["secret-access-key"],
        region_name=s3_config["location"],
    )
    async with session.resource("s3", endpoint_url=s3_config["endpoint"]) as s3:
        bucket = await s3.Bucket(s3_config["bucket-name"])
        deleted = 0
        batch: list[dict[str, str]] = []
        async for obj in bucket.objects.filter(Prefix="leveldata/"):
            batch.append({"Key": obj.key})
            if len(batch) == 1000:
                await bucket.delete_objects(Delete={"Objects": batch})
                deleted += len(batch)
                batch = []
        if batch:
            await bucket.delete_objects(Delete={"Objects": batch})
            deleted += len(batch)
        print(f"s3: deleted {deleted} objects")

    print("done. all level data deleted.")


asyncio.run(main())
