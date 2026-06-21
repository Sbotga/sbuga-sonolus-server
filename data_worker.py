import asyncio
import io
import json
import os

import aiohttp
import asyncpg
import aioboto3
from contextlib import asynccontextmanager
from fastapi import FastAPI

from helpers.models.api.music import Music
from helpers.search import build_search_maps, _load_from_disk as load_search_from_disk

MUSIC_CACHE_FILE = "cache/music_data.json"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS chart_data (
    music_id INT NOT NULL,
    difficulty TEXT NOT NULL,
    combo INT NOT NULL,
    duration INT NOT NULL,
    bundle_hash TEXT NOT NULL DEFAULT '',
    converter_version TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (music_id, difficulty)
)
"""

ADD_CONVERTER_VERSION_SQL = """
ALTER TABLE chart_data ADD COLUMN IF NOT EXISTS converter_version TEXT NOT NULL DEFAULT ''
"""

_music_cache: dict[str, list[dict]] = {}
_search_map_data: dict = {}
_chart_info: dict[int, dict[str, dict]] = {}
_bundle_hashes: dict[str, dict[str, str]] = (
    {}
)  # {music_id_str: {jacket, score, long/{abn}, short/{abn}}}
_versions: dict = {}
_CHECK_INTERVAL = 300
_lock = asyncio.Lock()

_api_url: str = ""
_s3_config: dict = {}
_db_pool: asyncpg.Pool | None = None


def _ensure_dirs():
    os.makedirs("cache", exist_ok=True)


def _load_from_disk():
    global _music_cache, _versions, _search_map_data
    _ensure_dirs()

    if os.path.exists(MUSIC_CACHE_FILE):
        try:
            with open(MUSIC_CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            _versions = data.get("versions", {})
            _music_cache = {"en": data["en"], "jp": data["jp"]}
            print(
                f"[DataWorker] loaded music from disk en={len(_music_cache['en'])} jp={len(_music_cache['jp'])}"
            )
        except Exception as e:
            print(f"[DataWorker] failed to load music from disk: {e}")

    if load_search_from_disk():
        from helpers.search import (
            _search_map,
            _vocal_id_map,
            _playlist_map,
            _all_artists,
            _all_captions,
            _min_level,
            _max_level,
        )

        _search_map_data = {
            "search_map": {k: [list(lk) for lk in v] for k, v in _search_map.items()},
            "vocal_id_map": {
                k: [list(lk) for lk in v] for k, v in _vocal_id_map.items()
            },
            "playlist_map": {k: list(v) for k, v in _playlist_map.items()},
            "all_artists": _all_artists,
            "all_captions": _all_captions,
            "min_level": _min_level,
            "max_level": _max_level,
        }


def _save_music_to_disk():
    _ensure_dirs()
    with open(MUSIC_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {
                "versions": _versions,
                "en": _music_cache["en"],
                "jp": _music_cache["jp"],
            },
            f,
            ensure_ascii=False,
        )


def _get_merged_multi(cache: dict[str, list[dict]], regions: list[str]) -> list[Music]:
    # regions in priority order, first region with the music id wins
    region_maps: dict[str, dict[int, Music]] = {}
    for r in regions:
        region_maps[r] = {m["id"]: Music.model_validate(m) for m in cache.get(r, [])}

    all_ids: set[int] = set()
    for rm in region_maps.values():
        all_ids.update(rm.keys())

    merged = []
    for mid in sorted(all_ids):
        for r in regions:
            if mid in region_maps[r]:
                merged.append(region_maps[r][mid])
                break
    return merged


async def _fetch_json(session: aiohttp.ClientSession, url: str) -> dict | list | None:
    async with session.get(url) as resp:
        if resp.status != 200:
            return None
        return await resp.json()


async def _fetch_bytes(session: aiohttp.ClientSession, url: str) -> bytes | None:
    async with session.get(url) as resp:
        if resp.status != 200:
            return None
        return await resp.read()


async def _load_chart_info_from_db():
    global _chart_info
    if not _db_pool:
        return
    async with _db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT music_id, difficulty, combo, duration FROM chart_data"
        )
    _chart_info = {}
    for row in rows:
        mid = row["music_id"]
        if mid not in _chart_info:
            _chart_info[mid] = {}
        _chart_info[mid][row["difficulty"]] = {
            "combo": row["combo"],
            "duration": row["duration"],
        }
    total = sum(len(d) for d in _chart_info.values())
    print(f"[DataWorker] loaded {total} chart infos from db")


# ---- music data + search maps ----


async def _check_and_update_music():
    global _music_cache, _search_map_data, _versions

    from helpers.config_loader import get_config

    all_regions = get_config()["api"]["region-priority"]
    supported = {"en", "jp"}
    regions = [r for r in all_regions if r in supported]
    async with aiohttp.ClientSession() as session:
        version_results = await asyncio.gather(
            *[
                _fetch_json(session, f"{_api_url}/api/pjsk_data/version?region={r}")
                for r in regions
            ]
        )

    new_versions_map = {}
    for r, v in zip(regions, version_results):
        new_versions_map[r] = v["data_version"] if v else ""

    if _music_cache:
        changed = False
        for r in regions:
            old = _versions.get(r, {}).get("data_version", "")
            if new_versions_map[r] and new_versions_map[r] != old:
                changed = True
                break
        if not changed:
            return

    print("[DataWorker] data version changed, fetching music data...")
    async with aiohttp.ClientSession() as session:
        music_results = await asyncio.gather(
            *[
                _fetch_json(
                    session,
                    f"{_api_url}/api/pjsk_data/musics?region={r}&ignore_leak=true&image_type=png",
                )
                for r in regions
            ]
        )

    new_cache = {}
    any_data = False
    for r, data in zip(regions, music_results):
        if data:
            musics = data["musics"] if isinstance(data, dict) else data
            new_cache[r] = musics
            if musics:
                any_data = True
        else:
            new_cache[r] = []

    if not any_data:
        print("[DataWorker] no data received, skipping")
        return

    print("[DataWorker] building search maps...")
    music_data_typed = {
        r: [Music.model_validate(m) for m in new_cache[r]] for r in regions
    }
    merged = _get_merged_multi(new_cache, regions)
    version_dict = {r: new_versions_map[r] for r in regions}
    build_search_maps(merged, music_data_typed, versions=version_dict)

    from helpers.search import (
        _search_map,
        _vocal_id_map,
        _playlist_map,
        _all_artists,
        _all_captions,
        _min_level,
        _max_level,
    )

    new_search_data = {
        "search_map": {k: [list(lk) for lk in v] for k, v in _search_map.items()},
        "vocal_id_map": {k: [list(lk) for lk in v] for k, v in _vocal_id_map.items()},
        "playlist_map": {k: list(v) for k, v in _playlist_map.items()},
        "all_artists": _all_artists,
        "all_captions": _all_captions,
        "min_level": _min_level,
        "max_level": _max_level,
    }

    _music_cache = new_cache
    _search_map_data = new_search_data
    for r in regions:
        _versions.setdefault(r, {})["data_version"] = new_versions_map[r]

    _save_music_to_disk()
    counts = " ".join(f"{r}={len(new_cache[r])}" for r in regions if new_cache[r])
    print(f"[DataWorker] music updated. {counts} maps={len(_search_map)} keys")

    await _check_and_update_charts()


# ---- charts + leveldata ----


async def _check_and_update_charts():
    global _chart_info, _versions
    if not _db_pool:
        return

    if not _music_cache:
        return

    from helpers.config_loader import get_config

    all_regions = get_config()["api"]["region-priority"]
    supported = {"en", "jp"}
    regions = [r for r in all_regions if r in supported]
    merged = _get_merged_multi(_music_cache, regions)

    known_music_ids = {m.id for m in merged}

    # fetch all music bundles (jacket, long, short, score)
    async with aiohttp.ClientSession() as session:
        jp_assetinfo, en_assetinfo = await asyncio.gather(
            _fetch_json(
                session, f"{_api_url}/api/pjsk_data/assetinfo?region=jp&filter=music/"
            ),
            _fetch_json(
                session, f"{_api_url}/api/pjsk_data/assetinfo?region=en&filter=music/"
            ),
        )
    if not jp_assetinfo and not en_assetinfo:
        print("[DataWorker] failed to fetch assetinfo, skipping charts")
        return

    all_bundles: dict[str, dict] = {}
    if en_assetinfo:
        all_bundles.update(en_assetinfo["bundles"])
    if jp_assetinfo:
        all_bundles.update(jp_assetinfo["bundles"])

    # parse bundle names into per-music per-type hashes
    # music/jacket/jacket_s_001 -> music_id=1, type=jacket
    # music/long/0001_01 -> music_id=1, type=long
    # music/short/0001_01 -> music_id=1, type=short
    # music/music_score/0001_01 -> music_id=1, type=score
    TYPE_MAP = {
        "jacket": "jacket",
        "long": "long",
        "short": "short",
        "music_score": "score",
    }
    new_bundle_hashes: dict[str, dict[str, str]] = {}
    bundle_hash_map: dict[int, str] = {}  # score bundle hash for chart processing

    for bundle_name, info in all_bundles.items():
        parts = bundle_name.split("/")
        if len(parts) < 3:
            continue
        bundle_type = TYPE_MAP.get(parts[1])
        if not bundle_type:
            continue
        try:
            music_id = int(parts[2].split("_")[0])
        except (ValueError, IndexError):
            continue
        if music_id not in known_music_ids:
            continue

        mid_str = str(music_id)
        if mid_str not in new_bundle_hashes:
            new_bundle_hashes[mid_str] = {}
        if bundle_type in ("long", "short"):
            new_bundle_hashes[mid_str][f"{bundle_type}/{parts[2]}"] = info["hash"]
        else:
            new_bundle_hashes[mid_str][bundle_type] = info["hash"]

        if bundle_type == "score":
            bundle_hash_map[music_id] = info["hash"]

    _bundle_hashes.clear()
    _bundle_hashes.update(new_bundle_hashes)

    import hashlib

    def _hash_known(bundles: dict[int, str]) -> str:
        if not bundles:
            return ""
        return hashlib.md5(json.dumps(sorted(bundles.items())).encode()).hexdigest()

    from sonolus_converters import __version__ as converter_version

    async with _db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT music_id, difficulty, bundle_hash, converter_version FROM chart_data"
        )
    db_bundle_hashes: dict[int, str] = {}
    db_converter_versions: dict[int, str] = {}
    existing_set: set[tuple[int, str]] = set()
    for r in rows:
        db_bundle_hashes[r["music_id"]] = r["bundle_hash"]
        db_converter_versions[r["music_id"]] = r["converter_version"]
        existing_set.add((r["music_id"], r["difficulty"]))

    changed_music_ids: set[int] = set()
    for music_id, remote_hash in bundle_hash_map.items():
        if db_bundle_hashes.get(music_id) != remote_hash:
            changed_music_ids.add(music_id)
        elif db_converter_versions.get(music_id) != converter_version:
            changed_music_ids.add(music_id)

    if not changed_music_ids:
        combined_hash = _hash_known(bundle_hash_map)
        for r in regions:
            _versions.setdefault(r, {})["assetinfo_hash"] = combined_hash
        return

    to_process: dict[int, dict[str, str]] = {}
    for music in merged:
        if music.id in changed_music_ids:
            to_process[music.id] = {
                d.difficulty: d.chart_url for d in music.difficulties
            }

    all_tasks: list[tuple[int, str, str, str]] = []
    for music_id, diff_urls in to_process.items():
        bh = bundle_hash_map.get(music_id, "")
        for difficulty, url in diff_urls.items():
            all_tasks.append((music_id, difficulty, url, bh))

    total_charts = len(all_tasks)
    print(
        f"[DataWorker] processing {total_charts} charts across {len(to_process)} bundles..."
    )

    from concurrent.futures import ThreadPoolExecutor

    download_sem = asyncio.Semaphore(200)
    upload_sem = asyncio.Semaphore(100)
    thread_pool = ThreadPoolExecutor(max_workers=os.cpu_count() or 4)
    loop = asyncio.get_event_loop()
    processed = 0

    def _convert_chart(chart_bytes: bytes) -> tuple[int, int, bytes]:
        from sonolus_converters import sus
        from sonolus_converters.LevelData import next_sekai

        text = chart_bytes.decode("utf-8")
        score = sus.load(io.StringIO(text))
        combo = score.combo_count
        duration = round(score.duration)
        buf = io.BytesIO()
        next_sekai.export(buf, score, as_compressed=True)
        return combo, duration, buf.getvalue()

    async def process_one(
        http_session: aiohttp.ClientSession,
        bucket,
        music_id: int,
        difficulty: str,
        url: str,
        bh: str,
    ):
        nonlocal processed

        async with download_sem:
            chart_bytes = await _fetch_bytes(http_session, url)

        if not chart_bytes:
            processed += 1
            return

        try:
            combo, duration, ld_bytes = await loop.run_in_executor(
                thread_pool, _convert_chart, chart_bytes
            )

            async with upload_sem:
                await bucket.upload_fileobj(
                    Fileobj=io.BytesIO(ld_bytes),
                    Key=f"leveldata/{music_id}/{difficulty}.gz",
                    ExtraArgs={"ContentType": "application/octet-stream"},
                )

            async with _db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO chart_data (music_id, difficulty, combo, duration, bundle_hash, converter_version)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (music_id, difficulty) DO UPDATE
                    SET combo = $3, duration = $4, bundle_hash = $5, converter_version = $6
                    """,
                    music_id,
                    difficulty,
                    combo,
                    duration,
                    bh,
                    converter_version,
                )

            if music_id not in _chart_info:
                _chart_info[music_id] = {}
            _chart_info[music_id][difficulty] = {"combo": combo, "duration": duration}

        except Exception as e:
            print(f"[DataWorker] chart error {music_id}/{difficulty}: {e}")

        processed += 1
        print(f"[DataWorker] charts {processed}/{total_charts}")

    s3_session = aioboto3.Session(
        aws_access_key_id=_s3_config["access-key-id"],
        aws_secret_access_key=_s3_config["secret-access-key"],
        region_name=_s3_config["location"],
    )
    async with s3_session.resource("s3", endpoint_url=_s3_config["endpoint"]) as s3:
        bucket = await s3.Bucket(_s3_config["bucket-name"])
        async with aiohttp.ClientSession() as http_session:
            await asyncio.gather(
                *[
                    process_one(http_session, bucket, mid, diff, url, bh)
                    for mid, diff, url, bh in all_tasks
                ]
            )

    thread_pool.shutdown(wait=False)

    combined_hash = _hash_known(bundle_hash_map)
    for r in regions:
        _versions.setdefault(r, {})["assetinfo_hash"] = combined_hash
    total = sum(len(d) for d in _chart_info.values())
    print(f"[DataWorker] charts done. {total} total")


# ---- main update loop ----


async def _update():
    await _check_and_update_music()
    await _check_and_update_charts()


async def _periodic_check():
    while True:
        await asyncio.sleep(_CHECK_INTERVAL)
        try:
            async with _lock:
                await _update()
        except Exception as e:
            print(f"[DataWorker] update error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _db_pool

    from helpers.config_loader import get_config

    config = get_config()
    psql = config["psql"]
    _db_pool = await asyncpg.create_pool(
        host=psql["host"],
        user=psql["user"],
        database=psql["database"],
        port=psql["port"],
        password=psql["password"],
    )

    async with _db_pool.acquire() as conn:
        await conn.execute(CREATE_TABLE_SQL)
        await conn.execute(ADD_CONVERTER_VERSION_SQL)

    _load_from_disk()
    await _load_chart_info_from_db()

    async def _initial_update():
        try:
            async with _lock:
                await _update()
        except Exception as e:
            print(f"[DataWorker] initial update error: {e}")

    asyncio.create_task(_initial_update())
    asyncio.create_task(_periodic_check())
    yield

    if _db_pool:
        await _db_pool.close()


app = FastAPI(lifespan=lifespan)


@app.get("/musics")
async def get_musics():
    return _music_cache


@app.get("/search_maps")
async def get_search_maps():
    return _search_map_data


@app.get("/chart_info")
async def get_chart_info_endpoint():
    return _chart_info


@app.get("/bundle_hashes")
async def get_bundle_hashes():
    return _bundle_hashes


@app.get("/versions")
async def get_versions():
    return _versions


def start_data_worker(api_url: str, port: int = 39042):
    global _api_url, _s3_config
    _api_url = api_url

    from helpers.config_loader import get_config

    config = get_config()
    _s3_config = config["s3"]

    import uvicorn

    uvicorn.run(
        "data_worker:app",
        host="127.0.0.1",
        port=port,
        workers=1,
        access_log=False,
    )
