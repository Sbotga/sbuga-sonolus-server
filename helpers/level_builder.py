import json
import random
import time

import aiohttp
from helpers.api import SbugaAPI
from helpers.config_loader import get_config as _get_config
from helpers.models.api.music import (
    GameCharacterData,
    Music,
    MusicArtist,
    MusicVocal,
    OutsideCharacterData,
    get_vocal_artist,
    translate_caption,
)
from helpers.search import load_from_response
from helpers.data_compilers import compile_backgrounds_list
from helpers.models.sonolus.item import LevelItem, UseItem, BackgroundItem
from helpers.models.sonolus.misc import SRL, Tag

_chart_info_cache: dict[int, dict[str, dict]] = {}
_bundle_hashes: dict[str, dict[str, str]] = {}
_music_data: dict[str, list[Music]] = {}
_last_version_check: float = 0
_known_version: str = ""
_VERSION_CHECK_INTERVAL = 5


def make_level_id(music_id: int, vocal_id: int, difficulty: str) -> str:
    return f"sss-{music_id}-{vocal_id}-{difficulty}"


def parse_level_id(level_id: str) -> tuple[int, int, str] | None:
    parts = level_id.split("-")
    if len(parts) != 4 or parts[0] != "sss":
        return None
    try:
        return int(parts[1]), int(parts[2]), parts[3]
    except ValueError:
        return None


DATA_WORKER_URL = f"http://127.0.0.1:{_get_config()['server']['data-worker-port']}"
_session: aiohttp.ClientSession | None = None


async def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session


import asyncio

_update_lock = asyncio.Lock()
_updating = False


async def _do_update():
    global _music_data, _known_version, _updating
    _updating = True
    try:
        session = await _get_session()
        async with session.get(f"{DATA_WORKER_URL}/musics") as resp:
            if resp.status != 200:
                return
            raw = await resp.json()

        async with session.get(f"{DATA_WORKER_URL}/search_maps") as resp:
            if resp.status != 200:
                return
            search_data = await resp.json()

        async with session.get(f"{DATA_WORKER_URL}/chart_info") as resp:
            if resp.status != 200:
                return
            new_chart_info = await resp.json()

        async with session.get(f"{DATA_WORKER_URL}/bundle_hashes") as resp:
            if resp.status != 200:
                return
            new_bundle_hashes = await resp.json()

        new_music_data = {
            "en": [Music.model_validate(m) for m in raw["en"]],
            "jp": [Music.model_validate(m) for m in raw["jp"]],
        }

        if search_data:
            load_from_response(search_data)

        _chart_info_cache.clear()
        for mid_str, diffs in new_chart_info.items():
            _chart_info_cache[int(mid_str)] = diffs

        _bundle_hashes.clear()
        _bundle_hashes.update(new_bundle_hashes)

        _music_data = new_music_data

        session = await _get_session()
        async with session.get(f"{DATA_WORKER_URL}/versions") as resp:
            if resp.status == 200:
                new_versions = await resp.json()
                _known_version = json.dumps(new_versions, sort_keys=True)

        print("[SSS Worker] data updated")
    except Exception as e:
        print(f"[SSS Worker] update failed: {e}")
    finally:
        _updating = False


async def fetch_music_data(api: SbugaAPI) -> dict[str, list[Music]]:
    global _last_version_check

    now = time.time()
    if now - _last_version_check < _VERSION_CHECK_INTERVAL:
        return _music_data or {"en": [], "jp": []}

    _last_version_check = now

    try:
        session = await _get_session()
        async with session.get(f"{DATA_WORKER_URL}/versions") as resp:
            if resp.status != 200:
                return _music_data or {"en": [], "jp": []}
            new_versions = await resp.json()
    except Exception:
        return _music_data or {"en": [], "jp": []}

    new_version_str = json.dumps(new_versions, sort_keys=True)
    if new_version_str == _known_version:
        return _music_data or {"en": [], "jp": []}

    if not _music_data:
        async with _update_lock:
            if not _music_data:
                await _do_update()
    elif not _updating:
        asyncio.create_task(_do_update())

    return _music_data or {"en": [], "jp": []}


def get_merged_musics(
    music_data: dict[str, list[Music]],
    show_spoilers: bool,
    localization: str = "en",
) -> list[Music]:
    en_map = {m.id: m for m in music_data.get("en", [])}
    jp_map = {m.id: m for m in music_data.get("jp", [])}

    all_game_chars: dict[int, GameCharacterData] = {}
    all_outside_chars: dict[int, OutsideCharacterData] = {}
    all_artists: dict[int, MusicArtist] = {}
    if localization != "ja":
        for m in music_data.get("en", []):
            for cid, cdata in m.game_characters.items():
                all_game_chars[cid] = cdata
            for cid, cdata in m.outside_characters.items():
                all_outside_chars[cid] = cdata
            if m.artist:
                all_artists[m.artist.id] = m.artist

    all_ids = set(en_map.keys()) | set(jp_map.keys())
    merged: list[Music] = []

    now_ms = int(time.time() * 1000)
    for mid in sorted(all_ids):
        music = jp_map.get(mid) or en_map.get(mid)
        if not music:
            continue
        if not show_spoilers and music.published_at > now_ms:
            continue
        if not show_spoilers:
            music = music.model_copy()
            music.vocals = [
                v
                for v in music.vocals
                if v.published_at is None or v.published_at <= now_ms
            ]
            if not music.vocals:
                continue

        if localization != "ja":
            music = music.model_copy()
            if mid in en_map:
                en_music = en_map[mid]
                music.game_characters = en_music.game_characters
                music.outside_characters = en_music.outside_characters
                if en_music.artist:
                    music.artist = en_music.artist
            else:
                music.game_characters = {
                    cid: all_game_chars[cid]
                    for cid in music.game_characters
                    if cid in all_game_chars
                }
                music.outside_characters = {
                    cid: all_outside_chars[cid]
                    for cid in music.outside_characters
                    if cid in all_outside_chars
                }
                if music.artist and music.artist.id in all_artists:
                    music.artist = all_artists[music.artist.id]

        merged.append(music)

    return merged


def get_display_title(
    music_id: int,
    music_data: dict[str, list[Music]],
    localization: str,
) -> str:
    en_map = {m.id: m for m in music_data.get("en", [])}
    jp_map = {m.id: m for m in music_data.get("jp", [])}

    en_music = en_map.get(music_id)
    jp_music = jp_map.get(music_id)

    if localization == "ja":
        if jp_music:
            return jp_music.title
        if en_music:
            return en_music.title
        return f"Music {music_id}"
    else:
        if en_music:
            return en_music.title
        if jp_music:
            return jp_music.title
        return f"Music {music_id}"


def get_display_artist(
    music_id: int,
    music_data: dict[str, list[Music]],
    localization: str,
) -> str | None:
    en_map = {m.id: m for m in music_data.get("en", [])}
    jp_map = {m.id: m for m in music_data.get("jp", [])}

    en_music = en_map.get(music_id)
    jp_music = jp_map.get(music_id)

    if localization == "ja":
        if jp_music and jp_music.artist:
            return jp_music.artist.name
        if en_music and en_music.artist:
            return en_music.artist.name
        return None
    else:
        if en_music and en_music.artist:
            return en_music.artist.name
        if jp_music and jp_music.artist:
            return jp_music.artist.name
        return None


def get_chart_info(music_id: int, difficulty: str) -> dict:
    return _chart_info_cache.get(music_id, {}).get(
        difficulty, {"combo": 0, "duration": 0}
    )


def get_leveldata_url(music_id: int, difficulty: str) -> str:
    config = _get_config()
    s3_base = config["s3"]["base-url"].rstrip("/")
    return f"{s3_base}/leveldata/{music_id}/{difficulty}.gz"


def _srl(
    url: str, music_id: int, asset_suffix: str, bundle_key: str | None = None
) -> SRL:
    key = bundle_key or asset_suffix
    h = _bundle_hashes.get(str(music_id), {}).get(key)
    return SRL(hash=f"{h}-{asset_suffix}" if h else None, url=url)


def invalidate_chart_cache():
    _chart_info_cache.clear()


def format_duration(seconds: float | int) -> str:
    total = round(seconds)
    mins = total // 60
    secs = total % 60
    return f"{mins}:{secs:02d}"


def _get_all_title_variants(
    music: Music,
    music_data: dict[str, list[Music]] | None,
) -> list[str]:
    seen: set[str] = set()
    variants: list[str] = []
    for source_list in (music_data or {}).values():
        for m in source_list:
            if m.id != music.id:
                continue
            for v in [m.title, m.pronunciation, *m.title_variants]:
                if v and v not in seen:
                    seen.add(v)
                    variants.append(v)
    for v in [music.title, music.pronunciation, *music.title_variants]:
        if v and v not in seen:
            seen.add(v)
            variants.append(v)
    return variants


def build_level_description(
    music: Music,
    combo: int,
    duration: float,
    music_data: dict[str, list[Music]] | None = None,
) -> str:
    lines = []

    if music.artist:
        lines.append(f"#AUTHOR:#SEPARATOR_COLON:{music.artist.name}")
        lines.append("")
    if music.lyricist:
        lines.append(f"#LYRICIST:#SEPARATOR_COLON:{music.lyricist}")
    if music.composer:
        lines.append(f"#COMPOSER:#SEPARATOR_COLON:{music.composer}")
    if music.arranger:
        lines.append(f"#ARRANGER:#SEPARATOR_COLON:{music.arranger}")

    lines.append("")

    length_sec = (
        music.sec_for_music_score_maker
        if music.sec_for_music_score_maker
        else round(duration)
    )
    if length_sec:
        lines.append(f"#LENGTH:#SEPARATOR_COLON: {format_duration(length_sec)}")
    if combo:
        lines.append(f"#COMBO:#SEPARATOR_COLON: {combo:,}")

    variants = _get_all_title_variants(music, music_data)
    if variants:
        lines.append("")
        lines.append(" ".join(variants))

    return "\n".join(lines)


def build_level_item(
    music: Music,
    vocal: MusicVocal,
    difficulty_name: str,
    play_level: int,
    engine,
    source: str,
    localization: str = "en",
    music_data: dict[str, list[Music]] | None = None,
    levelbg: str = "v3",
) -> LevelItem:
    level_id = make_level_id(music.id, vocal.id, difficulty_name)
    artist = get_vocal_artist(vocal, music)

    title = music.title
    if music_data:
        title = get_display_title(music.id, music_data, localization)

    jacket_variant = next(
        (v for v in vocal.variants if v.asset_type == "jacket" and v.jacket_url), None
    )
    cover_url = (
        jacket_variant.jacket_url if jacket_variant else (music.jacket_url or "")
    )

    if levelbg == "v3":
        bg_image_url = (
            jacket_variant.background_v3_url
            if jacket_variant and jacket_variant.background_v3_url
            else music.background_v3_url
        )
    elif levelbg == "v1":
        bg_image_url = (
            jacket_variant.background_v1_url
            if jacket_variant and jacket_variant.background_v1_url
            else music.background_v1_url
        )
    else:
        bg_image_url = ""

    backgrounds = compile_backgrounds_list(source, include_hidden=True)
    template_bg = next((b for b in backgrounds if b.name == "pjsk_template"), None)

    use_bg = UseItem(useDefault=True)
    if template_bg and bg_image_url:
        use_bg = UseItem(
            useDefault=False,
            item=BackgroundItem(
                name=f"sss-bg-{music.id}-{vocal.id}-{levelbg}",
                source=source,
                title=f"PJSK {levelbg.upper()}",
                subtitle=title,
                author="",
                tags=[],
                thumbnail=_srl(cover_url, music.id, "jacket"),
                data=template_bg.data,
                image=_srl(bg_image_url, music.id, f"bg{levelbg}", "jacket"),
                configuration=template_bg.configuration,
            ),
        )

    bgm_url = vocal.bgm_nosil_url or vocal.bgm_url or ""
    preview_url = vocal.preview_nosil_url or vocal.preview_url or ""

    return LevelItem(
        name=level_id,
        source=source,
        rating=play_level,
        title=title,
        artists=artist,
        author=(
            "HATSUNE MIKU: COLORFUL STAGE!"
            if localization == "en"
            else "プロジェクトセカイ カラフルステージ！ feat. 初音ミク"
        ),
        tags=[
            Tag(title=difficulty_name.capitalize()),
            Tag(title=translate_caption(vocal.caption, localization)),
        ],
        engine=engine.to_engine_item(),
        useSkin=UseItem(useDefault=True),
        useBackground=use_bg,
        useEffect=UseItem(useDefault=True),
        useParticle=UseItem(useDefault=True),
        cover=_srl(cover_url, music.id, "jacket"),
        bgm=_srl(bgm_url, music.id, "long", f"long/{vocal.assetbundle_name}"),
        preview=_srl(preview_url, music.id, "short", f"short/{vocal.assetbundle_name}"),
        data=_srl(get_leveldata_url(music.id, difficulty_name), music.id, "score"),
    )


def get_other_difficulties(
    music: Music, current_difficulty: str
) -> list[tuple[str, int]]:
    result = []
    for diff in music.difficulties:
        if diff.difficulty != current_difficulty:
            result.append((diff.difficulty, diff.play_level))
    result.sort(key=lambda x: x[1], reverse=True)
    return result


def get_other_versions(music: Music, current_vocal_id: int) -> list[MusicVocal]:
    return [v for v in music.vocals if v.id != current_vocal_id]


def get_same_artist_musics(
    musics: list[Music], current_music: Music, limit: int = 5
) -> list[Music]:
    if not current_music.artist:
        return []
    same = [
        m
        for m in musics
        if m.artist
        and m.artist.id == current_music.artist.id
        and m.id != current_music.id
    ]
    if len(same) > limit:
        same = random.sample(same, limit)
    return same
