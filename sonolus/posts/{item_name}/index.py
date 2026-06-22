from fastapi import APIRouter, HTTPException, status

from core import SonolusRequest
from helpers.data_compilers import compile_engines_list
from helpers.level_builder import fetch_music_data, get_merged_musics, has_music_data
from helpers.models.api.music import Music
from helpers.playlist_builder import (
    _COLLAB_PREFIX,
    build_collaboration_post,
    build_playlist_item,
)
from helpers.models.sonolus.item_section import PlaylistItemSection
from helpers.models.sonolus.response import ServerItemDetails

router = APIRouter()


@router.get("", response_model=ServerItemDetails)
async def main(request: SonolusRequest, item_name: str):
    locale = request.state.loc
    api = request.app.api
    source = request.app.base_url
    localization = request.state.localization

    if not has_music_data():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=locale.data_loading
        )

    music_data = await fetch_music_data(api)
    musics = get_merged_musics(music_data, True, request.state.localization)
    engines = await request.app.run_blocking(compile_engines_list, source, localization)

    if not engines:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=locale.not_found
        )

    engine = engines[0]

    if not item_name.startswith(_COLLAB_PREFIX):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=locale.not_found
        )

    try:
        target_id = int(item_name.removeprefix(_COLLAB_PREFIX))
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=locale.not_found
        )

    collab_musics = [
        m
        for m in musics
        if m.collaboration_id == target_id and m.vocals and m.difficulties
    ]
    if not collab_musics:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=locale.item_not_found("Post", item_name),
        )

    collab_name = collab_musics[0].collaboration

    # filter songs for display based on spoiler setting
    if request.state.show_spoilers:
        visible_songs = collab_musics
    else:
        import time

        now_ms = int(time.time() * 1000)
        visible_songs = [m for m in collab_musics if m.published_at <= now_ms]

    post = build_collaboration_post(
        collab_name=collab_name,
        collab_id=target_id,
        songs=collab_musics,
        source=source,
        spoiler_tag=locale.spoiler,
        songs_count_str=locale.songs_count(len(collab_musics)),
    )

    song_playlists = []
    for music in sorted(visible_songs, key=lambda m: m.published_at, reverse=True):
        sp = await build_playlist_item(
            music=music,
            engine=engine,
            source=source,
            localization=localization,
            music_data=music_data,
            levelbg=request.state.levelbg,
            spoiler_tag=locale.spoiler,
        )
        song_playlists.append(sp)

    sections = []
    if song_playlists:
        sections.append(
            PlaylistItemSection(
                title=collab_name,
                icon="star",
                items=song_playlists,
            )
        )

    return ServerItemDetails(
        item=post,
        description=collab_name,
        actions=[],
        hasCommunity=False,
        leaderboards=[],
        sections=sections,
    )
