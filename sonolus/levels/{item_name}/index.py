from fastapi import APIRouter, HTTPException, status

from core import SonolusRequest
from helpers.data_compilers import compile_engines_list
from helpers.level_builder import (
    fetch_music_data,
    get_merged_musics,
    parse_level_id,
    build_level_item,
    build_level_description,
    get_chart_info,
    get_other_difficulties,
    get_other_versions,
    get_same_artist_musics,
)
from helpers.models.sonolus.item_section import LevelItemSection
from helpers.models.sonolus.response import ServerItemDetails

router = APIRouter()


@router.get("", response_model=ServerItemDetails)
async def main(request: SonolusRequest, item_name: str):
    locale = request.state.loc
    api = request.app.api
    source = request.app.base_url

    parsed = parse_level_id(item_name)
    if not parsed:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=locale.not_found,
        )

    music_id, vocal_id, difficulty_name = parsed

    music_data = await fetch_music_data(api)
    musics = get_merged_musics(
        music_data, request.state.show_spoilers, request.state.localization
    )
    engines = await request.app.run_blocking(compile_engines_list, source)

    if not engines:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=locale.not_found,
        )

    engine = engines[0]

    music = next((m for m in musics if m.id == music_id), None)
    if not music:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=locale.item_not_found("Level", item_name),
        )

    vocal = next((v for v in music.vocals if v.id == vocal_id), None)
    if not vocal:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=locale.item_not_found("Level", item_name),
        )

    diff = next(
        (d for d in music.difficulties if d.difficulty == difficulty_name), None
    )
    if not diff:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=locale.item_not_found("Level", item_name),
        )

    chart_info = get_chart_info(music.id, difficulty_name)

    level = build_level_item(
        music=music,
        vocal=vocal,
        difficulty_name=difficulty_name,
        play_level=diff.play_level,
        engine=engine,
        source=source,
        localization=request.state.localization,
        music_data=music_data,
        levelbg=request.state.levelbg,
    )

    description = build_level_description(
        music=music,
        combo=chart_info["combo"],
        duration=chart_info["duration"],
        music_data=music_data,
    )

    sections = []

    other_diff_levels = []
    other_diffs = get_other_difficulties(music, difficulty_name)
    for diff_name, play_level in other_diffs:
        od_level = build_level_item(
            music=music,
            vocal=vocal,
            difficulty_name=diff_name,
            play_level=play_level,
            engine=engine,
            source=source,
            localization=request.state.localization,
            music_data=music_data,
            levelbg=request.state.levelbg,
        )
        other_diff_levels.append(od_level)

    other_ver_levels = []
    other_vocals = get_other_versions(music, vocal_id)
    for other_vocal in other_vocals:
        ov_level = build_level_item(
            music=music,
            vocal=other_vocal,
            difficulty_name=difficulty_name,
            play_level=diff.play_level,
            engine=engine,
            source=source,
            localization=request.state.localization,
            music_data=music_data,
            levelbg=request.state.levelbg,
        )
        other_ver_levels.append(ov_level)

    if other_diff_levels:
        sections.append(
            LevelItemSection(
                title="#OTHER_DIFFICULTIES",
                icon="level",
                items=other_diff_levels,
            )
        )

    if other_ver_levels:
        sections.append(
            LevelItemSection(
                title="#OTHER_VERSIONS",
                icon="level",
                items=other_ver_levels,
            )
        )

    same_artist = get_same_artist_musics(musics, music)
    if same_artist:
        sa_levels = []
        for sa_music in same_artist:
            if not sa_music.vocals or not sa_music.difficulties:
                continue
            sa_vocal = sa_music.vocals[0]
            sa_diff = sa_music.difficulties[-1]
            sa_level = build_level_item(
                music=sa_music,
                vocal=sa_vocal,
                difficulty_name=sa_diff.difficulty,
                play_level=sa_diff.play_level,
                engine=engine,
                source=source,
                localization=request.state.localization,
                music_data=music_data,
                levelbg=request.state.levelbg,
            )
            sa_levels.append(sa_level)
        if sa_levels:
            sections.append(
                LevelItemSection(
                    title="#SAME_AUTHOR",
                    icon="level",
                    items=sa_levels,
                )
            )

    result = ServerItemDetails(
        item=level,
        description=description,
        actions=[],
        hasCommunity=False,
        leaderboards=[],
        sections=sections,
    )
    return result
