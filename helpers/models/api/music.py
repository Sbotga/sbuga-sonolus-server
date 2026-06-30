from pydantic import BaseModel


class MusicArtist(BaseModel):
    id: int
    name: str
    pronunciation: str | None = None


class MusicDifficulty(BaseModel):
    difficulty: str
    play_level: int
    total_note_count: int
    chart_url: str = ""


class GameCharacterData(BaseModel):
    givenName: str = ""
    firstName: str = ""
    unit: str = ""


class OutsideCharacterData(BaseModel):
    name: str = ""


class VocalCharacter(BaseModel):
    character_type: str
    character_id: int
    seq: int


class VocalVariant(BaseModel):
    id: int
    seq: int
    asset_type: str
    assetbundle_name: str
    jacket_url: str | None = None
    background_v1_url: str | None = None
    background_v3_url: str | None = None


class MusicVocal(BaseModel):
    id: int
    vocal_type: str
    caption: str
    characters: list[VocalCharacter]
    assetbundle_name: str
    bgm_url: str | None = None
    bgm_nosil_url: str | None = None
    preview_url: str | None = None
    published_at: int | None = None
    variants: list[VocalVariant] = []


class Music(BaseModel):
    id: int
    title: str
    pronunciation: str | None = None
    title_variants: list[str] = []
    lyricist: str | None = None
    composer: str | None = None
    arranger: str | None = None
    artist: MusicArtist | None = None
    categories: list[str] = []
    tags: list[str] = []
    published_at: int
    released_at: int | None = None
    is_newly_written: bool = False
    is_full_length: bool = False
    filler_sec: float = 0.0
    sec_for_music_score_maker: int | None = None
    jacket_url: str = ""
    background_v1_url: str = ""
    background_v3_url: str = ""
    collaboration: str | None = None
    collaboration_id: int | None = None
    original_video: str | None = None
    difficulties: list[MusicDifficulty] = []
    vocals: list[MusicVocal] = []
    game_characters: dict[int, GameCharacterData] = {}
    outside_characters: dict[int, OutsideCharacterData] = {}


def _is_cjk(text: str) -> bool:
    return any("\u3000" <= c <= "\u9fff" or "\uf900" <= c <= "\ufaff" for c in text)


def _build_char_name(char_data: GameCharacterData) -> str:
    if not char_data.firstName:
        return char_data.givenName
    if _is_cjk(char_data.givenName):
        sep = ""
    else:
        sep = " "
    if char_data.unit == "piapro":
        return f"{char_data.firstName}{sep}{char_data.givenName}"
    return f"{char_data.givenName}{sep}{char_data.firstName}"


def get_vocal_artist(vocal: MusicVocal, music: "Music") -> str:
    if not vocal.characters:
        return vocal.caption
    characters = sorted(vocal.characters, key=lambda c: c.seq)
    names = []
    for c in characters:
        if c.character_type == "game_character":
            char_data = music.game_characters.get(c.character_id)
            names.append(
                _build_char_name(char_data)
                if char_data
                else f"Character {c.character_id}"
            )
        else:
            char_data = music.outside_characters.get(c.character_id)
            names.append(char_data.name if char_data else f"Character {c.character_id}")
    return " & ".join(names)


CAPTION_JP_TO_EN: dict[str, str] = {
    "バーチャル・シンガーver.": "VIRTUAL SINGER ver.",
    "セカイver.": "SEKAI ver.",
    "ワンダーランズ×ショウタイム ver.": "Wonderlands×Showtime ver.",
    "25時、ナイトコードで。ver.": "Nightcord at 25:00 ver.",
    "アナザーボーカルver.": "Cover ver.",
    "Inst.ver.": "Instrumental ver.",
    "エイプリルフールver.": "April Fool's ver.",
    "コネクトライブver.": "Connect Live ver.",
    "コネクトライブ(DAY1夜)ver.": "Connect Live (DAY1 Night) ver.",
    "コネクトライブ(DAY1昼)ver.": "Connect Live (DAY1 Day) ver.",
    "コネクトライブ(DAY2夜)ver.": "Connect Live (DAY2 Night) ver.",
    "コネクトライブ(DAY2昼)ver.": "Connect Live (DAY2 Day) ver.",
    "あんさんぶるスターズ！！コラボver.": "Ensemble Stars!! Crossover ver.",
    "「劇場版プロジェクトセカイ」ver.": "COLORFUL STAGE! The Movie ver.",
}

CAPTION_EN_TO_JP: dict[str, str] = {v: k for k, v in CAPTION_JP_TO_EN.items()}
CAPTION_EN_TO_JP.update(
    {
        "Leo/need ver.": "Leo/need ver.",
        "MORE MORE JUMP! ver.": "MORE MORE JUMP！ ver.",
        "MORE MORE JUMP！ ver.": "MORE MORE JUMP！ ver.",
        "Vivid BAD SQUAD ver.": "Vivid BAD SQUAD ver.",
        "COLORFUL LIVE ver.": "COLORFUL LIVE ver.",
        "English ver.": "English ver.",
    }
)


def translate_caption(caption: str, localization: str) -> str:
    if localization == "ja":
        return CAPTION_EN_TO_JP.get(caption, caption)
    return CAPTION_JP_TO_EN.get(caption, caption)
