from __future__ import annotations

import hashlib
import json
import os
import pickle
import re
import time
import unicodedata
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock, local
from typing import Any, Optional
from urllib.parse import urljoin

import pytz
import requests
from bs4 import BeautifulSoup
from ics import Calendar, DisplayAlarm, Event
from ics.grammar.parse import ContentLine

FLASHSCORE_URLS = [
    "https://www.flashscore.es/equipo/arenteiro/lWjqRFF0/partidos/",
    "https://www.flashscore.es/equipo/as-pontes/p4Tn3Zjq/partidos/",
    "https://www.flashscore.es/equipo/cartagena/IJyXDZ6C/partidos/",
    "https://www.flashscore.es/equipo/cartagena/d0OMU795/partidos/",
    "https://www.flashscore.es/equipo/cartagena-fc-ucam/fDwAJjmi/partidos/",
    "https://www.flashscore.es/equipo/celta-vigo/8pvUZFhf/partidos/",
    "https://www.flashscore.es/equipo/celta-vigo/GQPCyIeG/partidos/",
    "https://www.flashscore.es/equipo/coruxo/4O7Ik005/partidos/",
    "https://www.flashscore.es/equipo/deportivo-de-la-coruna/Q51ZzMS6/partidos/",
    "https://www.flashscore.es/equipo/deportivo-de-la-coruna/2aEkkrUp/partidos/",
    "https://www.flashscore.es/equipo/deportivo-fabril/EeCjPcis/partidos/",
    "https://www.flashscore.es/equipo/espana/bLyo6mco/partidos/",
    "https://www.flashscore.es/equipo/espana/t6OJR9UG/partidos/",
    "https://www.flashscore.es/equipo/espana/GSClRmaT/partidos/",
    "https://www.flashscore.es/equipo/fc-barcelona/SKbpVP5K/partidos/",
    "https://www.flashscore.es/equipo/fc-barcelona/YFBOvfnB/partidos/",
    "https://www.flashscore.es/equipo/feirense/ImnWBOMq/partidos/",
    "https://www.flashscore.es/equipo/feirense/t4p0iC41/partidos/",
    "https://www.flashscore.es/equipo/gran-pena/8EJDcWmn/partidos/",
    "https://www.flashscore.es/equipo/lugo/IJTeVFYq/partidos/",
    "https://www.flashscore.es/equipo/oporto/S2NmScGp/partidos/",
    "https://www.flashscore.es/equipo/ourense-cf/M9KnSe0f/partidos/",
    "https://www.flashscore.es/equipo/pontevedra/tpBOiyM5/partidos/",
    "https://www.flashscore.es/equipo/racing-de-ferrol/zcgwCMgS/partidos/",
    "https://www.flashscore.es/equipo/rapido-de-bouzas/AXqPPdgM/partidos/",
    "https://www.flashscore.es/equipo/real-madrid/W8mj7MDD/partidos/",
    "https://www.flashscore.es/equipo/real-madrid/4xlfVfnP/partidos/",
    "https://www.flashscore.es/equipo/valladares/8vubFQgt/partidos/",
    "https://www.flashscore.es/equipo/new-mexico-state/dOFseDcf/partidos/",
    "https://www.flashscore.es/equipo/cartagena/nuvYqDSI/partidos/",
    "https://www.flashscore.es/equipo/celta/pMyy6Tm6/partidos/",
    "https://www.flashscore.es/equipo/espana/OKERpL3q/partidos/",
    "https://www.flashscore.es/equipo/espana/6mbFVIw7/partidos/",
    "https://www.flashscore.es/equipo/espana/AgqYhHP4/partidos/",
    "https://www.flashscore.es/equipo/espana/4pfW5f9A/partidos/",
    "https://www.flashscore.es/equipo/los-angeles-lakers/ngegZ8bg/partidos/",
    "https://www.flashscore.es/equipo/new-mexico-state/xz9AG67f/partidos/",
    "https://www.flashscore.es/equipo/espana/MyYCrJtq/partidos/",
    "https://www.flashscore.es/equipo/espana/YaeNI5Fb/partidos/",
    "https://www.flashscore.es/equipo/cartagena/KIGhbjj8/partidos/",
    "https://www.flashscore.es/equipo/espana/2LhuBBUR/partidos/",
    "https://www.flashscore.es/jugador/alcaraz-garfia-carlos/UkhgIFEq/partidos/",
    "https://www.flashscore.es/jugador/badosa-paula/Wl76rX3I/partidos/",
    "https://www.flashscore.es/jugador/bautista-agut-roberto/riOJC5jb/partidos/",
    "https://www.flashscore.es/jugador/bouzas-maneiro-jessica/Gj20DdaG/partidos/",
    "https://www.flashscore.es/jugador/bucsa-cristina/fkFeyKHL/partidos/",
    "https://www.flashscore.es/jugador/carballes-baena-roberto/hj8fIZPg/partidos/",
    "https://www.flashscore.es/jugador/carreno-busta-pablo/W8WmcXdq/partidos/",
    "https://www.flashscore.es/jugador/davidovich-fokina-alejandro/0zQXLfz4/partidos/",
    "https://www.flashscore.es/jugador/de-minaur-alex/EZgZ9Xfh/partidos/",
    "https://www.flashscore.es/jugador/granollers-marcel/2B5Hvd1l/partidos/",
    "https://www.flashscore.es/jugador/jodar-rafael/OdngshVK/partidos/",
    "https://www.flashscore.es/jugador/landaluce-martin/AoOnGZEp/partidos/",
    "https://www.flashscore.es/jugador/martinez-pedro/WtiN88eD/partidos/",
    "https://www.flashscore.es/jugador/munar-jaume/zZieQm4D/partidos/",
    "https://www.flashscore.es/jugador/marin-carolina/jDfGrcm3/partidos/",
    "https://www.flashscore.es/jugador/ayora-angel/6HeCjvlb/",
    "https://www.flashscore.es/jugador/garcia-sergio/fRIZumKj/",
    "https://www.flashscore.es/jugador/rahm-jon/h41jCwka/",
    "https://www.flashscore.es/equipo/espana/bVHRrnee/partidos/",
    "https://www.flashscore.es/equipo/espana/6HSNrXx5/partidos/",
    "https://www.flashscore.es/jugador/robles-alvaro/zJqPFUWM/partidos/",
    "https://www.flashscore.es/jugador/xiao-maria/E3RdqzEe/partidos/",
    "https://www.flashscore.es/jugador/alvarez-mendoza-daniela/EDIHogcC/partidos/",
    "https://www.flashscore.es/jugador/fernandez-steiner-liliana/hW03wyYe/partidos/",
    "https://www.flashscore.es/jugador/gavira-adrian/bLBUASHU/partidos/",
    "https://www.flashscore.es/jugador/herrera-pablo/hCbHU9Xb/partidos/",
    "https://www.flashscore.es/jugador/moreno-matveeva-tania/KphYIs6m/partidos/",
    "https://www.flashscore.es/jugador/soria-paula/WpAF0HHc/partidos/",
    "https://www.flashscore.es/equipo/espana/2FUgemIm/partidos/",
    "https://www.flashscore.es/equipo/espana/Ol3yUR1i/partidos/",
    "https://www.flashscore.es/jugador/ghadfa-drissi-el-aissaoui-ayoub/xprZPZhL/partidos/",
    "https://www.flashscore.es/jugador/alonso-fernando/rVRhR90U/partidos/",
    "https://www.flashscore.es/jugador/acosta-pedro/KrGeaHq5/partidos/",
    "https://www.flashscore.es/jugador/aldeguer-fermin/rJIs1ByA/partidos/",
    "https://www.flashscore.es/jugador/ayuso-juan/vBkjjMHF/partidos/",
    "https://www.flashscore.es/jugador/carrasco-ana/QkiT86Ok/partidos/",
    "https://www.flashscore.es/jugador/garzo-hector/tpGe3W6o/partidos/",
    "https://www.flashscore.es/jugador/marquez-marc/UPhfij2b/partidos/",
    "https://www.flashscore.es/jugador/martin-jorge/zBBka6dR/partidos/",
    "https://www.flashscore.es/jugador/molina-miguel/8649E4Zn/partidos/",
    "https://www.flashscore.es/jugador/palou-alex/KCYsXBsC/partidos/",
    "https://www.flashscore.es/jugador/sainz-carlos-jr/l6kEzjTF/partidos/",
    "https://www.flashscore.es/jugador/sainz-carlos-sr/KOjcVmad/partidos/",
    "https://www.flashscore.es/jugador/sordo-dani/QHgmhx5C/partidos/",
    "https://www.flashscore.es/jugador/marti-pepe/bH3206EA/partidos/",
    "https://www.flashscore.es/jugador/lozano-serrano-rafael/KAW8M61s/partidos/",
    "https://www.flashscore.es/jugador/topuria-ilia/bBTa66GB/partidos/",
]
FUTBOLERAS_TEAM_URLS = [
    url.strip()
    for url in os.environ.get(
        "FUTBOLERAS_TEAM_URLS",
        (
            "https://www.futboleras.es/equipo/as-celtas-ref2315.html,"
            "https://www.futboleras.es/equipo/as-celtas-b-ref2405.html"
        ),
    ).split(",")
    if url.strip()
]

OUTPUT_DIR = Path(os.environ.get("FLASHSCORE_OUTPUT_DIR", "/var/www/html"))
OBSOLETE_FILE = OUTPUT_DIR / "obsolete.pkl"
PICKLE_FILE = OUTPUT_DIR / "SportsCalendar.pkl"
CALENDAR_FILE = OUTPUT_DIR / "SportsCalendar.ics"
CLASSIFICATION_CACHE_FILE = OUTPUT_DIR / "classification_cache.pkl"
MATCH_DETAIL_URL_TEMPLATE = "https://www.flashscore.es/partido/{gameid}/"

REQUEST_TIMEOUT_SECONDS = 20
CLASSIFICATION_FETCH_TIMEOUT_SECONDS = 20
SCRAPE_MAX_WORKERS = max(1, int(os.environ.get("FLASHSCORE_SCRAPE_MAX_WORKERS", "12")))
CLASSIFICATION_MAX_WORKERS = max(1, int(os.environ.get("FLASHSCORE_CLASSIFICATION_MAX_WORKERS", "8")))
FEED_MARKER = "cjs.initialFeeds"
FIELD_PATTERN = re.compile(r"([A-Za-z0-9]{1,4})÷(.*?)¬")
PAYLOAD_PATTERN = re.compile(r"`([^`]*)`", re.DOTALL)
ENVIRONMENT_ASSIGN_PATTERN = re.compile(r"window\.environment\s*=\s*")
LV_PATTERN = re.compile(r"LV÷(\{[^}]+\})_(.*?)¬", re.DOTALL)
TOKEN_PATTERN = re.compile(r"\{[^}]+\}")
CORE_SCRIPT_SRC_PATTERN = re.compile(r"/x/js/core_(\d+)_\d+\.js")
PARTICIPANT_ID_PATTERN = re.compile(r'participant_id\s*=\s*"([^"]+)"')
PARTICIPANT_URL_ID_PATTERN = re.compile(r"/jugador/[^/]+/([^/]+)/", re.IGNORECASE)
PROJECT_ID_PATTERN = re.compile(r"project_id\s*=\s*(\d+)")
FEED_SIGN_PATTERN = re.compile(r"var\s+feed_sign\s*=\s*['\"]([^'\"]+)['\"]")
FEED_SIGN_CONFIG_PATTERN = re.compile(r'"feed_sign":"([^"]+)"')
SPORT_SLUG_PATTERN = re.compile(r'sport\s*=\s*"([^"]+)"')
SPORT_ID_PATTERN = re.compile(r"sportId\s*:\s*(\d+)")
SOFASCORE_TEAM_ID_PATTERN = re.compile(r"/team/[^/?#]+/(\d+)(?:[/?#]|$)")
SOFASCORE_TEAM_SLUG_PATTERN = re.compile(r"/team/([^/?#]+)/\d+(?:[/?#]|$)")
UTC = pytz.UTC
PAST_RESULTS_DAYS = int(os.environ.get("FLASHSCORE_PAST_RESULTS_DAYS", "30"))
LEAGUE_FALLBACK_TEXT = os.environ.get("FLASHSCORE_LEAGUE_FALLBACK", "").strip()
CLASSIFICATION_CACHE_TTL_DAYS = int(os.environ.get("FLASHSCORE_CLASSIFICATION_CACHE_TTL_DAYS", "14"))
CLASSIFICATION_REFRESH_EMPTY_CACHE = os.environ.get("FLASHSCORE_CLASSIFICATION_REFRESH_EMPTY_CACHE", "1") == "1"
CLASSIFICATION_SKIP_FETCH_WHEN_PRESENT = os.environ.get("FLASHSCORE_CLASSIFICATION_SKIP_FETCH_WHEN_PRESENT", "1") == "1"
INCLUDE_MOTORSPORT_SESSIONS = os.environ.get("FLASHSCORE_INCLUDE_MOTORSPORT_SESSIONS", "1") == "1"
MOTORSPORT_SPORT_SLUGS = {"motorsport-auto-racing", "motorsport-moto-racing"}
MOTORSPORT_POSITION_FIELD_KEYS = ("CX", "WS", "NI")
TEAM_STANDINGS_DEFAULT_VIEW_ID = "1"
FEED_CONTEXT_KEYS = ("SA", "ZEE", "ZHS", "ZL", "ZY", "ZB", "ZAF")
INDIVIDUAL_COMPETITION_MERGE_SPORTS = {
    sport.strip().upper()
    for sport in os.environ.get(
        "FLASHSCORE_INDIVIDUAL_MERGE_SPORTS",
        "AUTOMOVILISMO,MOTOCICLISMO,CICLISMO",
    ).split(",")
    if sport.strip()
}
CLASSIFICATION_SPORTS = {
    sport.strip().upper()
    for sport in os.environ.get(
        "FLASHSCORE_CLASSIFICATION_SPORTS",
        "TENIS,TENIS DE MESA,BÁDMINTON,BADMINTON",
    ).split(",")
    if sport.strip()
}
TEAM_CLASSIFICATION_SPORTS = {
    sport.strip().upper()
    for sport in os.environ.get(
        "FLASHSCORE_TEAM_CLASSIFICATION_SPORTS",
        "FÚTBOL,FUTBOL,FÚTBOL SALA,FUTBOL SALA",
    ).split(",")
    if sport.strip()
}

# Manual competition fixes for events where Flashscore feed omits league data.
KNOWN_COMPETITIONS_BY_GAMEID = {
    "IsMJlFgn": "ESPAÑA: Copa del Rey",
    "U5yeEbb2": "ESPAÑA: Copa del Rey",
    "4GvSQdM5": "ATP - INDIVIDUALES: Doha (Catar), dura",
}

KNOWN_COMPETITIONS_BY_MATCHUP = {
    ("FÚTBOL SALA", "CARTAGENA", "MOVISTAR INTER"): "ESPAÑA: Copa del Rey",
    ("TENIS", "ALCARAZ C.", "RINDERKNECH A."): "ATP - INDIVIDUALES: Doha (Catar), dura",
}

CORE_FEED_SIGN_CACHE: dict[str, str] = {}
MOTORSPORT_TOURNAMENT_FEED_CACHE: dict[str, list[tuple[dict[str, str], str]]] = {}
TEAM_STANDINGS_FEED_CACHE: dict[str, dict[str, str]] = {}
FUTBOLERAS_PAGE_CACHE: dict[str, str] = {}
FUTBOLERAS_STANDINGS_CACHE: dict[str, dict[str, str]] = {}
CORE_FEED_SIGN_CACHE_LOCK = Lock()
MOTORSPORT_TOURNAMENT_FEED_CACHE_LOCK = Lock()
TEAM_STANDINGS_FEED_CACHE_LOCK = Lock()
FUTBOLERAS_PAGE_CACHE_LOCK = Lock()
FUTBOLERAS_STANDINGS_CACHE_LOCK = Lock()
SOFASCORE_API_BASE_URLS = [
    base_url.strip()
    for base_url in os.environ.get(
        "SOFASCORE_API_BASE_URLS",
        "https://www.sofascore.com/api/v1,https://api.sofascore.com/api/v1",
    ).split(",")
    if base_url.strip()
]
SOFASCORE_FETCH_PAGES = max(1, int(os.environ.get("SOFASCORE_FETCH_PAGES", "3")))
SOFASCORE_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Referer": "https://www.sofascore.com/",
    "Origin": "https://www.sofascore.com",
    "X-Requested-With": "XMLHttpRequest",
    "Sec-Fetch-Site": "same-site",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
}
SOFASCORE_PAGE_HEADERS = {
    "User-Agent": SOFASCORE_DEFAULT_HEADERS["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": SOFASCORE_DEFAULT_HEADERS["Accept-Language"],
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}
SOFASCORE_SPORT_NAME_MAP = {
    "football": "Fútbol",
    "futsal": "Fútbol Sala",
    "basketball": "Baloncesto",
    "tennis": "Tenis",
    "handball": "Balonmano",
    "ice hockey": "Hockey Hielo",
    "volleyball": "Voleibol",
    "baseball": "Béisbol",
    "american football": "Fútbol Americano",
}
COUNTRY_NAME_OVERRIDES = {
    "SPAIN": "ESPAÑA",
}
SOFASCORE_DEFAULT_TIMEZONE = os.environ.get("SOFASCORE_DEFAULT_TIMEZONE", "Europe/Madrid")
SOFASCORE_USE_CURL_CFFI = os.environ.get("SOFASCORE_USE_CURL_CFFI", "1") == "1"
SOFASCORE_CURL_CFFI_IMPERSONATE = os.environ.get("SOFASCORE_CURL_CFFI_IMPERSONATE", "chrome124")
OVERRUN_EXTENSION_MINUTES = max(5, int(os.environ.get("FLASHSCORE_OVERRUN_EXTENSION_MINUTES", "30")))
OVERRUN_EXTENSION_MAX_HOURS = max(1, int(os.environ.get("FLASHSCORE_OVERRUN_MAX_HOURS", "12")))
OVERRUN_EXTENSION_SPORTS = {
    sport.strip().upper()
    for sport in os.environ.get(
        "FLASHSCORE_OVERRUN_EXTENSION_SPORTS",
        "AUTOMOVILISMO,MOTOCICLISMO,CICLISMO",
    ).split(",")
    if sport.strip()
}
SOFASCORE_NEXT_DATA_PATTERN = re.compile(
    r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)
SOFASCORE_JSON_SCRIPT_PATTERN = re.compile(
    r'<script[^>]+type=["\']application/(?:ld\+)?json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)
SOFASCORE_INITIAL_STATE_PATTERN = re.compile(
    r"window\.__INITIAL_STATE__\s*=\s*(\{.*?\});",
    re.DOTALL,
)
SOFASCORE_LISTING_SCORE_PATTERN = re.compile(
    r"(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+"
    r"(?P<time>\d{1,2}:\d{2})\s+"
    r"(?P<home>.+?)\s+"
    r"(?P<score_home>\d+)\s*-\s*(?P<score_away>\d+)\s+"
    r"(?:(?:Finalizado|Finalizado tras tiempo extra|Cancelado|Aplazado|Suspendido)\s+)?"
    r"(?P<away>.+)$",
    re.IGNORECASE,
)
SOFASCORE_LISTING_FIXTURE_PATTERN = re.compile(
    r"(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+"
    r"(?P<time>\d{1,2}:\d{2})\s+"
    r"(?P<home>.+?)\s+\u2013\s+(?P<away>.+)$",
    re.IGNORECASE,
)
SOFASCORE_CURL_CFFI_MODULE: Any = None
SOFASCORE_CURL_CFFI_IMPORT_ATTEMPTED = False
SOFASCORE_CURL_CFFI_IMPORT_WARNED = False
FUTBOLERAS_SEASON_PATTERN = re.compile(r"LIGA-(\d{4})-(\d{4})-", re.IGNORECASE)
FUTBOLERAS_DATE_TIME_PATTERN = re.compile(
    r"J(?P<journey>\d+)\s+.+?\s+(?P<day>\d{1,2})/(?P<month>\d{1,2})\s+(?P<time>\d{2}:\d{2}|--:--)",
    re.IGNORECASE,
)
FUTBOLERAS_SCORE_PATTERN = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s*$")
FUTBOLERAS_DEFAULT_TIMEZONE = os.environ.get("FUTBOLERAS_DEFAULT_TIMEZONE", "Europe/Madrid")
FUTBOLERAS_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}
FIRST_LEG_PATTERNS = (
    re.compile(r"resultado del primer partido[:\s-]*(.+)", re.IGNORECASE),
    re.compile(r"resultado de la ida[:\s-]*(.+)", re.IGNORECASE),
    re.compile(r"(?:partido de )?ida[:\s-]*(.+)", re.IGNORECASE),
    re.compile(r"first leg[:\s-]*(.+)", re.IGNORECASE),
)
SCORE_TEXT_PATTERN = re.compile(r"(\d+)\s*[-:–]\s*(\d+)")
TWO_LEG_COMPETITION_HINTS = (
    "COPA",
    "PLAYOFF",
    "PLAY-OFF",
    "CHAMPIONS",
    "EUROPA",
    "CONFERENCE",
    "SUPERCOPA",
    "ELIMINATORIA",
    "QUALIFICATION",
    "QUALIFIER",
)


def load_pickle(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with path.open("rb") as handle:
            return pickle.load(handle)
    except (pickle.UnpicklingError, OSError, EOFError) as exc:
        print(f"No se pudo cargar {path}: {exc}")
        return default


def save_pickle(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        pickle.dump(value, handle)


def normalize_sofascore_team_id(url: str) -> str:
    match = SOFASCORE_TEAM_ID_PATTERN.search(url)
    if match:
        return match.group(1).strip()
    return ""


def normalize_sofascore_team_slug(url: str) -> str:
    match = SOFASCORE_TEAM_SLUG_PATTERN.search(url)
    if match:
        return match.group(1).strip()
    return ""


def extract_sofascore_team_name(url: str) -> str:
    slug = normalize_sofascore_team_slug(url)
    if slug:
        return slug.replace("-", " ").title()
    return "Equipo"


def extract_sofascore_team_name_from_soup(soup: BeautifulSoup, fallback_name: str) -> str:
    heading = soup.find("h1")
    if heading:
        heading_text = heading.get_text(strip=True)
        if heading_text:
            return heading_text

    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title:
        content = str(og_title.get("content", "")).strip()
        if content:
            if " - " in content:
                return content.split(" - ", 1)[0].strip()
            return content

    return fallback_name


def iter_json_nodes(node: Any) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    stack: list[Any] = [node]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            nodes.append(current)
            stack.extend(current.values())
            continue
        if isinstance(current, list):
            stack.extend(current)
    return nodes


def extract_json_strings_from_html(html: str) -> list[str]:
    json_chunks: list[str] = []
    for pattern in (SOFASCORE_NEXT_DATA_PATTERN, SOFASCORE_JSON_SCRIPT_PATTERN, SOFASCORE_INITIAL_STATE_PATTERN):
        for match in pattern.finditer(html):
            chunk = match.group(1).strip()
            if chunk:
                json_chunks.append(chunk)
    return json_chunks


def looks_like_sofascore_event(node: dict[str, Any]) -> bool:
    start_value = node.get("startTimestamp")
    if isinstance(start_value, str):
        if not start_value.isdigit():
            return False
    elif not isinstance(start_value, (int, float)):
        return False

    if not str(node.get("id", "")).strip():
        return False

    home_team = node.get("homeTeam")
    away_team = node.get("awayTeam")
    return isinstance(home_team, dict) or isinstance(away_team, dict)


def build_sofascore_fallback_event_id(
    source_url: str,
    start_ts: int,
    team1: str,
    team2: str,
    league: str,
) -> str:
    event_key = "|".join([source_url, str(start_ts), team1, team2, league])
    digest = hashlib.sha1(event_key.encode("utf-8")).hexdigest()[:16]
    return f"ss_fb_{digest}"


def parse_sofascore_listing_timestamp(date_text: str, time_text: str) -> Optional[int]:
    raw_datetime = f"{date_text.strip()} {time_text.strip()}"
    try:
        parsed_dt = datetime.strptime(raw_datetime, "%d/%m/%Y %H:%M")
    except ValueError:
        return None

    try:
        timezone = pytz.timezone(SOFASCORE_DEFAULT_TIMEZONE)
    except pytz.UnknownTimeZoneError:
        timezone = UTC

    localized = timezone.localize(parsed_dt)
    return int(localized.astimezone(UTC).timestamp())


def extract_sofascore_events_from_listing_text(
    soup: BeautifulSoup,
    source_url: str,
    fallback_team_name: str,
) -> dict[str, dict[str, Any]]:
    events: dict[str, dict[str, Any]] = {}
    text_lines = [line.strip() for line in soup.stripped_strings if line.strip()]
    if not text_lines:
        return events

    competition_hint = ""
    for line in text_lines:
        if ":" in line or "," in line:
            if any(keyword in line.lower() for keyword in ("federación", "liga", "cup", "copa", "primera", "segunda")):
                competition_hint = line
                break

    for line in text_lines:
        score_match = SOFASCORE_LISTING_SCORE_PATTERN.match(line)
        fixture_match = SOFASCORE_LISTING_FIXTURE_PATTERN.match(line)
        match = score_match or fixture_match
        if not match:
            continue

        start_ts = parse_sofascore_listing_timestamp(match.group("date"), match.group("time"))
        if start_ts is None:
            continue

        home_team = match.group("home").strip()
        away_team = match.group("away").strip()
        if not home_team:
            home_team = fallback_team_name
        if not home_team:
            continue

        event: dict[str, Any] = {
            "gameid": build_sofascore_fallback_event_id(source_url, start_ts, home_team, away_team, competition_hint),
            "league": competition_hint,
            "team1": home_team,
            "date": start_ts,
            "sports": "Fútbol",
            "status": "CONFIRMED",
            "url": source_url,
        }
        if away_team:
            event["team2"] = away_team

        if score_match:
            event["score_home"] = int(score_match.group("score_home"))
            event["score_away"] = int(score_match.group("score_away"))

        events[event["gameid"]] = event

    return events


def get_curl_cffi_requests_module() -> Any:
    global SOFASCORE_CURL_CFFI_IMPORT_ATTEMPTED
    global SOFASCORE_CURL_CFFI_MODULE
    global SOFASCORE_CURL_CFFI_IMPORT_WARNED

    if not SOFASCORE_USE_CURL_CFFI:
        return None
    if SOFASCORE_CURL_CFFI_IMPORT_ATTEMPTED:
        return SOFASCORE_CURL_CFFI_MODULE

    SOFASCORE_CURL_CFFI_IMPORT_ATTEMPTED = True
    try:
        from curl_cffi import requests as curl_cffi_requests
    except ImportError:
        if not SOFASCORE_CURL_CFFI_IMPORT_WARNED:
            print(
                "curl_cffi no está disponible para SofaScore. "
                "Instala con: python3 -m pip install curl_cffi"
            )
            SOFASCORE_CURL_CFFI_IMPORT_WARNED = True
        SOFASCORE_CURL_CFFI_MODULE = None
        return None

    SOFASCORE_CURL_CFFI_MODULE = curl_cffi_requests
    return SOFASCORE_CURL_CFFI_MODULE


def build_http_error(status_code: int, url: str) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    response.url = url
    return requests.HTTPError(f"{status_code} Client Error: status {status_code} for url: {url}", response=response)


def sofascore_curl_cffi_get(url: str, headers: dict[str, str], timeout_seconds: int) -> Any:
    curl_cffi_requests = get_curl_cffi_requests_module()
    if curl_cffi_requests is None:
        return None

    try:
        return curl_cffi_requests.get(
            url,
            headers=headers,
            impersonate=SOFASCORE_CURL_CFFI_IMPERSONATE,
            timeout=timeout_seconds,
        )
    except Exception:
        return None


def sofascore_get_response(
    session: requests.Session,
    url: str,
    headers: dict[str, str],
    timeout_seconds: int,
) -> Any:
    request_error: Optional[Exception] = None
    try:
        response = session.get(url, headers=headers, timeout=timeout_seconds)
        if response.status_code != 403:
            return response
    except requests.RequestException as exc:
        request_error = exc
    else:
        # 403 with requests: try curl_cffi before returning the blocked response.
        request_error = build_http_error(403, url)

    fallback_response = sofascore_curl_cffi_get(url, headers, timeout_seconds)
    if fallback_response is not None:
        return fallback_response

    if request_error:
        raise request_error
    raise build_http_error(0, url)


def fetch_sofascore_team_page(session: requests.Session, url: str) -> str:
    try:
        response = sofascore_get_response(
            session=session,
            url=url,
            headers=SOFASCORE_PAGE_HEADERS,
            timeout_seconds=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        print(f"No se pudo abrir página de equipo SofaScore {url}: {exc}")
        return ""

    status_code = int(getattr(response, "status_code", 0))
    if status_code >= 400:
        if status_code != 403:
            print(f"No se pudo abrir página de equipo SofaScore {url}: HTTP {status_code}")
        return ""
    return str(getattr(response, "text", ""))


def extract_sofascore_events_from_embedded_json(
    html: str,
    source_url: str,
    fallback_team_name: str,
) -> dict[str, dict[str, Any]]:
    events: dict[str, dict[str, Any]] = {}
    for chunk in extract_json_strings_from_html(html):
        try:
            payload = json.loads(chunk)
        except json.JSONDecodeError:
            continue

        for node in iter_json_nodes(payload):
            if not looks_like_sofascore_event(node):
                continue

            event = build_sofascore_event(node, source_url, fallback_team_name)
            if not event:
                continue

            gameid = event["gameid"]
            if gameid not in events:
                events[gameid] = event
            else:
                events[gameid] = merge_event_payload(events[gameid], event)
    return events


def is_http_forbidden_error(exc: Exception) -> bool:
    if not isinstance(exc, requests.HTTPError):
        return False
    response = exc.response
    if response is None:
        return False
    return response.status_code == 403


def extract_name_field(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("name", "shortName", "alternateName", "text", "@id"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
    return ""


def normalize_country_name(country_name: str) -> str:
    normalized = country_name.strip()
    if not normalized:
        return ""
    upper_name = normalized.upper()
    return COUNTRY_NAME_OVERRIDES.get(upper_name, upper_name)


def extract_sofascore_competition(event_obj: dict[str, Any]) -> str:
    tournament = event_obj.get("tournament")
    competition_name = extract_name_field(tournament)
    category_name = ""
    if isinstance(tournament, dict):
        category_name = extract_name_field(tournament.get("category"))

    country_text = normalize_country_name(category_name)
    if country_text and competition_name:
        if ":" in competition_name:
            return competition_name
        return f"{country_text}: {competition_name}"
    if competition_name:
        return competition_name
    return country_text


def extract_sofascore_score_value(raw_value: Any) -> Optional[int]:
    if isinstance(raw_value, dict):
        for key in ("current", "display", "normaltime", "extra", "value", "score", "text"):
            nested = raw_value.get(key)
            parsed = extract_sofascore_score_value(nested)
            if parsed is not None:
                return parsed
        return None
    if isinstance(raw_value, list):
        for item in raw_value:
            parsed = extract_sofascore_score_value(item)
            if parsed is not None:
                return parsed
        return None
    if isinstance(raw_value, (int, float)):
        return int(raw_value)
    if raw_value is None:
        return None

    match = re.search(r"\d+", str(raw_value))
    if not match:
        return None
    return int(match.group(0))


def extract_sofascore_tv_channels(event_obj: dict[str, Any]) -> list[str]:
    channel_nodes = event_obj.get("tvChannels")
    if not isinstance(channel_nodes, list):
        return []

    channels: list[str] = []
    for node in channel_nodes:
        channel_name = extract_name_field(node)
        if channel_name:
            channels.append(channel_name)
    return list(dict.fromkeys(channels))


def normalize_sofascore_sport_name(event_obj: dict[str, Any]) -> str:
    sport_name = extract_name_field(event_obj.get("sport"))
    if not sport_name:
        return "Fútbol"
    return SOFASCORE_SPORT_NAME_MAP.get(sport_name.strip().lower(), sport_name.strip())


def build_sofascore_event_url(event_obj: dict[str, Any], source_url: str) -> str:
    event_id = str(event_obj.get("id", "")).strip()
    event_slug = str(event_obj.get("slug", "")).strip()
    custom_id = str(event_obj.get("customId", "")).strip()
    if event_slug and custom_id and event_id:
        return f"https://www.sofascore.com/es/football/match/{event_slug}/{custom_id}#id:{event_id}"
    if event_id:
        return f"https://www.sofascore.com/es/event/{event_id}"
    return source_url


def build_sofascore_event(
    event_obj: dict[str, Any],
    source_url: str,
    fallback_team_name: str,
) -> Optional[dict[str, Any]]:
    event_id = str(event_obj.get("id", "")).strip()
    start_raw = str(event_obj.get("startTimestamp", "")).strip()
    if not event_id or not start_raw.isdigit():
        return None

    team1 = extract_name_field(event_obj.get("homeTeam"))
    team2 = extract_name_field(event_obj.get("awayTeam"))
    if not team1 and fallback_team_name:
        team1 = fallback_team_name
    if not team1:
        return None

    event: dict[str, Any] = {
        "gameid": f"ss_{event_id}",
        "league": extract_sofascore_competition(event_obj),
        "team1": team1,
        "date": int(start_raw),
        "sports": normalize_sofascore_sport_name(event_obj),
        "status": "CONFIRMED",
        "url": build_sofascore_event_url(event_obj, source_url),
    }

    if team2:
        event["team2"] = team2

    end_raw = str(event_obj.get("endTimestamp", "")).strip()
    if end_raw.isdigit():
        end_ts = int(end_raw)
        if end_ts > event["date"]:
            event["date_end"] = end_ts

    score_home = extract_sofascore_score_value(event_obj.get("homeScore"))
    score_away = extract_sofascore_score_value(event_obj.get("awayScore"))
    if score_home is not None and score_away is not None:
        event["score_home"] = score_home
        event["score_away"] = score_away

    status = event_obj.get("status")
    if isinstance(status, dict):
        status_text = extract_name_field(status)
        status_type = str(status.get("type", "")).strip().lower()
        if status_text:
            event["result_status"] = status_text
        if status_type in {"canceled", "cancelled"}:
            event["status"] = "CANCELLED"

    tv_channels = extract_sofascore_tv_channels(event_obj)
    if tv_channels:
        event["tv"] = tv_channels

    return event


def fetch_sofascore_events_by_bucket(
    session: requests.Session,
    team_id: str,
    bucket: str,
    source_url: str,
) -> list[dict[str, Any]]:
    last_error: Optional[Exception] = None

    for base_url in SOFASCORE_API_BASE_URLS:
        all_events: list[dict[str, Any]] = []
        try_next_base = False
        try:
            for page in range(SOFASCORE_FETCH_PAGES):
                api_url = f"{base_url}/team/{team_id}/events/{bucket}/{page}"
                request_headers = SOFASCORE_DEFAULT_HEADERS.copy()
                request_headers["Referer"] = source_url
                response = sofascore_get_response(
                    session=session,
                    url=api_url,
                    headers=request_headers,
                    timeout_seconds=REQUEST_TIMEOUT_SECONDS,
                )
                status_code = int(getattr(response, "status_code", 0))

                if status_code == 404:
                    if page == 0:
                        try_next_base = True
                    break

                if status_code >= 400:
                    raise build_http_error(status_code, api_url)

                payload = response.json() if hasattr(response, "json") else {}
                page_events = payload.get("events", [])
                if not isinstance(page_events, list) or not page_events:
                    break

                all_events.extend(node for node in page_events if isinstance(node, dict))

                has_next_page = payload.get("hasNextPage")
                if has_next_page is False:
                    break
        except (requests.RequestException, json.JSONDecodeError) as exc:
            last_error = exc
            continue

        if try_next_base:
            continue

        return all_events

    if last_error:
        raise last_error
    return []


def scrape_sofascore_events(urls: list[str]) -> tuple[dict[str, dict[str, Any]], int]:
    if not urls:
        return {}, 0

    gamelist: dict[str, dict[str, Any]] = {}
    failed_urls = 0

    with requests.Session() as session:
        for url in urls:
            team_id = normalize_sofascore_team_id(url)
            if not team_id:
                print(f"No se pudo extraer el team id de SofaScore para {url}")
                failed_urls += 1
                continue

            fallback_team_name = extract_sofascore_team_name(url)
            team_page_html = fetch_sofascore_team_page(session, url)

            team_page_soup: Optional[BeautifulSoup] = None
            if team_page_html:
                team_page_soup = BeautifulSoup(team_page_html, "html.parser")
                fallback_team_name = extract_sofascore_team_name_from_soup(team_page_soup, fallback_team_name)

            extracted_events: dict[str, dict[str, Any]] = {}
            bucket_errors: list[tuple[str, Exception]] = []
            used_html_fallback = False

            for bucket in ("last", "next"):
                try:
                    bucket_events = fetch_sofascore_events_by_bucket(session, team_id, bucket, url)
                except Exception as exc:
                    bucket_errors.append((bucket, exc))
                    continue

                for node in bucket_events:
                    event = build_sofascore_event(node, url, fallback_team_name)
                    if not event:
                        continue
                    gameid = event["gameid"]
                    if gameid not in extracted_events:
                        extracted_events[gameid] = event
                    else:
                        extracted_events[gameid] = merge_event_payload(extracted_events[gameid], event)

            if not extracted_events and team_page_html:
                embedded_events = extract_sofascore_events_from_embedded_json(team_page_html, url, fallback_team_name)
                if embedded_events:
                    used_html_fallback = True
                    for gameid, event in embedded_events.items():
                        if gameid not in extracted_events:
                            extracted_events[gameid] = event
                        else:
                            extracted_events[gameid] = merge_event_payload(extracted_events[gameid], event)

            if not extracted_events and team_page_soup is not None:
                text_events = extract_sofascore_events_from_listing_text(team_page_soup, url, fallback_team_name)
                if text_events:
                    used_html_fallback = True
                    for gameid, event in text_events.items():
                        if gameid not in extracted_events:
                            extracted_events[gameid] = event
                        else:
                            extracted_events[gameid] = merge_event_payload(extracted_events[gameid], event)

            had_forbidden_errors = any(is_http_forbidden_error(exc) for _, exc in bucket_errors)
            had_non_forbidden_errors = any(not is_http_forbidden_error(exc) for _, exc in bucket_errors)

            if bucket_errors and had_forbidden_errors and used_html_fallback:
                print(f"SofaScore API bloqueada (403) para {url}; se aplicó fallback HTML.")
            elif bucket_errors:
                for bucket, exc in bucket_errors:
                    print(f"No se pudo obtener SofaScore {url} ({bucket}): {exc}")

            if not extracted_events:
                print(f"SofaScore sin eventos parseables para {url}")
                failed_urls += 1
                continue

            if had_non_forbidden_errors or (had_forbidden_errors and not used_html_fallback):
                failed_urls += 1

            for gameid, event in extracted_events.items():
                if gameid not in gamelist:
                    gamelist[gameid] = event
                else:
                    gamelist[gameid] = merge_event_payload(gamelist[gameid], event)

    print(
        "Diagnóstico SofaScore: "
        f"urls={len(urls)}, fallidas={failed_urls}, eventos={len(gamelist)}"
    )
    return gamelist, failed_urls


def extract_futboleras_team_name(soup: BeautifulSoup, url: str) -> str:
    team_heading = soup.find(id="teamName")
    if team_heading:
        team_name = team_heading.get_text(" ", strip=True)
        if team_name:
            return team_name

    title = soup.find("title")
    if title and title.text:
        title_text = title.text.strip()
        if " - " in title_text:
            return title_text.split(" - ", 1)[0].strip()
        return title_text

    slug = url.rsplit("/", 1)[-1].replace(".html", "")
    slug = re.sub(r"-ref\d+$", "", slug, flags=re.IGNORECASE)
    return slug.replace("-", " ").strip().title() or "Equipo"


def extract_futboleras_competition_context(
    soup: BeautifulSoup,
    team_name: str,
) -> tuple[str, Optional[int], Optional[int], str]:
    competition_name = ""
    season_start_year: Optional[int] = None
    season_end_year: Optional[int] = None
    competition_id = ""

    competition_select = soup.find("select", id="teamStatistics2TeamCompetitionSelect")
    if competition_select:
        selected_option = competition_select.find("option", selected=True) or competition_select.find("option")
        if selected_option:
            option_text = selected_option.get_text(" ", strip=True)
            option_value = str(selected_option.get("value", "")).strip()
            if option_value:
                competition_id = option_value
            season_match = FUTBOLERAS_SEASON_PATTERN.search(option_value)
            if season_match:
                season_start_year = int(season_match.group(1))
                season_end_year = int(season_match.group(2))
            if option_text:
                competition_name = option_text

    if not competition_name:
        results_title = soup.find("h2", class_=re.compile(r"resultsTitle", re.IGNORECASE))
        if results_title:
            title_text = results_title.get_text(" ", strip=True)
            title_text = re.sub(r"\s+", " ", title_text).strip()
            upper_prefix = "RESULTADOS "
            if title_text.upper().startswith(upper_prefix):
                title_text = title_text[len(upper_prefix):].strip()
            team_upper = team_name.upper()
            if title_text.upper().startswith(f"{team_upper} "):
                title_text = title_text[len(team_name):].strip()
            competition_name = title_text

    competition_name = normalize_league(competition_name)
    if competition_name and ":" not in competition_name:
        competition_name = f"ESPAÑA: {competition_name}"

    return competition_name, season_start_year, season_end_year, competition_id


def infer_futboleras_year(
    month: int,
    season_start_year: Optional[int],
    season_end_year: Optional[int],
) -> int:
    if season_start_year and season_end_year:
        return season_start_year if month >= 7 else season_end_year

    now_utc = datetime.now(tz=UTC)
    inferred_year = now_utc.year
    if now_utc.month <= 6 and month >= 7:
        inferred_year -= 1
    elif now_utc.month >= 7 and month <= 6:
        inferred_year += 1
    return inferred_year


def parse_futboleras_datetime(
    date_label: str,
    season_start_year: Optional[int],
    season_end_year: Optional[int],
) -> tuple[Optional[int], bool, str]:
    normalized_label = re.sub(r"\s+", " ", date_label.replace("\xa0", " ")).strip()
    match = FUTBOLERAS_DATE_TIME_PATTERN.search(normalized_label)
    if not match:
        return None, False, ""

    journey_text = match.group("journey").strip()
    day = int(match.group("day"))
    month = int(match.group("month"))
    time_text = match.group("time").strip()

    year = infer_futboleras_year(month, season_start_year, season_end_year)
    all_day = time_text == "--:--"
    if all_day:
        hour, minute = 12, 0
    else:
        hour, minute = (int(part) for part in time_text.split(":", 1))

    try:
        timezone = pytz.timezone(FUTBOLERAS_DEFAULT_TIMEZONE)
    except pytz.UnknownTimeZoneError:
        timezone = UTC

    try:
        localized_dt = timezone.localize(datetime(year, month, day, hour, minute))
    except ValueError:
        return None, False, journey_text

    utc_timestamp = int(localized_dt.astimezone(UTC).timestamp())
    return utc_timestamp, all_day, journey_text


def build_futboleras_event_id(
    event_url: str,
    date_ts: int,
    home_team: str,
    away_team: str,
    competition_name: str,
) -> str:
    external_match_id = ""
    match = re.search(r"-ref(\d+)\\.html", event_url, flags=re.IGNORECASE)
    if match:
        external_match_id = match.group(1).strip()
    if external_match_id:
        return f"fu_{external_match_id}"

    event_key = "|".join([event_url, str(date_ts), home_team, away_team, competition_name])
    digest = hashlib.sha1(event_key.encode("utf-8")).hexdigest()[:16]
    return f"fu_{digest}"


def build_futboleras_event(
    item: Any,
    source_url: str,
    competition_name: str,
    season_start_year: Optional[int],
    season_end_year: Optional[int],
    fallback_team_name: str,
) -> Optional[dict[str, Any]]:
    if not hasattr(item, "select_one"):
        return None

    date_node = item.select_one(".resultsTopDate")
    if not date_node:
        return None

    date_text = date_node.get_text(" ", strip=True)
    event_ts, all_day, journey = parse_futboleras_datetime(date_text, season_start_year, season_end_year)
    if event_ts is None:
        return None

    home_team = ""
    away_team = ""
    home_node = item.select_one(".resultsLocalTeam")
    away_node = item.select_one(".resultsVisitorTeam")
    if home_node:
        home_team = home_node.get_text(" ", strip=True)
    if away_node:
        away_team = away_node.get_text(" ", strip=True)

    if not home_team and fallback_team_name:
        home_team = fallback_team_name
    if not home_team:
        return None

    href = str(item.get("href", "")).strip()
    event_url = urljoin(source_url, href) if href else source_url
    gameid = build_futboleras_event_id(event_url, event_ts, home_team, away_team, competition_name)

    event: dict[str, Any] = {
        "gameid": gameid,
        "league": competition_name,
        "team1": home_team,
        "date": event_ts,
        "sports": "Fútbol",
        "status": "CONFIRMED",
        "url": event_url,
    }

    if away_team:
        event["team2"] = away_team

    if all_day:
        event["all_day"] = True

    goals_node = item.select_one(".resultsGoals")
    if goals_node:
        score_text = goals_node.get_text(" ", strip=True)
        score_match = FUTBOLERAS_SCORE_PATTERN.search(score_text)
        if score_match:
            event["score_home"] = int(score_match.group(1))
            event["score_away"] = int(score_match.group(2))

    status_node = item.select_one(".resultsTopStatus")
    if status_node:
        status_text = status_node.get_text(" ", strip=True)
        if status_text:
            event["result_status"] = status_text
            lower_status = status_text.lower()
            if any(keyword in lower_status for keyword in ("cancel", "aplaz", "suspend")):
                event["status"] = "CANCELLED"

    if journey:
        event["journey"] = journey

    return event


def extract_futboleras_events_from_soup(soup: BeautifulSoup, source_url: str) -> dict[str, dict[str, Any]]:
    events: dict[str, dict[str, Any]] = {}
    team_name = extract_futboleras_team_name(soup, source_url)
    competition_name, season_start_year, season_end_year, _competition_id = extract_futboleras_competition_context(
        soup, team_name
    )

    result_items = soup.select("a.resultsItemOddWrapper, a.resultsItemEvenWrapper")
    for item in result_items:
        event = build_futboleras_event(
            item=item,
            source_url=source_url,
            competition_name=competition_name,
            season_start_year=season_start_year,
            season_end_year=season_end_year,
            fallback_team_name=team_name,
        )
        if not event:
            continue

        gameid = event["gameid"]
        if gameid not in events:
            events[gameid] = event
        else:
            events[gameid] = merge_event_payload(events[gameid], event)

    return events


def normalize_futboleras_team_key(team_name: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(team_name))
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = normalized.upper()
    normalized = re.sub(r"[^A-Z0-9]+", " ", normalized)
    return normalized.strip()


def extract_futboleras_related_team_urls(soup: BeautifulSoup, source_url: str) -> list[str]:
    related_urls: list[str] = []
    for link in soup.select("#relatedTeamsItemsWrapper a[href]"):
        href = str(link.get("href", "")).strip()
        if not href or "/equipo/" not in href:
            continue
        team_url = urljoin(source_url, href)
        if team_url not in related_urls:
            related_urls.append(team_url)

    if source_url not in related_urls:
        related_urls.insert(0, source_url)
    return related_urls


def fetch_futboleras_page_html(session: requests.Session, url: str) -> str:
    with FUTBOLERAS_PAGE_CACHE_LOCK:
        cached_html = FUTBOLERAS_PAGE_CACHE.get(url)
    if cached_html is not None:
        return cached_html

    response = session.get(
        url,
        headers=FUTBOLERAS_DEFAULT_HEADERS,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    page_html = response.text

    with FUTBOLERAS_PAGE_CACHE_LOCK:
        FUTBOLERAS_PAGE_CACHE[url] = page_html
    return page_html


def compute_futboleras_rank_map(events: dict[str, dict[str, Any]]) -> dict[str, str]:
    standings: dict[str, dict[str, int]] = {}
    for event in events.values():
        team1 = str(event.get("team1", "")).strip()
        team2 = str(event.get("team2", "")).strip()
        score_home = event.get("score_home")
        score_away = event.get("score_away")

        if not team1 or not team2:
            continue
        if not isinstance(score_home, int) or not isinstance(score_away, int):
            continue
        if str(event.get("status", "")).upper() == "CANCELLED":
            continue

        team1_stats = standings.setdefault(team1, {"points": 0, "goal_diff": 0, "goals_for": 0, "played": 0})
        team2_stats = standings.setdefault(team2, {"points": 0, "goal_diff": 0, "goals_for": 0, "played": 0})

        team1_stats["played"] += 1
        team2_stats["played"] += 1
        team1_stats["goal_diff"] += score_home - score_away
        team2_stats["goal_diff"] += score_away - score_home
        team1_stats["goals_for"] += score_home
        team2_stats["goals_for"] += score_away

        if score_home > score_away:
            team1_stats["points"] += 3
        elif score_home < score_away:
            team2_stats["points"] += 3
        else:
            team1_stats["points"] += 1
            team2_stats["points"] += 1

    ordered_teams = sorted(
        standings.items(),
        key=lambda item: (
            -item[1]["points"],
            -item[1]["goal_diff"],
            -item[1]["goals_for"],
            normalize_futboleras_team_key(item[0]),
        ),
    )

    rank_map: dict[str, str] = {}
    previous_signature: Optional[tuple[int, int, int]] = None
    current_rank = 0
    for index, (team_name, stats) in enumerate(ordered_teams, start=1):
        if stats["played"] <= 0:
            continue
        signature = (stats["points"], stats["goal_diff"], stats["goals_for"])
        if signature != previous_signature:
            current_rank = index
            previous_signature = signature
        rank_map[normalize_futboleras_team_key(team_name)] = f"#{current_rank}"

    return rank_map


def build_futboleras_competition_rank_map(
    session: requests.Session,
    source_url: str,
    source_soup: BeautifulSoup,
    competition_id: str,
) -> dict[str, str]:
    cache_key = competition_id.strip() or source_url
    with FUTBOLERAS_STANDINGS_CACHE_LOCK:
        cached_map = FUTBOLERAS_STANDINGS_CACHE.get(cache_key)
    if cached_map is not None:
        return cached_map

    all_events: dict[str, dict[str, Any]] = {}
    related_team_urls = extract_futboleras_related_team_urls(source_soup, source_url)
    fetched_pages: dict[str, str] = {}

    if len(related_team_urls) <= 1:
        for team_url in related_team_urls:
            try:
                page_html = fetch_futboleras_page_html(session, team_url)
            except requests.RequestException as exc:
                print(f"No se pudo obtener Futboleras {team_url}: {exc}")
                continue
            fetched_pages[team_url] = page_html
    else:
        thread_local_session = local()
        worker_sessions: list[requests.Session] = []
        worker_sessions_lock = Lock()

        def fetch_team_page(worker_url: str) -> tuple[str, str]:
            worker_session = getattr(thread_local_session, "session", None)
            if worker_session is None:
                worker_session = requests.Session()
                thread_local_session.session = worker_session
                with worker_sessions_lock:
                    worker_sessions.append(worker_session)
            page_html = fetch_futboleras_page_html(worker_session, worker_url)
            return worker_url, page_html

        max_workers = min(SCRAPE_MAX_WORKERS, len(related_team_urls))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_team_page, team_url): team_url for team_url in related_team_urls}
            for future in as_completed(futures):
                team_url = futures[future]
                try:
                    fetched_url, page_html = future.result()
                except requests.RequestException as exc:
                    print(f"No se pudo obtener Futboleras {team_url}: {exc}")
                    continue
                except Exception as exc:
                    print(f"No se pudo obtener Futboleras {team_url}: {exc}")
                    continue
                fetched_pages[fetched_url] = page_html

        for worker_session in worker_sessions:
            worker_session.close()

    for team_url, page_html in fetched_pages.items():

        team_soup = BeautifulSoup(page_html, "html.parser")
        team_name = extract_futboleras_team_name(team_soup, team_url)
        _competition_name, _start_year, _end_year, team_competition_id = extract_futboleras_competition_context(
            team_soup, team_name
        )
        if competition_id and team_competition_id and team_competition_id != competition_id:
            continue

        team_events = extract_futboleras_events_from_soup(team_soup, team_url)
        for gameid, event in team_events.items():
            if gameid not in all_events:
                all_events[gameid] = event
            else:
                all_events[gameid] = merge_event_payload(all_events[gameid], event)

    rank_map = compute_futboleras_rank_map(all_events)
    with FUTBOLERAS_STANDINGS_CACHE_LOCK:
        FUTBOLERAS_STANDINGS_CACHE[cache_key] = rank_map
    return rank_map


def apply_futboleras_ranks_to_events(events: dict[str, dict[str, Any]], rank_map: dict[str, str]) -> None:
    if not rank_map:
        return

    for event in events.values():
        team1 = str(event.get("team1", "")).strip()
        team2 = str(event.get("team2", "")).strip()
        if not team1 or not team2:
            continue

        rank_home = rank_map.get(normalize_futboleras_team_key(team1), "")
        rank_away = rank_map.get(normalize_futboleras_team_key(team2), "")
        if rank_home:
            event["rank_home"] = rank_home
        if rank_away:
            event["rank_away"] = rank_away


def scrape_futboleras_url(
    url: str,
    session: Optional[requests.Session] = None,
) -> tuple[dict[str, dict[str, Any]], bool]:
    own_session = session is None
    if session is None:
        session = requests.Session()
    try:
        try:
            page_html = fetch_futboleras_page_html(session, url)
        except requests.RequestException as exc:
            print(f"No se pudo obtener Futboleras {url}: {exc}")
            return {}, True

        soup = BeautifulSoup(page_html, "html.parser")
        extracted_events = extract_futboleras_events_from_soup(soup, url)
        if not extracted_events:
            print(f"Futboleras sin eventos parseables para {url}")
            return {}, True

        team_name = extract_futboleras_team_name(soup, url)
        _competition_name, _start_year, _end_year, competition_id = extract_futboleras_competition_context(
            soup, team_name
        )
        if competition_id:
            rank_map = build_futboleras_competition_rank_map(session, url, soup, competition_id)
            apply_futboleras_ranks_to_events(extracted_events, rank_map)

        return extracted_events, False
    finally:
        if own_session:
            session.close()


def scrape_futboleras_events(urls: list[str]) -> tuple[dict[str, dict[str, Any]], int]:
    if not urls:
        return {}, 0

    unique_urls = list(dict.fromkeys(urls))
    gamelist: dict[str, dict[str, Any]] = {}
    failed_urls = 0

    max_workers = min(SCRAPE_MAX_WORKERS, len(unique_urls))
    if max_workers <= 1:
        with requests.Session() as session:
            for url in unique_urls:
                extracted_events, failed = scrape_futboleras_url(url, session=session)
                if failed:
                    failed_urls += 1
                    continue
                for gameid, event in extracted_events.items():
                    if gameid not in gamelist:
                        gamelist[gameid] = event
                    else:
                        gamelist[gameid] = merge_event_payload(gamelist[gameid], event)
    else:
        thread_local_session = local()
        worker_sessions: list[requests.Session] = []
        worker_sessions_lock = Lock()

        def scrape_worker(worker_url: str) -> tuple[dict[str, dict[str, Any]], bool]:
            session = getattr(thread_local_session, "session", None)
            if session is None:
                session = requests.Session()
                thread_local_session.session = session
                with worker_sessions_lock:
                    worker_sessions.append(session)
            return scrape_futboleras_url(worker_url, session=session)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(scrape_worker, url): url for url in unique_urls}
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    extracted_events, failed = future.result()
                except Exception as exc:
                    print(f"No se pudo procesar Futboleras {url}: {exc}")
                    failed_urls += 1
                    continue
                if failed:
                    failed_urls += 1
                    continue
                for gameid, event in extracted_events.items():
                    if gameid not in gamelist:
                        gamelist[gameid] = event
                    else:
                        gamelist[gameid] = merge_event_payload(gamelist[gameid], event)

        for session in worker_sessions:
            session.close()

    print(
        "Diagnóstico Futboleras: "
        f"urls={len(unique_urls)}, fallidas={failed_urls}, eventos={len(gamelist)}"
    )
    return gamelist, failed_urls


def extract_sport_name(soup: BeautifulSoup) -> str:
    title = soup.find("title")
    if not title or not title.text:
        return ""
    title_text = title.text
    if " - " in title_text:
        title_text = title_text.split(" - ", 1)[1]
    if "," in title_text:
        title_text = title_text.split(",", 1)[0]
    return title_text.split("Flashscore.es / ")[-1].strip()


def parse_tv_channels(tv_raw: Optional[str]) -> list[str]:
    if not tv_raw:
        return []
    try:
        tv_data = json.loads(tv_raw)
    except json.JSONDecodeError:
        return []

    channels = tv_data.get("1")
    if not isinstance(channels, list):
        return []

    return [channel.get("BN", "").strip() for channel in channels if isinstance(channel, dict) and channel.get("BN")]


def normalize_rank_entry(rank_value: Any) -> str:
    if isinstance(rank_value, list):
        parts = [str(item).strip() for item in rank_value if str(item).strip()]
        if len(parts) >= 2:
            ranking_name = parts[0]
            ranking_position = parts[1]
            if ranking_name and ranking_position:
                return f"{ranking_name} #{ranking_position}"
        if parts:
            return parts[0]
        return ""

    if isinstance(rank_value, (int, float)):
        return f"#{int(rank_value)}"

    if isinstance(rank_value, str):
        return rank_value.strip()

    return ""


def should_fetch_classification(event: dict[str, Any]) -> bool:
    gameid = str(event.get("gameid", "")).strip()
    # Classification endpoint works only with Flashscore match ids.
    if not gameid or gameid.startswith(("fu_", "ss_", "bs_")):
        return False

    has_opponent = bool(str(event.get("team2", "")).strip())
    if has_opponent:
        # Team events: always try classification (with standings fallback).
        return True

    sport_name = str(event.get("sports", "")).strip().upper()
    if sport_name in CLASSIFICATION_SPORTS:
        return True

    return False


def extract_environment_data(page_html: str) -> dict[str, Any]:
    match = ENVIRONMENT_ASSIGN_PATTERN.search(page_html)
    if not match:
        return {}

    idx = match.end()
    length = len(page_html)
    while idx < length and page_html[idx].isspace():
        idx += 1
    if idx >= length or page_html[idx] != "{":
        return {}

    start_idx = idx
    depth = 0
    in_string = False
    escaped = False

    for pos in range(start_idx, length):
        char = page_html[pos]
        if in_string:
            if escaped:
                escaped = False
                continue
            if char == "\\":
                escaped = True
                continue
            if char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
            continue
        if char == "{":
            depth += 1
            continue
        if char == "}":
            depth -= 1
            if depth == 0:
                json_text = page_html[start_idx : pos + 1]
                try:
                    data = json.loads(json_text)
                except json.JSONDecodeError:
                    return {}
                if isinstance(data, dict):
                    return data
                return {}

    return {}


def extract_feed_sign_from_page_html(page_html: str) -> str:
    match = FEED_SIGN_PATTERN.search(page_html) or FEED_SIGN_CONFIG_PATTERN.search(page_html)
    if not match:
        return ""
    return match.group(1).strip()


def extract_project_id_from_environment_data(environment: dict[str, Any], page_html: str) -> str:
    project_id = str(environment.get("project_id", "")).strip()
    if project_id.isdigit():
        return project_id

    config_data = environment.get("config", {})
    if isinstance(config_data, dict):
        project_data = config_data.get("project", {})
        if isinstance(project_data, dict):
            project_id = str(project_data.get("id", "")).strip()
            if project_id.isdigit():
                return project_id

    return extract_project_id(page_html)


def extract_primary_participant_id(participants: Any) -> str:
    if not isinstance(participants, list) or not participants:
        return ""

    first_participant = participants[0]
    if not isinstance(first_participant, dict):
        return ""

    participant_id = str(first_participant.get("id", "")).strip()
    if participant_id:
        return participant_id
    return str(first_participant.get("eventParticipantId", "")).strip()


def parse_team_standings_rank_map(payload: str) -> dict[str, str]:
    rank_map: dict[str, str] = {}
    if not payload:
        return rank_map

    for event_blob in normalize_feed_payload(payload).split("¬~"):
        if "TI÷" not in event_blob or "TR÷" not in event_blob:
            continue

        fields = parse_event_fields(event_blob)
        participant_id = str(fields.get("TI", "")).strip()
        rank_value = str(fields.get("TR", "")).strip()
        if not participant_id or not rank_value.isdigit():
            continue
        rank_map[participant_id] = f"#{int(rank_value)}"

    return rank_map


def fetch_team_standings_rank_map(
    session: requests.Session,
    project_id: str,
    feed_sign: str,
    tournament_id: str,
    tournament_stage_id: str,
) -> dict[str, str]:
    if not project_id or not feed_sign or not tournament_id or not tournament_stage_id:
        return {}

    feed_name = f"to_{tournament_id}_{tournament_stage_id}_{TEAM_STANDINGS_DEFAULT_VIEW_ID}"
    with TEAM_STANDINGS_FEED_CACHE_LOCK:
        cached_map = TEAM_STANDINGS_FEED_CACHE.get(feed_name)
    if cached_map is not None:
        return cached_map

    payload = fetch_feed_payload(session, project_id, feed_name, feed_sign)
    rank_map = parse_team_standings_rank_map(payload)
    with TEAM_STANDINGS_FEED_CACHE_LOCK:
        TEAM_STANDINGS_FEED_CACHE[feed_name] = rank_map
    return rank_map


def fallback_team_ranks_from_standings(
    session: requests.Session,
    page_url: str,
    page_html: str,
    environment: dict[str, Any],
    home_participants: Any,
    away_participants: Any,
) -> tuple[str, str]:
    home_id = extract_primary_participant_id(home_participants)
    away_id = extract_primary_participant_id(away_participants)
    if not home_id and not away_id:
        return "", ""

    stats2_config = environment.get("stats2_config", {})
    if not isinstance(stats2_config, dict):
        return "", ""

    tournament_id = str(stats2_config.get("tournament", "")).strip()
    tournament_stage_id = str(stats2_config.get("tournamentStage", "")).strip()
    if not tournament_id or not tournament_stage_id:
        return "", ""

    project_id = extract_project_id_from_environment_data(environment, page_html)
    feed_sign = extract_feed_sign_from_page_html(page_html)
    if not feed_sign:
        soup = BeautifulSoup(page_html, "html.parser")
        core_script_url, _core_project_id = extract_core_script_context(soup, page_url)
        feed_sign = extract_feed_sign(session, core_script_url)
    if not feed_sign:
        return "", ""

    rank_map = fetch_team_standings_rank_map(
        session,
        project_id,
        feed_sign,
        tournament_id,
        tournament_stage_id,
    )
    if not rank_map:
        return "", ""

    return rank_map.get(home_id, ""), rank_map.get(away_id, "")


def fetch_event_classification(session: requests.Session, gameid: str) -> tuple[str, str, bool]:
    if not gameid:
        return "", "", False

    url = MATCH_DETAIL_URL_TEMPLATE.format(gameid=gameid)
    response = None
    last_error: Optional[Exception] = None
    for attempt in range(2):
        try:
            response = session.get(url, timeout=CLASSIFICATION_FETCH_TIMEOUT_SECONDS)
            response.raise_for_status()
            break
        except requests.RequestException as exc:
            last_error = exc
            if attempt == 0:
                time.sleep(0.2)
                continue
            print(f"No se pudo obtener clasificación para {gameid}: {exc}")
            return "", "", False

    if response is None:
        if last_error is not None:
            print(f"No se pudo obtener clasificación para {gameid}: {last_error}")
        return "", "", False

    environment = extract_environment_data(response.text)
    if not environment:
        return "", "", False

    participants_data = environment.get("participantsData", {})
    home_participants = participants_data.get("home", [])
    away_participants = participants_data.get("away", [])

    home_rank_raw = home_participants[0].get("rank", []) if home_participants else []
    away_rank_raw = away_participants[0].get("rank", []) if away_participants else []

    rank_home = normalize_rank_entry(home_rank_raw)
    rank_away = normalize_rank_entry(away_rank_raw)
    if rank_home or rank_away:
        return rank_home, rank_away, True

    fallback_home, fallback_away = fallback_team_ranks_from_standings(
        session,
        url,
        response.text,
        environment,
        home_participants,
        away_participants,
    )
    if fallback_home:
        rank_home = fallback_home
    if fallback_away:
        rank_away = fallback_away

    return rank_home, rank_away, True


def apply_classification_to_event(event: dict[str, Any], rank_home: str, rank_away: str) -> None:
    if rank_home:
        event["rank_home"] = rank_home
    else:
        event.pop("rank_home", None)

    if rank_away:
        event["rank_away"] = rank_away
    else:
        event.pop("rank_away", None)


def event_has_sufficient_rank_data(event: dict[str, Any]) -> bool:
    rank_home = str(event.get("rank_home", "")).strip()
    rank_away = str(event.get("rank_away", "")).strip()
    if not rank_home and not rank_away:
        return False

    has_opponent = bool(str(event.get("team2", "")).strip())
    if has_opponent:
        return bool(rank_home and rank_away)
    return True


def enrich_events_with_classification(
    gamelist: dict[str, dict[str, Any]],
    cache_file: Path = CLASSIFICATION_CACHE_FILE,
) -> None:
    cache_data = load_pickle(cache_file, {})
    if not isinstance(cache_data, dict):
        cache_data = {}

    now_timestamp = int(datetime.now(tz=UTC).timestamp())
    ttl_seconds = max(0, CLASSIFICATION_CACHE_TTL_DAYS) * 24 * 60 * 60

    events_pending_fetch: dict[str, list[dict[str, Any]]] = {}
    for event in gamelist.values():
        if not should_fetch_classification(event):
            continue

        gameid = str(event.get("gameid", "")).strip()
        if not gameid:
            continue

        if CLASSIFICATION_SKIP_FETCH_WHEN_PRESENT and event_has_sufficient_rank_data(event):
            current_home_rank = str(event.get("rank_home", "")).strip()
            current_away_rank = str(event.get("rank_away", "")).strip()
            cache_data[gameid] = {
                "rank_home": current_home_rank,
                "rank_away": current_away_rank,
                "fetched_at": now_timestamp,
            }
            continue

        cache_entry = cache_data.get(gameid, {})
        if isinstance(cache_entry, dict):
            cached_home_rank = str(cache_entry.get("rank_home", "")).strip()
            cached_away_rank = str(cache_entry.get("rank_away", "")).strip()
            fetched_at = cache_entry.get("fetched_at", 0)
            fetched_at_int = int(fetched_at) if str(fetched_at).isdigit() else 0
            is_fresh = ttl_seconds > 0 and fetched_at_int > 0 and (now_timestamp - fetched_at_int) < ttl_seconds
            if CLASSIFICATION_REFRESH_EMPTY_CACHE and not cached_home_rank and not cached_away_rank:
                is_fresh = False
            if is_fresh:
                apply_classification_to_event(event, cached_home_rank, cached_away_rank)
                continue

        events_pending_fetch.setdefault(gameid, []).append(event)

    gameids_to_fetch = list(events_pending_fetch.keys())
    if not gameids_to_fetch:
        save_pickle(cache_file, cache_data)
        return

    def apply_cached_ranks_if_available(gameid: str) -> bool:
        cache_entry = cache_data.get(gameid, {})
        if not isinstance(cache_entry, dict):
            return False
        cached_home_rank = str(cache_entry.get("rank_home", "")).strip()
        cached_away_rank = str(cache_entry.get("rank_away", "")).strip()
        if not cached_home_rank and not cached_away_rank:
            return False
        for pending_event in events_pending_fetch.get(gameid, []):
            apply_classification_to_event(pending_event, cached_home_rank, cached_away_rank)
        return True

    def apply_fetched_ranks(gameid: str, rank_home: str, rank_away: str, fetch_ok: bool) -> None:
        if not fetch_ok:
            # Keep stale cached ranks if available, and avoid overwriting cache with empty data.
            apply_cached_ranks_if_available(gameid)
            return

        cache_data[gameid] = {
            "rank_home": rank_home,
            "rank_away": rank_away,
            "fetched_at": now_timestamp,
        }
        for pending_event in events_pending_fetch.get(gameid, []):
            apply_classification_to_event(pending_event, rank_home, rank_away)

    max_workers = min(CLASSIFICATION_MAX_WORKERS, len(gameids_to_fetch))
    if max_workers <= 1:
        with requests.Session() as session:
            for gameid in gameids_to_fetch:
                rank_home, rank_away, fetch_ok = fetch_event_classification(session, gameid)
                apply_fetched_ranks(gameid, rank_home, rank_away, fetch_ok)
    else:
        thread_local_session = local()
        worker_sessions: list[requests.Session] = []
        worker_sessions_lock = Lock()

        def fetch_worker(gameid: str) -> tuple[str, str, str, bool]:
            session = getattr(thread_local_session, "session", None)
            if session is None:
                session = requests.Session()
                thread_local_session.session = session
                with worker_sessions_lock:
                    worker_sessions.append(session)
            rank_home, rank_away, fetch_ok = fetch_event_classification(session, gameid)
            return gameid, rank_home, rank_away, fetch_ok

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_worker, gameid): gameid for gameid in gameids_to_fetch}
            for future in as_completed(futures):
                gameid = futures[future]
                try:
                    fetched_gameid, rank_home, rank_away, fetch_ok = future.result()
                except Exception as exc:
                    print(f"No se pudo obtener clasificación para {gameid}: {exc}")
                    apply_fetched_ranks(gameid, "", "", False)
                    continue
                apply_fetched_ranks(fetched_gameid, rank_home, rank_away, fetch_ok)

        for session in worker_sessions:
            session.close()

    save_pickle(cache_file, cache_data)


def parse_event_fields(event_blob: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for match in FIELD_PATTERN.finditer(event_blob):
        key = match.group(1).upper()
        value = match.group(2)
        if key not in fields:
            fields[key] = value
            continue
        if not fields[key].strip() and value.strip():
            fields[key] = value
    return fields


def merge_fields_by_priority(target: dict[str, str], source: dict[str, str]) -> None:
    for key, value in source.items():
        if key not in target:
            target[key] = value
            continue
        if not str(target[key]).strip() and str(value).strip():
            target[key] = value


def build_league_marker_map(page_chunks: list[tuple[dict[str, str], str]]) -> dict[tuple[str, str], str]:
    marker_map: dict[tuple[str, str], str] = {}
    for fields, event_blob in page_chunks:
        league = extract_league(fields, event_blob)
        if not league:
            continue

        for key, value in fields.items():
            if key == "ZA" or not key.startswith("Z"):
                continue
            marker = str(value).strip()
            if not marker or len(marker) > 40:
                continue
            if not re.fullmatch(r"[A-Za-z0-9_-]+", marker):
                continue
            marker_map.setdefault((key, marker), league)
    return marker_map


def inject_league_from_markers(fields: dict[str, str], marker_map: dict[tuple[str, str], str]) -> dict[str, str]:
    if str(fields.get("ZA", "")).strip():
        return fields

    for key, value in fields.items():
        marker = str(value).strip()
        if not marker:
            continue
        league = marker_map.get((key, marker))
        if league:
            updated = fields.copy()
            updated["ZA"] = league
            return updated
    return fields


def is_textual_value(value: str) -> bool:
    return any(char.isalpha() for char in value)


def is_likely_league_value(value: str) -> bool:
    stripped = value.strip()
    if not stripped:
        return False
    if len(stripped) > 120:
        return False
    if not is_textual_value(stripped):
        return False
    if stripped[0] in "{[":
        return False
    lower = stripped.lower()
    if "http" in lower or "\\/" in stripped:
        return False
    if any(char in stripped for char in "{}[]"):
        return False
    if lower.startswith("resultado del primer partido"):
        return False
    return True


def normalize_league(value: Any) -> str:
    text = str(value).strip()
    if not text:
        return ""
    if not is_likely_league_value(text):
        return ""
    return text


def competition_text_for_summary(event: dict[str, Any]) -> str:
    league = normalize_league(event.get("league", ""))
    if league:
        return league

    description = str(event.get("description", "")).strip()
    if description:
        league_part = description.split(" / ", 1)[0].strip()
        if normalize_league(league_part):
            return league_part
    return ""


def competition_from_description(description: str) -> str:
    first_part = description.split(" / ", 1)[0].strip()
    return normalize_league(first_part)


def competition_override(event: dict[str, Any]) -> str:
    gameid = str(event.get("gameid", "")).strip()
    if gameid in KNOWN_COMPETITIONS_BY_GAMEID:
        return KNOWN_COMPETITIONS_BY_GAMEID[gameid]

    sport = str(event.get("sports", "")).strip().upper()
    team1 = str(event.get("team1", "")).strip().upper()
    team2 = str(event.get("team2", "")).strip().upper()
    matchup_key = (sport, team1, team2)
    return KNOWN_COMPETITIONS_BY_MATCHUP.get(matchup_key, "")


def infer_competition_from_context(event: dict[str, Any]) -> str:
    sport = str(event.get("sports", "")).strip().upper()
    tv_channels = event.get("tv", [])
    if not isinstance(tv_channels, list):
        return ""

    tv_text = " ".join(str(channel) for channel in tv_channels).upper()
    if sport == "FÚTBOL":
        if "LALIGA" in tv_text:
            return "ESPAÑA: LaLiga EA Sports"
        if "LIGA F" in tv_text:
            return "ESPAÑA: Liga F"
    return ""


def parse_score(raw_value: Optional[str]) -> Optional[int]:
    if not raw_value:
        return None
    match = re.search(r"\d+", raw_value)
    if not match:
        return None
    return int(match.group(0))


def extract_scores(fields: dict[str, str]) -> tuple[Optional[int], Optional[int]]:
    score_key_pairs = (("AG", "AH"), ("BA", "BB"), ("DB", "DC"))
    for home_key, away_key in score_key_pairs:
        home_score = parse_score(fields.get(home_key))
        away_score = parse_score(fields.get(away_key))
        if home_score is not None and away_score is not None:
            return home_score, away_score
    return None, None


def parse_counter_value(raw_value: Optional[str]) -> Optional[int]:
    if raw_value is None:
        return None
    text = str(raw_value).strip()
    if not text or not text.isdigit():
        return None
    return int(text)


def extract_red_cards(fields: dict[str, str]) -> tuple[Optional[int], Optional[int]]:
    home_cards = parse_counter_value(fields.get("AJ"))
    away_cards = parse_counter_value(fields.get("AK"))
    return home_cards, away_cards


def extract_individual_position(fields: dict[str, str]) -> str:
    for key in MOTORSPORT_POSITION_FIELD_KEYS:
        value = str(fields.get(key, "")).strip()
        if value.isdigit():
            return f"#{int(value)}"
    return ""


def extract_result_status(fields: dict[str, str]) -> str:
    status_keys = ("AS", "AB", "AC", "AX", "AT", "AU")
    for key in status_keys:
        status_value = fields.get(key, "").strip()
        if status_value and not status_value.isdigit():
            return status_value
    return ""


def extract_end_timestamp(fields: dict[str, str], has_opponent: bool) -> int:
    start_raw = str(fields.get("AD", "")).strip()
    if not start_raw.isdigit():
        return 0
    start_ts = int(start_raw)

    # Primary end timestamp used by Flashscore payload when available.
    date_end_raw = str(fields.get("AP", "")).strip()
    if date_end_raw.isdigit():
        date_end_ts = int(date_end_raw)
        if date_end_ts > start_ts:
            return date_end_ts

    # Fallback: AO behaves as "last update" and is near real end for closed fixtures.
    # Keep this path conservative to avoid polluting in-progress or individual events.
    if not has_opponent:
        return 0

    if str(fields.get("AB", "")).strip() != "3":
        return 0

    date_updated_raw = str(fields.get("AO", "")).strip()
    if not date_updated_raw.isdigit():
        return 0

    date_updated_ts = int(date_updated_raw)
    duration_seconds = date_updated_ts - start_ts
    if duration_seconds <= 0:
        return 0
    if duration_seconds > 48 * 60 * 60:
        return 0

    return date_updated_ts


def normalize_score_text(value: str) -> str:
    if not value:
        return ""
    normalized = re.sub(r"\s+", " ", value).strip()
    # Normalize score separators 1-0 / 1:0 / 1 - 0 -> 1–0
    normalized = re.sub(r"(?<=\d)\s*[-:]\s*(?=\d)", "\u2013", normalized)
    return normalized.strip(" .;|")


def extract_first_leg_from_text(text: str) -> str:
    raw_text = str(text).strip()
    if not raw_text:
        return ""

    for pattern in FIRST_LEG_PATTERNS:
        match = pattern.search(raw_text)
        if not match:
            continue
        candidate = normalize_score_text(match.group(1))
        if candidate:
            return candidate

    return ""


def extract_first_leg_from_fields(fields: dict[str, str]) -> str:
    for value in fields.values():
        text = str(value).strip()
        if not text:
            continue
        lower_text = text.lower()
        if "primer partido" not in lower_text and " ida" not in lower_text and not lower_text.startswith("ida"):
            continue
        candidate = extract_first_leg_from_text(text)
        if candidate:
            return candidate
    return ""


def extract_za_from_blob(event_blob: str) -> str:
    # Some payload fragments are escaped; normalize separators for regex extraction.
    normalized_blob = event_blob.replace("\\u00ac", "¬").replace("\\u00f7", "÷")
    candidates: list[str] = []
    for value in re.findall(r"ZA÷(.*?)¬", normalized_blob, flags=re.IGNORECASE):
        league = normalize_league(value)
        if league:
            candidates.append(league)
    for league in candidates:
        if ":" in league:
            return league
    if candidates:
        return candidates[0]
    return ""


def extract_league(fields: dict[str, str], event_blob: str = "") -> str:
    # First, trust direct ZA extraction from the raw event blob.
    if event_blob:
        league_from_blob = extract_za_from_blob(event_blob)
        if league_from_blob:
            return league_from_blob

    # Preferred legacy key from parsed fields.
    if fields.get("ZA", "").strip():
        return fields["ZA"].strip()

    # Fallback to every Z* textual key seen in payloads.
    text_candidates: list[str] = []
    for key in sorted(fields):
        if not key.upper().startswith("Z"):
            continue
        value = fields.get(key, "").strip()
        if is_likely_league_value(value):
            text_candidates.append(value)

    # If any candidate already contains country/competition format, use it directly.
    for value in text_candidates:
        if ":" in value:
            return value

    # Otherwise compose a readable label from up to two distinct textual pieces.
    deduped: list[str] = []
    for value in text_candidates:
        if value not in deduped:
            deduped.append(value)
        if len(deduped) == 2:
            break

    if len(deduped) == 2:
        return f"{deduped[0]}: {deduped[1]}"
    if len(deduped) == 1:
        return deduped[0]

    # Last fallback: any textual value in the payload that already matches
    # "Country: Competition" shape.
    team_values = {fields.get("AE", "").strip(), fields.get("AF", "").strip()}
    for key in sorted(fields):
        value = fields.get(key, "").strip()
        if not value or value in team_values:
            continue
        if not is_likely_league_value(value):
            continue
        if ":" in value:
            return value
    return ""


def iter_feed_events(script_text: str) -> list[str]:
    all_events: list[str] = []
    for payload_match in PAYLOAD_PATTERN.finditer(script_text):
        payload = payload_match.group(1)
        all_events.extend(chunk for chunk in payload.split("¬~") if chunk)
    return all_events


def iter_feed_events_with_context(script_text: str) -> list[tuple[dict[str, str], str]]:
    enriched_events: list[tuple[dict[str, str], str]] = []
    current_league = ""

    for event_blob in iter_feed_events(script_text):
        fields = parse_event_fields(event_blob)
        if not fields:
            continue

        league_from_blob = extract_za_from_blob(event_blob)
        if league_from_blob:
            current_league = league_from_blob
        elif fields.get("AA") and current_league and not fields.get("ZA", "").strip():
            fields = fields.copy()
            fields["ZA"] = current_league

        enriched_events.append((fields, event_blob))

    return enriched_events


def build_event(fields: dict[str, str], sport_name: str, event_blob: str = "") -> Optional[dict[str, Any]]:
    game_id = fields.get("AA")
    timestamp_raw = fields.get("AD")
    if not game_id or not timestamp_raw or not timestamp_raw.isdigit():
        return None

    event = {
        "gameid": game_id,
        "league": extract_league(fields, event_blob),
        "team1": fields.get("AE", ""),
        "date": int(timestamp_raw),
        "sports": sport_name,
        "status": "CONFIRMED",
        "url": "",
    }

    team2 = fields.get("AF")
    if team2:
        event["team2"] = team2
    else:
        participant_position = extract_individual_position(fields)
        if participant_position:
            event["rank_home"] = participant_position

    date_end_ts = extract_end_timestamp(fields, has_opponent=bool(team2))
    if date_end_ts:
        event["date_end"] = date_end_ts

    score_home, score_away = extract_scores(fields)
    if score_home is not None and score_away is not None:
        event["score_home"] = score_home
        event["score_away"] = score_away

    red_cards_home, red_cards_away = extract_red_cards(fields)
    if red_cards_home is not None and red_cards_home > 0:
        event["red_cards_home"] = red_cards_home
    if red_cards_away is not None and red_cards_away > 0:
        event["red_cards_away"] = red_cards_away

    result_status = extract_result_status(fields)
    if result_status:
        event["result_status"] = result_status

    first_leg_result = extract_first_leg_from_fields(fields)
    if not first_leg_result and result_status:
        first_leg_result = extract_first_leg_from_text(result_status)
    if first_leg_result:
        event["first_leg_result"] = first_leg_result

    tv_channels = parse_tv_channels(fields.get("AL"))
    if tv_channels:
        event["tv"] = tv_channels

    return event


def merge_event_payload(existing_event: dict[str, Any], incoming_event: dict[str, Any]) -> dict[str, Any]:
    merged = existing_event.copy()

    for key in (
        "league",
        "team1",
        "team2",
        "result_status",
        "url",
        "rank_home",
        "rank_away",
        "first_leg_result",
        "red_cards_home",
        "red_cards_away",
    ):
        if not str(merged.get(key, "")).strip() and str(incoming_event.get(key, "")).strip():
            merged[key] = incoming_event[key]

    if "date_end" not in merged and "date_end" in incoming_event:
        merged["date_end"] = incoming_event["date_end"]

    if "score_home" not in merged and "score_home" in incoming_event:
        merged["score_home"] = incoming_event["score_home"]
    if "score_away" not in merged and "score_away" in incoming_event:
        merged["score_away"] = incoming_event["score_away"]

    existing_tv = merged.get("tv", [])
    incoming_tv = incoming_event.get("tv", [])
    if isinstance(existing_tv, list) and isinstance(incoming_tv, list):
        merged_tv = list(dict.fromkeys(existing_tv + incoming_tv))
        if merged_tv:
            merged["tv"] = merged_tv
    elif not existing_tv and incoming_tv:
        merged["tv"] = incoming_tv

    existing_rankings = merged.get("participant_rankings", [])
    incoming_rankings = incoming_event.get("participant_rankings", [])
    if isinstance(existing_rankings, list) and isinstance(incoming_rankings, list):
        merged_rankings = list(dict.fromkeys(str(item).strip() for item in (existing_rankings + incoming_rankings) if str(item).strip()))
        if merged_rankings:
            merged["participant_rankings"] = merged_rankings
    elif not existing_rankings and isinstance(incoming_rankings, list) and incoming_rankings:
        merged["participant_rankings"] = incoming_rankings

    return merged


def normalize_feed_payload(payload: str) -> str:
    return payload.replace("\\u00ac", "¬").replace("\\u00f7", "÷")


def sanitize_translation_value(value: str) -> str:
    cleaned = value.replace("{STAGE-PLACEHOLDER}", "").strip()
    return re.sub(r"\s{2,}", " ", cleaned)


def extract_core_script_context(soup: BeautifulSoup, page_url: str) -> tuple[str, str]:
    for script in soup.find_all("script", src=True):
        script_src = str(script.get("src", "")).strip()
        match = CORE_SCRIPT_SRC_PATTERN.search(script_src)
        if match:
            return urljoin(page_url, script_src), match.group(1)
    return "", ""


def extract_participant_id(page_html: str) -> str:
    match = PARTICIPANT_ID_PATTERN.search(page_html)
    if not match:
        return ""
    return match.group(1).strip()


def extract_participant_id_from_url(url: str) -> str:
    match = PARTICIPANT_URL_ID_PATTERN.search(url)
    if not match:
        return ""
    return match.group(1).strip()


def event_matches_participant(fields: dict[str, str], participant_id: str) -> bool:
    if not participant_id:
        return True

    row_participant_id = str(fields.get("PX", "")).strip()
    if row_participant_id and row_participant_id != participant_id:
        return False
    return True


def extract_project_id(page_html: str, default_project_id: str = "") -> str:
    if default_project_id:
        return default_project_id
    match = PROJECT_ID_PATTERN.search(page_html)
    if match:
        return match.group(1).strip()
    return "13"


def extract_sport_slug(page_html: str) -> str:
    match = SPORT_SLUG_PATTERN.search(page_html)
    if not match:
        return ""
    return match.group(1).strip()


def extract_sport_id(page_html: str) -> str:
    match = SPORT_ID_PATTERN.search(page_html)
    if not match:
        return ""
    return match.group(1).strip()


def extract_feed_sign(session: requests.Session, core_script_url: str) -> str:
    if not core_script_url:
        return ""
    with CORE_FEED_SIGN_CACHE_LOCK:
        cached_feed_sign = CORE_FEED_SIGN_CACHE.get(core_script_url, "")
    if cached_feed_sign:
        return cached_feed_sign

    try:
        response = session.get(core_script_url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"No se pudo obtener script core para firma de feed ({core_script_url}): {exc}")
        return ""

    core_js = response.text
    match = FEED_SIGN_PATTERN.search(core_js) or FEED_SIGN_CONFIG_PATTERN.search(core_js)
    if not match:
        print(f"No se encontró feed_sign en script core: {core_script_url}")
        return ""

    feed_sign = match.group(1).strip()
    if feed_sign:
        with CORE_FEED_SIGN_CACHE_LOCK:
            CORE_FEED_SIGN_CACHE[core_script_url] = feed_sign
    return feed_sign


def fetch_feed_payload(
    session: requests.Session,
    project_id: str,
    feed_name: str,
    feed_sign: str,
) -> str:
    feed_url = f"https://global.flashscore.ninja/{project_id}/x/feed/{feed_name}"
    try:
        response = session.get(
            feed_url,
            headers={"x-fsign": feed_sign},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"No se pudo obtener feed {feed_name}: {exc}")
        return ""

    payload = normalize_feed_payload(response.text)
    if "÷" not in payload:
        return ""
    return payload


def build_translation_map(payloads: list[str]) -> dict[str, str]:
    translation_map: dict[str, str] = {}
    for payload in payloads:
        if not payload:
            continue
        normalized_payload = normalize_feed_payload(payload)
        if not normalized_payload.endswith("¬"):
            normalized_payload = f"{normalized_payload}¬"
        for token, raw_value in LV_PATTERN.findall(normalized_payload):
            value = sanitize_translation_value(raw_value)
            if token not in translation_map or (not translation_map[token] and value):
                translation_map[token] = value
    return translation_map


def resolve_tokens_in_text(value: str, translation_map: dict[str, str]) -> str:
    if not value:
        return value

    resolved = value
    for _ in range(5):
        updated = TOKEN_PATTERN.sub(lambda match: translation_map.get(match.group(0), match.group(0)), resolved)
        if updated == resolved:
            break
        resolved = updated

    return sanitize_translation_value(resolved)


def resolve_tokens_in_fields(fields: dict[str, str], translation_map: dict[str, str]) -> dict[str, str]:
    if not translation_map:
        return fields

    resolved_fields: dict[str, str] = {}
    has_changes = False
    for key, value in fields.items():
        if "{" in value and "}" in value:
            resolved_value = resolve_tokens_in_text(value, translation_map)
        else:
            resolved_value = sanitize_translation_value(value)
        resolved_fields[key] = resolved_value
        if resolved_value != value:
            has_changes = True
    return resolved_fields if has_changes else fields


def iter_contextual_rows_from_payload(
    payload: str,
    translation_map: dict[str, str],
) -> list[tuple[dict[str, str], str]]:
    synthetic_script = f"cjs.initialFeeds['dynamic_feed'] = {{data: `{payload}`}}"
    rows: list[tuple[dict[str, str], str]] = []
    current_league = ""
    current_context: dict[str, str] = {}

    for event_blob in iter_feed_events(synthetic_script):
        fields = parse_event_fields(event_blob)
        if not fields:
            continue

        resolved_fields = resolve_tokens_in_fields(fields, translation_map)
        resolved_league = normalize_league(resolved_fields.get("ZA", ""))
        if resolved_league:
            current_league = resolved_league

        for key in FEED_CONTEXT_KEYS:
            context_value = str(resolved_fields.get(key, "")).strip()
            if context_value:
                current_context[key] = context_value

        if not resolved_fields.get("AA"):
            continue

        row_fields = resolved_fields.copy()
        if current_league and not row_fields.get("ZA", "").strip():
            row_fields["ZA"] = current_league

        for key, value in current_context.items():
            if value and not str(row_fields.get(key, "")).strip():
                row_fields[key] = value

        rows.append((row_fields, event_blob))

    return rows


def collect_recent_tournaments(
    rows: list[tuple[dict[str, str], str]],
    default_sport_id: str,
) -> set[tuple[str, str, str]]:
    now = datetime.now(tz=UTC)
    lookback_cutoff = int((now - timedelta(days=max(0, PAST_RESULTS_DAYS))).timestamp())
    tournaments: set[tuple[str, str, str]] = set()

    for fields, _event_blob in rows:
        timestamp_raw = str(fields.get("AD", "")).strip()
        if timestamp_raw.isdigit() and int(timestamp_raw) < lookback_cutoff:
            continue

        tournament_id = str(fields.get("ZEE", "")).strip()
        category_id = str(fields.get("ZHS", "")).strip()
        sport_id = str(fields.get("SA", "")).strip() or default_sport_id

        if not tournament_id or not category_id.isdigit() or not sport_id.isdigit():
            continue
        tournaments.add((sport_id, category_id, tournament_id))

    return tournaments


def fetch_motorsport_tournament_rows(
    session: requests.Session,
    project_id: str,
    feed_sign: str,
    sport_id: str,
    category_id: str,
    tournament_id: str,
) -> list[tuple[dict[str, str], str]]:
    feed_name = f"t_{sport_id}_{category_id}_{tournament_id}_1_es_1"
    with MOTORSPORT_TOURNAMENT_FEED_CACHE_LOCK:
        cached_rows = MOTORSPORT_TOURNAMENT_FEED_CACHE.get(feed_name)
    if cached_rows is not None:
        return cached_rows

    payload = fetch_feed_payload(session, project_id, feed_name, feed_sign)
    if not payload:
        with MOTORSPORT_TOURNAMENT_FEED_CACHE_LOCK:
            MOTORSPORT_TOURNAMENT_FEED_CACHE[feed_name] = []
        return []

    translation_map = build_translation_map([payload])
    rows = iter_contextual_rows_from_payload(payload, translation_map)
    with MOTORSPORT_TOURNAMENT_FEED_CACHE_LOCK:
        MOTORSPORT_TOURNAMENT_FEED_CACHE[feed_name] = rows
    return rows


def fetch_motorsport_session_chunks(
    session: requests.Session,
    page_html: str,
    project_id: str,
    feed_sign: str,
    participant_id: str,
    base_rows: list[tuple[dict[str, str], str]],
) -> list[tuple[dict[str, str], str]]:
    if not INCLUDE_MOTORSPORT_SESSIONS:
        return []

    sport_slug = extract_sport_slug(page_html)
    if sport_slug not in MOTORSPORT_SPORT_SLUGS:
        return []

    default_sport_id = extract_sport_id(page_html)
    tournament_keys = collect_recent_tournaments(base_rows, default_sport_id)
    if not tournament_keys:
        return []

    participant_rows: list[tuple[dict[str, str], str]] = []
    for sport_id, category_id, tournament_id in sorted(tournament_keys):
        tournament_rows = fetch_motorsport_tournament_rows(
            session,
            project_id,
            feed_sign,
            sport_id,
            category_id,
            tournament_id,
        )
        if not tournament_rows:
            continue
        for row_fields, row_blob in tournament_rows:
            if str(row_fields.get("PX", "")).strip() != participant_id:
                continue
            participant_rows.append((row_fields, row_blob))

    return participant_rows


def append_page_chunk(
    fields: dict[str, str],
    event_blob: str,
    page_chunks: list[tuple[dict[str, str], str]],
    page_game_fields: dict[str, dict[str, str]],
    page_game_blobs: dict[str, list[str]],
) -> None:
    page_chunks.append((fields, event_blob))
    game_id = fields.get("AA")
    if not game_id:
        return
    if game_id not in page_game_fields:
        page_game_fields[game_id] = {}
    merge_fields_by_priority(page_game_fields[game_id], fields)
    page_game_blobs.setdefault(game_id, []).append(event_blob)


def fetch_participant_feed_chunks(
    session: requests.Session,
    url: str,
    page_html: str,
    soup: BeautifulSoup,
) -> list[tuple[dict[str, str], str]]:
    participant_id = extract_participant_id(page_html) or extract_participant_id_from_url(url)
    if not participant_id:
        return []

    core_script_url, core_project_id = extract_core_script_context(soup, url)
    project_id = extract_project_id(page_html, core_project_id)
    feed_sign = extract_feed_sign(session, core_script_url)
    if not feed_sign:
        return []

    profile_payload = fetch_feed_payload(session, project_id, f"pl_1_{participant_id}_x", feed_sign)
    events_payload = fetch_feed_payload(session, project_id, f"pe_1_1_{participant_id}_x", feed_sign)
    if not events_payload:
        return []

    translation_map = build_translation_map([profile_payload, events_payload])
    chunks = [
        (fields, event_blob)
        for fields, event_blob in iter_contextual_rows_from_payload(events_payload, translation_map)
        if event_matches_participant(fields, participant_id)
    ]

    motorsport_chunks = fetch_motorsport_session_chunks(
        session,
        page_html,
        project_id,
        feed_sign,
        participant_id,
        chunks,
    )
    if motorsport_chunks:
        chunks.extend(motorsport_chunks)

    return chunks


def scrape_flashscore_url(
    url: str,
    session: Optional[requests.Session] = None,
) -> tuple[dict[str, dict[str, Any]], int, int, bool]:
    page_events: dict[str, dict[str, Any]] = {}
    za_detected_count = 0
    za_missing_count = 0

    own_session = session is None
    if session is None:
        session = requests.Session()
    try:
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
        except requests.RequestException as exc:
            print(f"Error al acceder a la página {url}: {exc}")
            return page_events, 0, 0, True

        page_html = response.text
        soup = BeautifulSoup(page_html, "html.parser")
        sport_name = extract_sport_name(soup)
        page_game_fields: dict[str, dict[str, str]] = {}
        page_game_blobs: dict[str, list[str]] = {}
        page_chunks: list[tuple[dict[str, str], str]] = []
        is_participant_url = "/jugador/" in url
        participant_id = ""
        if is_participant_url:
            participant_id = extract_participant_id(page_html) or extract_participant_id_from_url(url)

        if is_participant_url:
            participant_chunks = fetch_participant_feed_chunks(session, url, page_html, soup)
            if participant_chunks:
                print(f"Cargados {len(participant_chunks)} bloques dinámicos para {url}")
                for fields, event_blob in participant_chunks:
                    append_page_chunk(fields, event_blob, page_chunks, page_game_fields, page_game_blobs)
            else:
                # Fallback for player pages: parse static feed and keep only rows of the player.
                for script in soup.find_all("script"):
                    script_text = script.get_text() or ""
                    if FEED_MARKER not in script_text:
                        continue
                    for fields, event_blob in iter_feed_events_with_context(script_text):
                        if not event_matches_participant(fields, participant_id):
                            continue
                        append_page_chunk(fields, event_blob, page_chunks, page_game_fields, page_game_blobs)
        else:
            for script in soup.find_all("script"):
                script_text = script.get_text() or ""
                if FEED_MARKER not in script_text:
                    continue
                for fields, event_blob in iter_feed_events_with_context(script_text):
                    append_page_chunk(fields, event_blob, page_chunks, page_game_fields, page_game_blobs)

        league_marker_map = build_league_marker_map(page_chunks)
        for game_id, merged_fields in page_game_fields.items():
            merged_blob = "¬~".join(page_game_blobs.get(game_id, []))
            merged_fields = inject_league_from_markers(merged_fields, league_marker_map)
            detected_league = extract_league(merged_fields, merged_blob)
            if detected_league:
                za_detected_count += 1
                if not merged_fields.get("ZA", "").strip():
                    merged_fields = merged_fields.copy()
                    merged_fields["ZA"] = detected_league
            else:
                za_missing_count += 1

            event = build_event(merged_fields, sport_name, merged_blob)
            if not event:
                continue
            if game_id not in page_events:
                page_events[game_id] = event
            else:
                page_events[game_id] = merge_event_payload(page_events[game_id], event)

        return page_events, za_detected_count, za_missing_count, False
    finally:
        if own_session:
            session.close()


def scrape_flashscore_events(urls: list[str]) -> tuple[dict[str, dict[str, Any]], int]:
    gamelist: dict[str, dict[str, Any]] = {}
    failed_urls = 0
    za_detected_count = 0
    za_missing_count = 0
    unique_urls = list(dict.fromkeys(urls))
    max_workers = min(SCRAPE_MAX_WORKERS, len(unique_urls))

    def merge_page_events(page_events: dict[str, dict[str, Any]]) -> None:
        for game_id, event in page_events.items():
            if game_id not in gamelist:
                gamelist[game_id] = event
            else:
                gamelist[game_id] = merge_event_payload(gamelist[game_id], event)

    if max_workers <= 1:
        with requests.Session() as session:
            for url in unique_urls:
                page_events, page_za_detected, page_za_missing, failed = scrape_flashscore_url(url, session=session)
                if failed:
                    failed_urls += 1
                za_detected_count += page_za_detected
                za_missing_count += page_za_missing
                merge_page_events(page_events)
    else:
        thread_local_session = local()
        worker_sessions: list[requests.Session] = []
        worker_sessions_lock = Lock()

        def scrape_worker(worker_url: str) -> tuple[dict[str, dict[str, Any]], int, int, bool]:
            session = getattr(thread_local_session, "session", None)
            if session is None:
                session = requests.Session()
                thread_local_session.session = session
                with worker_sessions_lock:
                    worker_sessions.append(session)
            return scrape_flashscore_url(worker_url, session=session)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(scrape_worker, url): url for url in unique_urls}
            for future in as_completed(futures):
                url = futures[future]
                try:
                    page_events, page_za_detected, page_za_missing, failed = future.result()
                except Exception as exc:
                    print(f"Error al procesar la página {url}: {exc}")
                    failed_urls += 1
                    continue

                if failed:
                    failed_urls += 1
                za_detected_count += page_za_detected
                za_missing_count += page_za_missing
                merge_page_events(page_events)

        for session in worker_sessions:
            session.close()

    print(
        f"Diagnóstico ZA: detectado_en_evento={za_detected_count}, "
        f"sin_ZA_en_blob={za_missing_count}"
    )
    return gamelist, failed_urls


def merge_golf_events(gamelist: dict[str, dict[str, Any]]) -> None:
    def split_participant_names(value: Any) -> list[str]:
        participant_text = str(value or "")
        return [participant.strip() for participant in participant_text.split("/") if participant.strip()]

    def participant_key(name: str) -> str:
        normalized = unicodedata.normalize("NFKD", name)
        ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
        return re.sub(r"\s+", " ", ascii_text).strip().upper()

    golf_merged: dict[str, dict[str, Any]] = {}
    league_to_first_gameid: dict[str, str] = {}
    keys_to_remove: list[str] = []

    for game_id, event in list(gamelist.items()):
        if event.get("sports", "").upper() != "GOLF":
            continue

        league_val = event.get("league", "")
        if league_val in league_to_first_gameid:
            first_game_id = league_to_first_gameid[league_val]
            golf_merged[first_game_id]["team1"].extend(split_participant_names(event.get("team1", "")))
            keys_to_remove.append(game_id)
            continue

        event_copy = event.copy()
        event_copy["team1"] = split_participant_names(event_copy.get("team1", ""))
        golf_merged[game_id] = event_copy
        league_to_first_gameid[league_val] = game_id
        keys_to_remove.append(game_id)

    for key in keys_to_remove:
        gamelist.pop(key, None)

    for game_id, merged_event in golf_merged.items():
        unique_participants: list[str] = []
        seen_participants: set[str] = set()
        for participant_name in merged_event["team1"]:
            participant_name = str(participant_name).strip()
            if not participant_name:
                continue
            normalized_name = participant_key(participant_name)
            if normalized_name in seen_participants:
                continue
            seen_participants.add(normalized_name)
            unique_participants.append(participant_name)
        merged_event["team1"] = "/".join(unique_participants)
        gamelist[game_id] = merged_event


def should_merge_individual_competition_event(event: dict[str, Any]) -> bool:
    sport_name = str(event.get("sports", "")).strip().upper()
    if sport_name not in INDIVIDUAL_COMPETITION_MERGE_SPORTS:
        return False

    if str(event.get("team2", "")).strip():
        return False

    if not str(event.get("team1", "")).strip():
        return False

    if not normalize_league(event.get("league", "")):
        return False

    date_value = event.get("date")
    if isinstance(date_value, int):
        return True
    if isinstance(date_value, str) and date_value.isdigit():
        return True
    return False


def individual_competition_merge_key(event: dict[str, Any]) -> tuple[str, str, int, int]:
    sport_name = str(event.get("sports", "")).strip().upper()
    league_name = normalize_league(event.get("league", ""))
    date_start = int(event.get("date", 0))
    date_end_raw = event.get("date_end", 0)
    if isinstance(date_end_raw, int):
        date_end = date_end_raw
    elif isinstance(date_end_raw, str) and date_end_raw.isdigit():
        date_end = int(date_end_raw)
    else:
        date_end = 0
    return sport_name, league_name, date_start, date_end


def merge_individual_competition_events(gamelist: dict[str, dict[str, Any]]) -> None:
    grouped_events: dict[tuple[str, str, int, int], list[tuple[str, dict[str, Any]]]] = {}
    for game_id, event in gamelist.items():
        if not should_merge_individual_competition_event(event):
            continue
        merge_key = individual_competition_merge_key(event)
        grouped_events.setdefault(merge_key, []).append((game_id, event))

    merged_groups = 0
    removed_events = 0

    for grouped_list in grouped_events.values():
        if len(grouped_list) <= 1:
            continue

        base_game_id, base_event = grouped_list[0]
        merged_event = base_event.copy()

        participant_names: list[str] = []
        participant_rankings: list[str] = []
        merged_tv_channels: list[str] = []
        statuses: list[str] = []

        for _event_id, event in grouped_list:
            participant_name = str(event.get("team1", "")).strip()
            if participant_name and participant_name not in participant_names:
                participant_names.append(participant_name)

            participant_rank = str(event.get("rank_home", "")).strip() or str(event.get("rank_away", "")).strip()
            if participant_name and participant_rank:
                ranking_text = f"{participant_name} ({participant_rank})"
                if ranking_text not in participant_rankings:
                    participant_rankings.append(ranking_text)

            tv_channels = event.get("tv", [])
            if isinstance(tv_channels, list):
                for channel in tv_channels:
                    channel_name = str(channel).strip()
                    if channel_name and channel_name not in merged_tv_channels:
                        merged_tv_channels.append(channel_name)

            status = str(event.get("status", "")).strip().upper()
            if status:
                statuses.append(status)

        if participant_names:
            merged_event["team1"] = "/".join(participant_names)
        merged_event.pop("team2", None)
        merged_event.pop("rank_home", None)
        merged_event.pop("rank_away", None)
        if participant_rankings:
            merged_event["participant_rankings"] = participant_rankings
        else:
            merged_event.pop("participant_rankings", None)

        if merged_tv_channels:
            merged_event["tv"] = merged_tv_channels
        else:
            merged_event.pop("tv", None)

        if statuses and all(status == "CANCELLED" for status in statuses):
            merged_event["status"] = "CANCELLED"
        else:
            merged_event["status"] = "CONFIRMED"

        for game_id, _event in grouped_list[1:]:
            gamelist.pop(game_id, None)
            removed_events += 1

        gamelist[base_game_id] = merged_event
        merged_groups += 1

    if merged_groups:
        print(
            "Fusión de competiciones individuales: "
            f"grupos={merged_groups}, eventos_eliminados={removed_events}"
        )


def event_timestamp(event: dict[str, Any], key: str = "date") -> int:
    value = event.get(key, 0)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return 0


def normalize_matchup_key(team1: str, team2: str) -> tuple[str, str]:
    home = team1.strip().upper()
    away = team2.strip().upper()
    return tuple(sorted((home, away)))


def competition_supports_first_leg(event: dict[str, Any]) -> bool:
    result_status = str(event.get("result_status", "")).upper()
    if "PRIMER PARTIDO" in result_status or "IDA" in result_status or "FIRST LEG" in result_status:
        return True

    league_name = normalize_league(event.get("league", "")).upper()
    return any(hint in league_name for hint in TWO_LEG_COMPETITION_HINTS)


def build_scoreline_text(event: dict[str, Any]) -> str:
    team1 = str(event.get("team1", "")).strip()
    team2 = str(event.get("team2", "")).strip()
    score_home = event.get("score_home")
    score_away = event.get("score_away")
    if not team1 or not team2 or score_home is None or score_away is None:
        return ""
    return f"{team1} {score_home}\u2013{score_away} {team2}"


def infer_first_leg_from_previous_events(
    event: dict[str, Any],
    events_by_league_and_pair: dict[tuple[str, tuple[str, str]], list[dict[str, Any]]],
) -> str:
    team1 = str(event.get("team1", "")).strip()
    team2 = str(event.get("team2", "")).strip()
    if not team1 or not team2:
        return ""

    league_name = normalize_league(event.get("league", ""))
    if not league_name:
        return ""

    pair_key = normalize_matchup_key(team1, team2)
    bucket_key = (league_name.upper(), pair_key)
    current_ts = event_timestamp(event)
    if current_ts <= 0:
        return ""

    candidate_events = events_by_league_and_pair.get(bucket_key, [])
    previous_with_score = [
        item
        for item in candidate_events
        if event_timestamp(item) < current_ts and build_scoreline_text(item)
    ]
    if not previous_with_score:
        return ""

    previous_with_score.sort(key=event_timestamp)
    return build_scoreline_text(previous_with_score[-1])


def annotate_first_leg_results(gamelist: dict[str, dict[str, Any]]) -> None:
    grouped_events: dict[tuple[str, tuple[str, str]], list[dict[str, Any]]] = {}
    for event in gamelist.values():
        team1 = str(event.get("team1", "")).strip()
        team2 = str(event.get("team2", "")).strip()
        league_name = normalize_league(event.get("league", ""))
        if not team1 or not team2 or not league_name:
            continue
        pair_key = normalize_matchup_key(team1, team2)
        grouped_events.setdefault((league_name.upper(), pair_key), []).append(event)

    for event in gamelist.values():
        first_leg_text = normalize_score_text(str(event.get("first_leg_result", "")).strip())
        if not first_leg_text:
            first_leg_text = extract_first_leg_from_text(str(event.get("result_status", "")))

        if not first_leg_text and competition_supports_first_leg(event):
            first_leg_text = infer_first_leg_from_previous_events(event, grouped_events)

        if first_leg_text:
            event["first_leg_result"] = first_leg_text
        else:
            event.pop("first_leg_result", None)


def event_datetime_utc(event: dict[str, Any], key: str = "date") -> datetime:
    return datetime.fromtimestamp(event_timestamp(event, key), tz=UTC)


def update_obsolete_links(gamelist: dict[str, dict[str, Any]], obsolete_file: Path) -> None:
    obsolete_links = set(load_pickle(obsolete_file, []))
    obsolete_cutoff = datetime.now(tz=UTC) - timedelta(weeks=4)

    for event in gamelist.values():
        url = event.get("url", "")
        if not url:
            continue

        timestamp_key = "date_end" if "date_end" in event else "date"
        if event_datetime_utc(event, timestamp_key) <= obsolete_cutoff:
            obsolete_links.add(url)

    save_pickle(obsolete_file, sorted(obsolete_links))


def enrich_event_with_existing_data(event: dict[str, Any], existing_event: dict[str, Any]) -> dict[str, Any]:
    enriched = event.copy()

    if not normalize_league(enriched.get("league", "")):
        existing_league = normalize_league(existing_event.get("league", ""))
        if existing_league:
            enriched["league"] = existing_league

    for key in (
        "team1",
        "team2",
        "result_status",
        "url",
        "rank_home",
        "rank_away",
        "first_leg_result",
        "red_cards_home",
        "red_cards_away",
    ):
        if not str(enriched.get(key, "")).strip() and str(existing_event.get(key, "")).strip():
            enriched[key] = existing_event[key]

    if "date_end" not in enriched and "date_end" in existing_event:
        enriched["date_end"] = existing_event["date_end"]

    if "score_home" not in enriched and "score_home" in existing_event:
        enriched["score_home"] = existing_event["score_home"]
    if "score_away" not in enriched and "score_away" in existing_event:
        enriched["score_away"] = existing_event["score_away"]

    current_tv = enriched.get("tv", [])
    existing_tv = existing_event.get("tv", [])
    if isinstance(current_tv, list) and isinstance(existing_tv, list):
        merged_tv = list(dict.fromkeys(current_tv + existing_tv))
        if merged_tv:
            enriched["tv"] = merged_tv
    elif not current_tv and existing_tv:
        enriched["tv"] = existing_tv

    current_rankings = enriched.get("participant_rankings", [])
    existing_rankings = existing_event.get("participant_rankings", [])
    if isinstance(current_rankings, list) and isinstance(existing_rankings, list):
        merged_rankings = list(
            dict.fromkeys(str(item).strip() for item in (current_rankings + existing_rankings) if str(item).strip())
        )
        if merged_rankings:
            enriched["participant_rankings"] = merged_rankings
    elif not current_rankings and isinstance(existing_rankings, list) and existing_rankings:
        enriched["participant_rankings"] = existing_rankings

    return enriched


def merge_with_existing_events(
    gamelist: dict[str, dict[str, Any]],
    pickle_file: Path,
    past_results_days: int = PAST_RESULTS_DAYS,
    mark_cancellations: bool = True,
    use_existing_events: bool = True,
) -> dict[str, dict[str, Any]]:
    now_utc = datetime.now(tz=UTC)
    lookback_cutoff = (now_utc - timedelta(days=max(0, past_results_days))).date()

    gamelist = {
        key: value
        for key, value in gamelist.items()
        if event_datetime_utc(value).date() >= lookback_cutoff
    }

    existing_events: dict[str, dict[str, Any]] = load_pickle(pickle_file, {})
    if existing_events:
        for key, value in list(gamelist.items()):
            existing_event = existing_events.get(key)
            if not existing_event:
                continue
            gamelist[key] = enrich_event_with_existing_data(value, existing_event)

    if existing_events and use_existing_events:
        future_existing = {
            key: value
            for key, value in existing_events.items()
            if event_datetime_utc(value) > now_utc
        }
        if mark_cancellations:
            cancelled_keys = set(future_existing) - set(gamelist)
            for key in cancelled_keys:
                existing_events[key]["status"] = "CANCELLED"

        gamelist = {**existing_events, **gamelist}

    merged = {
        key: value
        for key, value in gamelist.items()
        if event_datetime_utc(value).date() >= lookback_cutoff
    }

    for event in merged.values():
        event["league"] = normalize_league(event.get("league", ""))

    return merged


def build_event_name(event: dict[str, Any], description: str = "") -> str:
    def team_with_red_card(team_name: Any, red_cards_value: Any) -> str:
        team_text = str(team_name or "").strip()
        if not team_text:
            return ""
        red_cards = parse_counter_value(red_cards_value) or 0
        if red_cards <= 0:
            return team_text
        if red_cards == 1:
            return f"{team_text} 🟥"
        return f"{team_text} 🟥x{red_cards}"

    parts: list[str] = []
    sport = event.get("sports", "")
    if sport:
        parts.append(f"{sport}: ")

    name = "".join(parts)
    team1 = team_with_red_card(event.get("team1"), event.get("red_cards_home"))
    team2 = team_with_red_card(event.get("team2"), event.get("red_cards_away"))
    score_home = event.get("score_home")
    score_away = event.get("score_away")

    if team1 and team2 and score_home is not None and score_away is not None:
        name += f"{team1} {score_home}\u2013{score_away} {team2}"
    else:
        if team1:
            name += team1
        if team2:
            name += f" \u2013 {team2}"

    league = competition_override(event)
    if not league:
        league = competition_text_for_summary(event)
    if not league and description:
        league = competition_from_description(description)
    if not league:
        league = infer_competition_from_context(event)
    if not league and LEAGUE_FALLBACK_TEXT:
        league = LEAGUE_FALLBACK_TEXT
    if league and (team1 or team2):
        return f"{name} / {league}"
    if league:
        return f"{name}{league}"
    return name.rstrip()


def build_classification_description(event: dict[str, Any]) -> str:
    participant_rankings = event.get("participant_rankings", [])
    if isinstance(participant_rankings, list):
        normalized_rankings = [str(item).strip() for item in participant_rankings if str(item).strip()]
    else:
        normalized_rankings = []

    competition_name = (
        competition_override(event)
        or competition_text_for_summary(event)
        or normalize_league(event.get("league", ""))
    )
    prefix = "Clasificación"
    if competition_name:
        prefix = f"{prefix} ({competition_name})"

    if normalized_rankings:
        return f"{prefix}: {', '.join(normalized_rankings)}"

    team1 = str(event.get("team1", "")).strip()
    team2 = str(event.get("team2", "")).strip()
    rank_home = str(event.get("rank_home", "")).strip()
    rank_away = str(event.get("rank_away", "")).strip()

    if not rank_home and not rank_away:
        return ""

    team1_text = f"{team1} ({rank_home})" if team1 and rank_home else team1
    team2_text = f"{team2} ({rank_away})" if team2 and rank_away else team2
    if team1_text and team2_text:
        return f"{prefix}: {team1_text} - {team2_text}"
    if team1_text:
        return f"{prefix}: {team1_text}"
    if team2_text:
        return f"{prefix}: {team2_text}"
    return ""


def build_first_leg_description(event: dict[str, Any]) -> str:
    first_leg_text = normalize_score_text(str(event.get("first_leg_result", "")).strip())
    if not first_leg_text:
        first_leg_text = extract_first_leg_from_text(str(event.get("result_status", "")))
    if not first_leg_text:
        return ""
    return f"Ida: {first_leg_text}"


def build_red_cards_description(event: dict[str, Any]) -> str:
    sport_name = str(event.get("sports", "")).strip().upper()
    if sport_name not in {"FÚTBOL", "FUTBOL"}:
        return ""

    home_cards_raw = event.get("red_cards_home")
    away_cards_raw = event.get("red_cards_away")
    home_cards = parse_counter_value(home_cards_raw) or 0
    away_cards = parse_counter_value(away_cards_raw) or 0
    if home_cards <= 0 and away_cards <= 0:
        return ""

    team1 = str(event.get("team1", "")).strip()
    team2 = str(event.get("team2", "")).strip()

    parts: list[str] = []
    if home_cards > 0:
        label = team1 if team1 else "Local"
        parts.append(f"{label} {home_cards}")
    if away_cards > 0:
        label = team2 if team2 else "Visitante"
        parts.append(f"{label} {away_cards}")
    if not parts:
        return ""

    return f"Expulsiones: {', '.join(parts)}"


def build_description(event: dict[str, Any]) -> str:
    description_parts: list[str] = []

    classification_text = build_classification_description(event)
    if classification_text:
        description_parts.append(classification_text)

    first_leg_text = build_first_leg_description(event)
    if first_leg_text:
        description_parts.append(first_leg_text)

    red_cards_text = build_red_cards_description(event)
    if red_cards_text:
        description_parts.append(red_cards_text)

    tv_channels = event.get("tv", [])
    if tv_channels:
        description_parts.append(f"TV: {', '.join(tv_channels)}")

    return " / ".join(description_parts)


def infer_duration(name: str) -> timedelta:
    upper_name = name.upper()

    if any(keyword in upper_name for keyword in ("MOTOGP", "MOTO2", "MOTO3", "MOTO-E")):
        if "ENTRENAMIENTOS" in upper_name:
            return timedelta(minutes=30)
        if "CARRERA" in upper_name:
            return timedelta(minutes=60)
        if "CLASIFICACIÓN" in upper_name:
            return timedelta(minutes=20)
        if "WARM UP" in upper_name:
            return timedelta(minutes=30)
        return timedelta(minutes=60)

    if "VUELTAS" in upper_name:
        return timedelta(hours=6)
    if "WRC" in upper_name:
        return timedelta(minutes=60)
    if "3X3" in upper_name:
        return timedelta(minutes=30)
    if "CLASIFICACIÓN " in upper_name and not any(
        blocked in upper_name for blocked in ("BALONCESTO", "WATERPOLO", "BALONMANO")
    ):
        return timedelta(minutes=20)
    if "FÓRMULA 1" in upper_name and "CLASIFICACIÓN" in upper_name:
        return timedelta(minutes=62)
    if "ENTRENAMIENTOS" in upper_name:
        if "WEC" in upper_name:
            return timedelta(hours=4)
        return timedelta(minutes=60)
    if "VOLEY PLAYA" in upper_name:
        return timedelta(minutes=60)

    if "WEC" in upper_name and "CARRERA" in upper_name:
        if "24 HOURS" in upper_name:
            return timedelta(hours=24)
        if "8 HOURS" in upper_name:
            return timedelta(hours=8)
        return timedelta(hours=6)

    return timedelta(hours=2)


def should_extend_overrun_event(
    event: dict[str, Any],
    inferred_duration: timedelta,
    now_utc: datetime,
) -> bool:
    if "date_end" in event:
        return False
    if event.get("all_day", False):
        return False
    if str(event.get("status", "")).strip().upper() == "CANCELLED":
        return False

    sport_name = str(event.get("sports", "")).strip().upper()
    if sport_name not in OVERRUN_EXTENSION_SPORTS:
        return False

    has_opponent = bool(str(event.get("team2", "")).strip())
    if not has_opponent:
        # For individual competitions, a ranking/position means result published.
        if str(event.get("rank_home", "")).strip() or str(event.get("rank_away", "")).strip():
            return False
        participant_rankings = event.get("participant_rankings", [])
        if isinstance(participant_rankings, list) and any(str(item).strip() for item in participant_rankings):
            return False

    start_utc = event_datetime_utc(event, "date")
    if start_utc > now_utc:
        return False

    planned_end_utc = start_utc + inferred_duration
    if now_utc <= planned_end_utc:
        return False

    return (now_utc - planned_end_utc) <= timedelta(hours=OVERRUN_EXTENSION_MAX_HOURS)


def apply_end_or_duration(calendar_event: Event, event: dict[str, Any], name: str) -> None:
    inferred_duration = infer_duration(name)
    if "date_end" not in event:
        now_utc = datetime.now(tz=UTC)
        if should_extend_overrun_event(event, inferred_duration, now_utc):
            calendar_event.end = now_utc + timedelta(minutes=OVERRUN_EXTENSION_MINUTES)
        else:
            calendar_event.duration = inferred_duration
        return

    start = event_datetime_utc(event, "date")
    end = event_datetime_utc(event, "date_end")
    duration = end - start

    if duration.days >= 1:
        if not event.get("all_day", False):
            calendar_event.duration = timedelta(hours=12)
            calendar_event.extra.append(
                ContentLine(name="RRULE", value=f"FREQ=DAILY;COUNT={duration.days + 1}")
            )
    else:
        calendar_event.end = end


def build_calendar(gamelist: dict[str, dict[str, Any]]) -> Calendar:
    calendar = Calendar()

    sorted_events = sorted(gamelist.values(), key=lambda item: (item.get("date", 0), item.get("gameid", "")))
    for event in sorted_events:
        calendar_event = Event()
        calendar_event.uid = event["gameid"]
        description = build_description(event)
        calendar_event.description = description
        calendar_event.begin = event_datetime_utc(event)
        calendar_event.alarms.append(DisplayAlarm(trigger=timedelta(minutes=-15)))

        name = build_event_name(event, description)
        calendar_event.name = name
        calendar_event.status = event.get("status", "CONFIRMED")

        if event.get("all_day", False):
            calendar_event.make_all_day()

        apply_end_or_duration(calendar_event, event, name)
        calendar.events.add(calendar_event)

    return calendar


def save_calendar(calendar: Calendar, calendar_file: Path) -> None:
    calendar_file.parent.mkdir(parents=True, exist_ok=True)
    # ics==0.7.x serializes alarms via str(Component), which emits a known FutureWarning.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=(
                r"Behaviour of str\(Component\) will change in version 0\.9 "
                r"to only return a short description, NOT the ics representation\. "
                r"Use the explicit Component\.serialize\(\) to get the ics representation\."
            ),
            category=FutureWarning,
            module=r"ics\.component",
        )
        serialized_calendar = calendar.serialize()
    with calendar_file.open("w", encoding="utf-8") as handle:
        handle.write(serialized_calendar)


def main() -> None:
    with ThreadPoolExecutor(max_workers=2) as executor:
        flashscore_future = executor.submit(scrape_flashscore_events, FLASHSCORE_URLS)
        futboleras_future = executor.submit(scrape_futboleras_events, FUTBOLERAS_TEAM_URLS)
        gamelist, flashscore_failed_urls = flashscore_future.result()
        futboleras_events, futboleras_failed_urls = futboleras_future.result()
    if futboleras_events:
        for gameid, event in futboleras_events.items():
            if gameid not in gamelist:
                gamelist[gameid] = event
            else:
                gamelist[gameid] = merge_event_payload(gamelist[gameid], event)
        print(f"Eventos añadidos desde Futboleras: {len(futboleras_events)}")

    if not gamelist:
        print("No se han obtenido eventos nuevos. Se conserva el calendario anterior.")
        gamelist = load_pickle(PICKLE_FILE, {})
        if not gamelist:
            print("No hay datos previos para generar el calendario.")
            return
        calendar = build_calendar(gamelist)
        save_calendar(calendar, CALENDAR_FILE)
        return

    merge_golf_events(gamelist)
    merge_individual_competition_events(gamelist)
    update_obsolete_links(gamelist, OBSOLETE_FILE)

    mark_cancellations = flashscore_failed_urls == 0
    use_existing_events = flashscore_failed_urls != 0
    if not mark_cancellations:
        print("Scraping incompleto: se omite marcación de eventos cancelados en esta ejecución.")
    if futboleras_failed_urls:
        print(
            "Scraping Futboleras incompleto: "
            f"{futboleras_failed_urls} URL(s) no disponibles en esta ejecución."
        )
    if not use_existing_events:
        print("Scraping completo: se reconstruye el calendario con datos frescos.")
    gamelist = merge_with_existing_events(
        gamelist,
        PICKLE_FILE,
        mark_cancellations=mark_cancellations,
        use_existing_events=use_existing_events,
    )
    annotate_first_leg_results(gamelist)
    enrich_events_with_classification(gamelist, CLASSIFICATION_CACHE_FILE)

    save_pickle(PICKLE_FILE, gamelist)
    calendar = build_calendar(gamelist)
    save_calendar(calendar, CALENDAR_FILE)


if __name__ == "__main__":
    main()
