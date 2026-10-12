"""
Daily scraper for https://www.frauenhaus-suche.de/  (wdr-ddj-cloud scraper)

Collects one row per Frauenhaus / Schutzwohnung into a pandas DataFrame and
uploads it as a dated CSV file via the project's `upload_dataframe` helper.
The framework calls `run()` (bottom of this file) on the schedule configured
in scrapers_config.json.

Columns
-------
scrape_date, scrape_time, frauenhaus_id, frauenhaus_name, frauenhaus_status,
ort, bundesland, traegerin, art_der_einrichtung, aufnahmemoeglichkeit

Contact details (phone, e-mail) are deliberately NOT
scraped.

How the file is organised (top to bottom)
-----------------------------------------
1. Imports
2. Configuration
3. Small helpers (text cleaning, time)
4. Download (one HTTP request)
5. Parsing (turn the HTML into a list of dicts, one per house)
6. Validation and DataFrame
7. Entry point: run()
"""

# makes type hints like `str | None` work on Python 3.9 as well.
from __future__ import annotations

# --------------------------------------------------------------------------- #
# 1. Imports
# --------------------------------------------------------------------------- #
import logging  # timestamped INFO/WARNING/ERROR messages
import re  # regular expressions for text matching
import unicodedata  # normalises umlauts so "ä" is always the same character
from bisect import bisect_left  # fast "nearest earlier position" lookup
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError  # time zone handling

import pandas as pd  
import requests  # HTTP download
from bs4 import BeautifulSoup, NavigableString, Tag  # HTML parsing
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry  # automatic retries on network errors

from ddj_cloud.utils.storage import upload_dataframe  # project helper: DataFrame -> S3

# --------------------------------------------------------------------------- #
# 2. Configuration
# --------------------------------------------------------------------------- #

URL = "https://www.frauenhaus-suche.de/"
TZ_NAME = "Europe/Berlin"  # scrape_date / scrape_time are given in this time zone

# one CSV per day, all in one folder of the project's bucket. Daily files
# never overwrite each other, so the history builds up by itself (and a failed
# day cannot damage older days). Re-running on the same day replaces that day's file.
OUTPUT_FOLDER = "frauenhaus_suche_scraper"
USER_AGENT = "Mozilla/5.0 (compatible; frauenhaus-request/1.0)"

# Every detail block on the page starts with this sub-heading.
# "anchor" to find the start of each house's information.
MARKER_TEXT = "Allgemeine Informationen"

# HTML tags that hold the house name ("Frauenhaus Arnsberg") and the
# "Ort, Bundesland" line. If the site changes its heading levels, change them here.
NAME_TAG = "h1"
LOCATION_TAG = "h2"
HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}

# the status (green/grey/red/purple) is only shown as an icon, not as text,
# so we translate the icon's file name into the status text from the site's legend.
STATUS_BY_ICON = {
    "available_green": "Aufnahme möglich",
    "unknown_grey": "Ohne Angabe",
    "occupied_red": "Keine Aufnahme möglich",
    "emergency-room_purple": "Sofortaufnahme/Notaufnahme",
}
# Regex that recognises one of the four icon file names inside an image URL.
ICON_RE = re.compile("(" + "|".join(map(re.escape, STATUS_BY_ICON)) + r")\.svg", re.I)

# every house has its own numeric ID inside the HTML. The detail
# block is <div id="modal_2182"> and the matching list entry is
# <div id="resultItem_2182">. We use this ID as frauenhaus_id.
MODAL_ID_RE = re.compile(r"modal_(\d+)")
ITEM_ID_RE = re.compile(r"resultItem_(\d+)")

# each list entry also carries its status as a CSS class (e.g. "orderRed").
# NOT the main source (the icon is), but independent
# cross-check: if icon and CSS class ever disagree, the run is aborted.
STATUS_BY_CSS_CLASS = {
    "orderGreen": "Aufnahme möglich",
    "orderGrey": "Ohne Angabe",
    "orderRed": "Keine Aufnahme möglich",
    "orderPurple": "Sofortaufnahme/Notaufnahme",
}

# used only as a sanity check. If the site suddenly returns something that
# is not a German state, we get a warning instead of silently storing junk.
BUNDESLAENDER = {
    "Baden-Württemberg",
    "Bayern",
    "Berlin",
    "Brandenburg",
    "Bremen",
    "Hamburg",
    "Hessen",
    "Mecklenburg-Vorpommern",
    "Niedersachsen",
    "Nordrhein-Westfalen",
    "Rheinland-Pfalz",
    "Saarland",
    "Sachsen",
    "Sachsen-Anhalt",
    "Schleswig-Holstein",
    "Thüringen",
    "international",
}

# The four fields we read from the text of a detail block ("Label: value").
TARGET_LABELS = ("Ort", "Trägerin", "Art der Einrichtung", "Aufnahmemöglichkeit")
# These two can contain several entries (bullet lists); we join them with "; ".
LIST_LABELS = {"Art der Einrichtung", "Aufnahmemöglichkeit"}
# Matches "Ort: Arnsberg" when label and value are in the same text node.
INLINE_LABEL_RE = re.compile(r"^(" + "|".join(map(re.escape, TARGET_LABELS)) + r"):\s+(\S.*)$")
# The section after the fields we need. Reading stops here so that free-text
# comments ("Wichtige Informationen") can never be mistaken for a field.
STOP_TEXT = "Weiteres"
MAX_TOKENS_PER_RECORD = 500  # safety cap so a broken page can't make us read forever
MAX_LABEL_LENGTH = 80  # a text ending in ":" and at most this long counts as a label

# If more than 5 % of the records lack in frauenhaus:status, ort, bundesland, art_der_einrichtung, the run is aborted. Normally all are filled in 
MAX_MISSING_SHARE = 0.05

# explicit list of columns and types to guarantee the same layout every
# day. 

DTYPES = {
    "scrape_date": "object",
    "scrape_time": "object",
    "frauenhaus_id": "Int64",
    "frauenhaus_name": "object",
    "frauenhaus_status": "object",
    "ort": "object",
    "bundesland": "object",
    "traegerin": "object",
    "art_der_einrichtung": "object",
    "aufnahmemoeglichkeit": "object",
}
COLUMNS = list(DTYPES)

log = logging.getLogger("frauenhaus")
log.setLevel(logging.INFO)  # AWS Lambda hides INFO messages unless the logger asks for them


# --------------------------------------------------------------------------- #
# 3. Small helpers
# --------------------------------------------------------------------------- #
def clean(text: str) -> str:
    """Normalise unicode, collapse all whitespace (incl. non-breaking spaces), strip."""
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", text)).strip()


def now_berlin() -> datetime:
    """Current time """
    try:
        return datetime.now(ZoneInfo(TZ_NAME)).replace(microsecond=0, tzinfo=None)
    except ZoneInfoNotFoundError:
        # Windows has no time zone database unless `tzdata` is installed.
        log.warning("Time zone database missing (pip install tzdata); using local time.")
        return datetime.now().replace(microsecond=0)


# --------------------------------------------------------------------------- #
# 4. Download
# --------------------------------------------------------------------------- #
def fetch_html(url: str = URL) -> str:
    """Download the page once and return it as text.  """
    retry = Retry(
        total=4,  # up to 4 retries
        backoff_factor=2,  # wait 2s, 4s, 8s, ...
        status_forcelist=(429, 500, 502, 503, 504),  # retry on these server errors
        allowed_methods=frozenset({"GET"}),
    )
    with requests.Session() as session:
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.mount("http://", HTTPAdapter(max_retries=retry))
        session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "de-DE,de;q=0.9"})
        response = session.get(url, timeout=(10, 60))  # (connect, read) seconds
        response.raise_for_status()  # turn HTTP errors into exceptions
        response.encoding = "utf-8"  # page is UTF-8; avoids garbled umlauts
        return response.text


# --------------------------------------------------------------------------- #
# 5. Parsing
# --------------------------------------------------------------------------- #
def status_of_img(img: Tag) -> str | None:
    """Return the status text for the four traffic-light status icons  """
    src = img.get("src") or img.get("data-src") or ""  # data-src = lazy-loaded images
    m = ICON_RE.search(src)
    if m:
        return STATUS_BY_ICON[m.group(1).lower()]
    # Fallback: some images carry the status as alt/title text instead.
    for attr in ("alt", "title"):
        val = clean(img.get(attr) or "")
        if val in STATUS_BY_ICON.values():
            return val
    return None


def iter_tokens(marker: NavigableString, next_marker: NavigableString | None):
    """Yield (heading_tag_or_None, text) for each text snippet after `marker`.

    we don't know the exact HTML tags around the labels and values, so we
    read the *text in page order* instead. That is more robust than depending
    on specific tags or CSS classes. Reading stops when the next house begins
    (next marker, or next name heading) so data never leaks between houses.
    """
    count = 0
    for el in marker.next_elements:  # walk forward through the document
        if el is next_marker:  # next house's block starts -> stop
            return
        if isinstance(el, Tag):
            if el.name == NAME_TAG:  # next house's name heading -> stop
                return
            continue  # other tags carry no text themselves
        if type(el) is not NavigableString:  # skip HTML comments, doctype, CDATA
            continue
        if el.parent is not None and el.parent.name in ("script", "style", "noscript"):
            continue  # skip code that is not visible text
        text = clean(str(el))
        if not text:  # skip pure whitespace
            continue
        # Remember whether this text sits inside a heading (h1..h6); headings
        # mark section boundaries in parse_fields().
        heading = next((p.name for p in el.parents if p.name in HEADING_TAGS), None)
        yield heading, text
        count += 1
        if count >= MAX_TOKENS_PER_RECORD:  # safety cap
            return


def parse_fields(tokens) -> dict[str, list[str]]:
    """Turn the text stream of one house into {label: [value lines]}.

    the detail block is a sequence like "Ort:" / "Arnsberg" / "Trägerin:" /
    "Frauen helfen Frauen e.V." ... This function pairs each label with the
    text that follows it, until the next label or section heading. Only the
    first occurrence of each wanted label is kept.
    """
    fields: dict[str, list[str]] = {}
    label: str | None = None  # label we are currently collecting values for
    parts: list[str] = []  # value lines collected so far

    def store() -> None:
        # Save the finished label (only if we want it and don't have it yet).
        if label in TARGET_LABELS and label not in fields:
            fields[label] = parts

    for heading, text in tokens:
        if text == STOP_TEXT:  # reached "Weiteres" -> all needed fields are done
            break
        if heading is not None:  # a section heading ends the current label
            store()
            label, parts = None, []
            continue
        m = INLINE_LABEL_RE.match(text)
        if m:  # "Ort: Arnsberg" in one piece
            store()
            label, parts = m.group(1), [m.group(2)]
        elif text.endswith(":") and len(text) <= MAX_LABEL_LENGTH:  # a label such as "Trägerin:"
            store()
            label, parts = text[:-1].strip(), []
        elif label is not None:  # ordinary value text -> belongs to current label
            parts.append(text)
    store()  # don't forget the last label
    return fields


def join_value(parts: list[str] | None, joiner: str) -> str | None:
    """Join value lines into one string; return None if there is nothing.

    the site writes "–" for "no information". We store that as a proper
    missing value (null) instead of a dash, so later filtering works cleanly.
    """
    if not parts:
        return None
    parts = [p for p in parts if p not in {"-", "–", "—"}]
    return joiner.join(parts) or None


def split_location(text: str) -> tuple[str, str | None]:
    """'Bad Hersfeld, Hessen' -> ('Bad Hersfeld', 'Hessen').

    the Bundesland is only given inside the "Ort, Bundesland" heading.
    We split at the LAST comma because a city name could contain a comma
    itself (e.g. "Frankfurt, Main"), while a Bundesland never does.
    """
    if "," in text:
        ort, bundesland = text.rsplit(",", 1)
        return clean(ort), clean(bundesland) or None
    return clean(text), None


def site_id_of(marker: NavigableString) -> int | None:
    """Return the site's numeric ID of the house that `marker` belongs to.

    he detail block sits inside <div id="modal_2182">. Walking up the
    parents until we find that id gives us the house's own ID, which stays the
    same from day to day.
    """
    for parent in marker.parents:
        m_id = MODAL_ID_RE.fullmatch(parent.get("id") or "")
        if m_id:
            return int(m_id.group(1))
    return None


def parse_page(html: str) -> tuple[list[dict], int | None]:
    """Parse the whole page.

    Returns (records, announced_count): one dict per house, and the number from
    'Ihre Suche ergab N Treffer' (or None if not found).
    """
    soup = BeautifulSoup(html, "html.parser")

    # --- Block A: document-order index of every tag ------------------------
    # later we need to ask "does element X come after element Y on the
    # page?" (e.g. to make sure an icon belongs to *this* house and not the
    # previous one). Numbering all tags in page order makes that a cheap comparison.
    pos = {id(t): i for i, t in enumerate(soup.find_all(True))}

    # --- Block B: announced hit count --------------------------------------
    # the page itself says "Ihre Suche ergab 352 Treffer". If we parse a
    # different number of houses, something is wrong and we must not save.
    m = re.search(r"Ihre Suche ergab\s+(\d+)\s+Treffer", soup.get_text(" "))
    announced = int(m.group(1)) if m else None
    # That sentence also marks where the result list begins. It is the lower
    # bound for the first house, so the legend icons at the top of the page
    # (green/grey/red/purple explanation) are never mistaken for a house's status.
    treffer_node = soup.find(string=re.compile(r"Ihre Suche ergab"))
    first_lower = pos.get(id(treffer_node.parent), -1) if treffer_node is not None else -1

    # --- Block C: collect all status icons ---------------------------------
    # We list every status icon with
    # its page position once, so each house can look up "the nearest status
    # icon before my name" quickly.
    icon_pos: list[int] = []
    icon_status: list[str] = []
    for img in soup.find_all("img"):
        st = status_of_img(img)
        if st:
            icon_pos.append(pos[id(img)])  # ascending order, required by bisect
            icon_status.append(st)

    # --- Block D: find one anchor per house --------------------------------
    # each house has exactly one text "Allgemeine Informationen". These
    # anchors tell us how many houses there are and where each one starts.
    markers = [
        s
        for s in soup.find_all(string=True)
        if type(s) is NavigableString and clean(str(s)) == MARKER_TEXT
    ]
    log.info("Found %d detail blocks (announced: %s)", len(markers), announced)

    # --- Block D2: status according to the CSS class of each list entry ----
    # a second, independent source for the status (see STATUS_BY_CSS_CLASS).
    # Result: {site_id: status_text}.
    css_status: dict[int, str] = {}
    for div in soup.find_all("div", id=ITEM_ID_RE):
        m_item = ITEM_ID_RE.fullmatch(div.get("id") or "")
        if not m_item:
            continue
        for cls in div.get("class", []):
            if cls in STATUS_BY_CSS_CLASS:
                css_status[int(m_item.group(1))] = STATUS_BY_CSS_CLASS[cls]

    # --- Block E: build one record per house -------------------------------
    records: list[dict] = []
    for i, marker in enumerate(markers):
        anchor = marker.parent
        next_marker = markers[i + 1] if i + 1 < len(markers) else None
        # Everything belonging to this house lies AFTER the previous house's
        # anchor. `lower` enforces that, so nothing is taken from the wrong house.
        lower = pos[id(markers[i - 1].parent)] if i > 0 else first_lower

        # E0: the site's own ID (see site_id_of()).
        site_id = site_id_of(marker)

        # E1: name and "Ort, Bundesland" are the headings just before the anchor.
        loc_tag = anchor.find_previous(LOCATION_TAG)
        if loc_tag is not None and pos.get(id(loc_tag), -1) <= lower:
            loc_tag = None  # found heading belongs to an earlier house -> reject
        name_tag = loc_tag.find_previous(NAME_TAG) if loc_tag is not None else None
        if name_tag is not None and pos.get(id(name_tag), -1) <= lower:
            name_tag = None

        name = clean(name_tag.get_text(" ")) if name_tag is not None else ""
        ort_h2, bundesland = (
            split_location(clean(loc_tag.get_text(" "))) if loc_tag is not None else (None, None)
        )

        # E2: status = the nearest status icon in front of the name heading.
        status = None
        if name_tag is not None:
            k = bisect_left(icon_pos, pos[id(name_tag)]) - 1  # last icon before the name
            if k >= 0 and icon_pos[k] > lower:  # ...and inside this house's range
                status = icon_status[k]

        # E3: labelled fields (Ort, Trägerin, Art der Einrichtung, Aufnahmemöglichkeit).
        fields = parse_fields(iter_tokens(marker, next_marker))
        # Prefer the "Ort:" field; fall back to the heading if it is missing.
        ort = join_value(fields.get("Ort"), " ") or ort_h2

        records.append(
            {
                "frauenhaus_id": site_id,
                "frauenhaus_name": name or None,
                "frauenhaus_status": status,
                "ort": ort,
                "bundesland": bundesland,
                "traegerin": join_value(
                    fields.get("Trägerin"), " "
                ),  # "Name" + "(Verband)" on one line
                "art_der_einrichtung": join_value(fields.get("Art der Einrichtung"), "; "),
                "aufnahmemoeglichkeit": join_value(fields.get("Aufnahmemöglichkeit"), "; "),
                # Helper value, only used by validate(); never written to the DataFrame.
                "_css_status": css_status.get(site_id) if site_id is not None else None,
            }
        )

    return records, announced


# --------------------------------------------------------------------------- #
# 6. Validation and DataFrame
# --------------------------------------------------------------------------- #
def validate(records: list[dict], announced: int | None) -> tuple[list[str], list[str]]:
    """Check the parsed data. Returns (fatal_problems, warnings).
    """
    fatal: list[str] = []
    warnings: list[str] = []
    n = len(records)

    if n == 0:
        return ["No records found - page structure probably changed."], warnings

    # Compare with the number the page itself announces.
    if announced is None:
        warnings.append("Could not read the announced hit count ('Ihre Suche ergab N Treffer').")
    elif announced != n:
        fatal.append(f"Parsed {n} records but the page announces {announced}.")

    # A house without a name is useless.
    if any(not r["frauenhaus_name"] for r in records):
        fatal.append("At least one record has no name.")

    # is the id unique?
    ids = [r["frauenhaus_id"] for r in records]
    if any(i is None for i in ids):
        fatal.append("At least one record has no site ID.")
    dupes = sorted({i for i in ids if i is not None and ids.count(i) > 1})
    if dupes:
        fatal.append(f"Duplicate IDs: {dupes[:10]}")

    # Cross-check: status from the icon must equal status from the CSS class.
    mismatch = [
        r
        for r in records
        if r["_css_status"]
        and r["frauenhaus_status"]
        and r["_css_status"] != r["frauenhaus_status"]
    ]
    if mismatch:
        fatal.append(
            f"Status icon and CSS class disagree for {len(mismatch)} records "
            f"(e.g. {mismatch[0]['frauenhaus_name']!r})."
        )

    # Important fields: a few gaps are a warning, many gaps are fatal.
    for col in ("frauenhaus_status", "ort", "bundesland", "art_der_einrichtung"):
        missing = [r for r in records if not r[col]]
        if not missing:
            continue
        msg = f"{len(missing)}/{n} records without '{col}' (e.g. {missing[0]['frauenhaus_name']!r})"
        (fatal if len(missing) / n > MAX_MISSING_SHARE else warnings).append(msg)

    # Bundesland values we don't know (possible parse error or new value).
    unknown_bl = sorted(
        {
            r["bundesland"]
            for r in records
            if r["bundesland"] and r["bundesland"] not in BUNDESLAENDER
        }
    )
    if unknown_bl:
        warnings.append(f"Unexpected Bundesland values: {unknown_bl}")

    return fatal, warnings


def build_dataframe(records: list[dict], scraped_at: datetime) -> pd.DataFrame:
    """Turn the records into a pandas DataFrame with the fixed column types.
    """
    scrape_date = scraped_at.strftime("%Y-%m-%d")  # e.g. "2026-10-12"
    scrape_time = scraped_at.strftime("%H:%M:%S")  # e.g. "10:40:09"
    rows = [
        {
            "scrape_date": scrape_date,
            "scrape_time": scrape_time,
            # keep only real columns (drops helper keys such as "_css_status")
            **{k: rec[k] for k in COLUMNS[2:]},
        }
        for rec in records
    ]
    # `columns=` fixes the column order; `astype` fixes the types (see DTYPES).
    return pd.DataFrame(rows, columns=COLUMNS).astype(DTYPES)


# --------------------------------------------------------------------------- #
# 7. Entry point
# --------------------------------------------------------------------------- #
def run() -> None:
    """Called by the wdr-ddj-cloud framework: download -> parse -> validate -> upload.

    exceptions instead of return codes: in the cloud, a raised exception
    marks the run as failed (and shows up in the project's error reporting).
    Nothing is uploaded in that case, so bad data never reaches the bucket.
    """
    # Take the timestamp right BEFORE the request, so it reflects the moment of scraping.
    scraped_at = now_berlin()

    # Step 1 + 2: download and parse.
    records, announced = parse_page(fetch_html())

    # Step 3: validate. On any fatal problem: stop, upload nothing.
    fatal, warnings = validate(records, announced)
    for warning in warnings:
        log.warning(warning)
    if fatal:
        message = "Validation failed - nothing was uploaded: " + " | ".join(fatal)
        raise RuntimeError(message)

    # Step 4: build the DataFrame and upload it as the CSV of today.
    df = build_dataframe(records, scraped_at)
    key = f"{OUTPUT_FOLDER}/frauenhaeuser_{scraped_at:%Y-%m-%d}.csv"
    upload_dataframe(df, key)
    log.info("Uploaded %d rows to %s", len(df), key)
