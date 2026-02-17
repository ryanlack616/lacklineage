#!/usr/bin/env python3
"""
scan_documents.py — OCR family documents and match them to people in lineage.db.

Two-phase scan:
  Phase 1: Parse filenames for embedded names/doc types (fast, no OCR).
  Phase 2: Tesseract OCR on remaining unmatched or all images (slower).

Handles Ancestry.com export naming convention:
    0001_Person Name_hash.jpg
    0002_Newspapers.com - Paper - Date - Description_hash.jpg

Skips _dupes/ and other junk directories automatically.

Usage:
    python scan_documents.py                         # full two-phase scan (default: raw-data/)
    python scan_documents.py <folder>                # scan specific folder
    python scan_documents.py --rescan                # re-scan everything
    python scan_documents.py --filename-only         # phase 1 only (no OCR)
    python scan_documents.py --vision                # phase 3: MiniCPM-o 4.5 vision AI pass
    python scan_documents.py --vision --rescan       # re-analyze all with vision AI
    python scan_documents.py --thumbnails-only       # just regenerate thumbnails
    python scan_documents.py --export-only           # just re-export documents.json
    python scan_documents.py --review                # review unverified matches
    python scan_documents.py --stats                 # show document stats
    python scan_documents.py --boost                 # recalc confidence with doc bonus

Requires:
    pip install Pillow pytesseract
    Tesseract OCR installed at "C:\\Program Files\\Tesseract-OCR\\tesseract.exe"
    (Optional for PDF: pip install pdf2image + poppler)
"""

import hashlib, json, os, re, shutil, sqlite3, sys, time
from datetime import datetime
from pathlib import Path
from difflib import SequenceMatcher

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

TESSERACT_CMD = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
SUPPORTED_EXTS = {".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp", ".gif", ".webp", ".pdf"}
SKIP_DIRS = {"_dupes", "__pycache__", ".git", "node_modules"}
SCRIPT_DIR = Path(__file__).parent
DB_PATH = str(SCRIPT_DIR / "lineage.db")
RAW_DIR = SCRIPT_DIR / "raw-data"
THUMB_DIR = SCRIPT_DIR / "data" / "thumbs"
THUMB_WIDTH = 400                 # px, longest edge
THUMB_QUALITY = 82                # JPEG quality

# Vision AI (Ollama + MiniCPM-o 4.5)
OLLAMA_URL = "http://127.0.0.1:11434"
VISION_MODEL = "openbmb/minicpm-o4.5"
VISION_MAX_PX = 1800              # resize images before sending to model
VISION_PROMPT = """Analyze this genealogy document image. Extract ALL of the following information you can find:

1. PEOPLE: List every person name mentioned (first, middle, last). Format each as "PERSON: Firstname Lastname"
2. DATES: List every date found. Format as "DATE: YYYY-MM-DD description" (e.g. "DATE: 1923-05-14 birth")
3. PLACES: List locations mentioned. Format as "PLACE: City, State/Country"
4. RELATIONSHIPS: Any family relationships stated. Format as "REL: Person1 is [relationship] of Person2"
5. DOCTYPE: What type of document is this? One of: certificate, obituary, census, military, newspaper, letter, photo, record, other
6. SUMMARY: One sentence describing what this document is about.

Be thorough. Extract every name and date visible, even if partially legible. If text is unclear, prefix with "?" to indicate uncertainty."""

# Matching thresholds
NAME_MATCH_THRESHOLD = 0.78       # minimum similarity for OCR name matches
FILENAME_MATCH_THRESHOLD = 0.82   # higher bar for filename-only matches
MIN_TEXT_LENGTH = 10              # minimum OCR chars to attempt matching
MAX_OCR_MATCHES_PER_DOC = 5      # cap weak matches per document


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def file_hash(filepath):
    """SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def guess_doc_type(filename):
    """Guess document type from filename keywords."""
    lower = filename.lower()
    if any(w in lower for w in ("death certificate", "death cert")):
        return "certificate"
    if any(w in lower for w in ("birth certificate", "birth cert", "birth announcement")):
        return "certificate"
    if any(w in lower for w in ("marriage", "wedding", "marr")):
        return "certificate"
    if any(w in lower for w in ("obituary", "obit")):
        return "obituary"
    if any(w in lower for w in ("census",)):
        return "census"
    if any(w in lower for w in ("military", "draft", "service", "enlistment")):
        return "military"
    if any(w in lower for w in ("newspaper", "newspapers.com")):
        return "newspaper"
    if any(w in lower for w in ("letter", "correspondence")):
        return "letter"
    if any(w in lower for w in ("portrait", "enhanced", "photo")):
        return "photo"
    if any(w in lower for w in ("certificate", "record")):
        return "certificate"
    if lower.endswith((".pdf", ".doc")):
        return "document"
    ext = Path(filename).suffix.lower()
    if ext in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tif", ".tiff"}:
        return "photo"
    return "other"


# ---------------------------------------------------------------------------
# FILENAME PARSER — extract names, dates, doc type from Ancestry filenames
# ---------------------------------------------------------------------------

def parse_ancestry_filename(filename):
    """Parse Ancestry.com export filenames.

    Format: 0001_Description Text_hexhash.ext

    Returns dict with:
        description: cleaned description text
        names_found: list of potential person names
        years_found: set of years
        doc_hint: guessed document type from filename
    """
    stem = Path(filename).stem

    # Strip leading sequence number: "0001_..."
    m = re.match(r"^\d{4}_(.+)$", stem)
    if m:
        stem = m.group(1)

    # Strip trailing hex hash: "..._a8b7c6d5"
    m = re.match(r"^(.+?)_[0-9a-f]{8}$", stem, re.IGNORECASE)
    if m:
        stem = m.group(1)

    description = stem.strip()

    names = []
    years = set()

    # --- Newspapers.com format ---
    news_m = re.match(
        r"Newspapers\.com\s*-\s*(.+?)\s*-\s*(\d{1,2}\s+\w+\s+\d{4})\s*-\s*\d+\s+(.+)",
        description, re.IGNORECASE
    )
    if news_m:
        _paper, _date, content = news_m.groups()
        yr_m = re.search(r"\b(\d{4})\b", _date)
        if yr_m:
            years.add(int(yr_m.group(1)))
        content = content.strip()

        # "Obituary for MARY A. HARRISON"
        obit_m = re.match(r"Obituary\s+for\s+(.+?)(?:\s*\(Aged\s+\d+\))?$", content, re.IGNORECASE)
        if obit_m:
            names.append(clean_name(obit_m.group(1)))
        # "Marriage of Caudle _ Lack"
        marr_m = re.match(r"Marriage\s+of\s+(.+?)\s*[_&]\s*(.+)", content, re.IGNORECASE)
        if marr_m:
            names.append(clean_name(marr_m.group(1)))
            names.append(clean_name(marr_m.group(2)))
        # "Birth announcement Peter Michael Lack"
        birth_m = re.match(r"Birth\s+announcement\s+(.+)", content, re.IGNORECASE)
        if birth_m:
            names.append(clean_name(birth_m.group(1)))
        # Fallback: treat whole content as a name
        if not names:
            names.append(clean_name(content))
    else:
        # --- Non-newspaper filename ---
        desc_clean = re.sub(r"'s?\s+(Portrait|Photo|Picture|Image)\b.*", "", description, flags=re.IGNORECASE)
        desc_clean = re.sub(r"\s+(Enhanced|Colorized|Restored)\b.*", "", desc_clean, flags=re.IGNORECASE)
        desc_clean = re.sub(r"\bDeath Certificate\b", "", desc_clean, flags=re.IGNORECASE)
        desc_clean = re.sub(r"\bBirth\s*\d{4}\b", "", desc_clean, flags=re.IGNORECASE)
        desc_clean = re.sub(r"\s+of\s+\w+\b.*", "", desc_clean, flags=re.IGNORECASE)
        desc_clean = re.sub(r"\s+(top|bottom|left|right|front|back|row)\b.*", "", desc_clean, flags=re.IGNORECASE)

        desc_clean = desc_clean.strip(" _-,")
        if desc_clean and not re.match(r"^(IMG|image|Photo|photo|DSC|DSCN|pic)\b", desc_clean, re.IGNORECASE):
            if len(desc_clean.split()) >= 2 or len(desc_clean) > 3:
                names.append(clean_name(desc_clean))

    # Years from anywhere in desc
    for yr_m in re.finditer(r"\b(1[7-9]\d{2}|20[0-3]\d)\b", description):
        years.add(int(yr_m.group(1)))

    names = [n for n in names if n and len(n) > 2]

    return {
        "description": description,
        "names_found": names,
        "years_found": years,
        "doc_hint": guess_doc_type(filename),
    }


def clean_name(raw):
    """Clean up an extracted name string."""
    name = raw.strip(" _-.,;:'\"")
    name = re.sub(r"\s*\([^)]*\)", "", name)
    name = re.sub(r"\s+\d+$", "", name)
    name = " ".join(name.split())
    if name.isupper():
        name = name.title()
    return name


# ---------------------------------------------------------------------------
# OCR
# ---------------------------------------------------------------------------

def ocr_image(filepath):
    """OCR an image file using Tesseract. Returns extracted text."""
    try:
        import pytesseract
        from PIL import Image
    except ImportError:
        print("ERROR: Install required packages: pip install Pillow pytesseract")
        sys.exit(1)

    pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

    ext = Path(filepath).suffix.lower()
    if ext == ".pdf":
        return ocr_pdf(filepath)
    if ext in (".doc", ".docx"):
        return ""

    img = Image.open(filepath)
    if img.mode not in ("L", "RGB"):
        img = img.convert("RGB")
    text = pytesseract.image_to_string(img, lang="eng")
    return text.strip()


def ocr_pdf(filepath):
    """OCR a PDF file (converts pages to images first)."""
    try:
        import pytesseract
        from pdf2image import convert_from_path
    except ImportError:
        print("  (skipping PDF — install pdf2image + poppler)")
        return ""
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD
    try:
        pages = convert_from_path(filepath, dpi=300)
    except Exception as e:
        print(f"  (PDF convert failed: {e})")
        return ""
    texts = []
    for page in pages:
        texts.append(pytesseract.image_to_string(page, lang="eng"))
    return "\n".join(texts).strip()


# ---------------------------------------------------------------------------
# VISION AI — MiniCPM-o 4.5 via Ollama for deep document understanding
# ---------------------------------------------------------------------------

def vision_analyze(filepath):
    """Send an image to MiniCPM-o 4.5 via Ollama API. Returns analysis text."""
    import base64
    try:
        import urllib.request
        from PIL import Image
    except ImportError:
        return ""

    ext = Path(filepath).suffix.lower()
    if ext in (".pdf", ".doc", ".docx"):
        return ""

    # Resize large images to keep inference fast
    try:
        img = Image.open(filepath)
        if img.mode in ("RGBA", "P", "LA"):
            img = img.convert("RGB")
        elif img.mode not in ("L", "RGB"):
            img = img.convert("RGB")
        w, h = img.size
        if max(w, h) > VISION_MAX_PX:
            ratio = VISION_MAX_PX / max(w, h)
            img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)
        import io
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        img_bytes = buf.getvalue()
    except Exception:
        with open(filepath, "rb") as f:
            img_bytes = f.read()

    encoded = base64.b64encode(img_bytes).decode("utf-8")

    payload = json.dumps({
        "model": VISION_MODEL,
        "prompt": VISION_PROMPT,
        "stream": False,
        "images": [encoded],
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{OLLAMA_URL}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return result.get("response", "").strip()
    except Exception as e:
        raise RuntimeError(f"Ollama API error: {e}")


def parse_vision_names(vision_text):
    """Extract person names from vision analysis output."""
    names = []
    for line in vision_text.split("\n"):
        line = line.strip()
        # "PERSON: Firstname Lastname"
        m = re.match(r"(?:\d+\.\s*)?PERSON:\s*\??\s*(.+)", line, re.IGNORECASE)
        if m:
            name = clean_name(m.group(1))
            if name and len(name) > 2:
                names.append(name)
    # Also try to catch names from the summary or relationship lines
    for line in vision_text.split("\n"):
        line = line.strip()
        m = re.match(r"REL:\s*(.+?)\s+is\s+\w+\s+of\s+(.+)", line, re.IGNORECASE)
        if m:
            for raw in [m.group(1), m.group(2)]:
                name = clean_name(raw)
                if name and len(name) > 2:
                    names.append(name)
    return list(set(names))


def parse_vision_years(vision_text):
    """Extract years from vision analysis date lines."""
    years = set()
    for m in re.finditer(r"\b(1[7-9]\d{2}|20[0-3]\d)\b", vision_text):
        years.add(int(m.group(1)))
    return years


def parse_vision_doctype(vision_text):
    """Extract document type from vision analysis."""
    for line in vision_text.split("\n"):
        m = re.match(r"(?:\d+\.\s*)?DOCTYPE:\s*(.+)", line, re.IGNORECASE)
        if m:
            dtype = m.group(1).strip().lower()
            valid = {"certificate", "obituary", "census", "military",
                     "newspaper", "letter", "photo", "record", "other"}
            if dtype in valid:
                return dtype
            # partial match
            for v in valid:
                if v in dtype:
                    return v
    return None


# ---------------------------------------------------------------------------
# PHASE 3: VISION AI SCAN — deep document understanding via MiniCPM-o 4.5
# ---------------------------------------------------------------------------

def scan_vision(folder, conn, people, rescan=False):
    """Run MiniCPM-o 4.5 vision analysis on documents for deep extraction."""
    import urllib.request

    # Check Ollama is running and model is available
    try:
        req = urllib.request.Request(f"{OLLAMA_URL}/api/tags")
        with urllib.request.urlopen(req, timeout=5) as resp:
            tags = json.loads(resp.read().decode("utf-8"))
            models = [m["name"] for m in tags.get("models", [])]
            if not any(VISION_MODEL in m for m in models):
                print(f"\n  ERROR: Model '{VISION_MODEL}' not found in Ollama.")
                print(f"  Run: ollama pull {VISION_MODEL}")
                return
    except Exception as e:
        print(f"\n  ERROR: Cannot reach Ollama at {OLLAMA_URL}: {e}")
        print("  Make sure Ollama is running (ollama serve)")
        return

    if rescan:
        docs = conn.execute(
            "SELECT id, filepath, filename FROM document"
        ).fetchall()
    else:
        docs = conn.execute(
            "SELECT id, filepath, filename FROM document "
            "WHERE vision_text IS NULL OR vision_text = ''"
        ).fetchall()

    if not docs:
        print("\n--- Phase 3 (Vision AI): No documents need analysis ---")
        return

    print(f"\n--- Phase 3: Vision AI analysis ({len(docs)} documents) ---")
    print(f"  Model: {VISION_MODEL}")
    t0 = time.time()
    analyzed = 0
    new_matches = 0
    errors = 0
    skipped = 0
    upgraded_types = 0

    for i, (doc_id, filepath, filename) in enumerate(docs, 1):
        if not os.path.exists(filepath):
            skipped += 1
            progress_bar(i, len(docs), t0, extra=f"AI:{analyzed} match:{new_matches} err:{errors}")
            continue
        ext = Path(filepath).suffix.lower()
        if ext in (".doc", ".docx", ".pdf"):
            skipped += 1
            progress_bar(i, len(docs), t0, extra=f"AI:{analyzed} match:{new_matches} err:{errors}")
            continue

        progress_bar(i, len(docs), t0, extra=f"AI:{analyzed} match:{new_matches} err:{errors}")

        try:
            vision_text = vision_analyze(filepath)
        except Exception as e:
            errors += 1
            continue

        if not vision_text:
            continue

        conn.execute(
            "UPDATE document SET vision_text = ?, vision_date = ? WHERE id = ?",
            (vision_text, datetime.now().isoformat(), doc_id)
        )
        analyzed += 1

        # Upgrade doc_type if vision gives better info
        v_dtype = parse_vision_doctype(vision_text)
        if v_dtype:
            current_type = conn.execute(
                "SELECT doc_type FROM document WHERE id = ?", (doc_id,)
            ).fetchone()[0]
            if current_type in (None, "photo", "other") and v_dtype not in ("photo", "other"):
                conn.execute(
                    "UPDATE document SET doc_type = ? WHERE id = ?",
                    (v_dtype, doc_id)
                )
                upgraded_types += 1

        # Extract names and match
        v_names = parse_vision_names(vision_text)
        v_years = parse_vision_years(vision_text)

        if v_names:
            matches = match_names_to_people(v_names, people, v_years)
            for pid, conf, snippet, _ in matches:
                # Boost vision confidence slightly — it's more contextual than OCR regex
                conf = min(conf + 0.05, 1.0)
                existing = conn.execute(
                    "SELECT confidence, match_type FROM document_match "
                    "WHERE document_id = ? AND person_id = ?",
                    (doc_id, pid)
                ).fetchone()
                if existing:
                    if conf > existing[0]:
                        conn.execute(
                            "UPDATE document_match SET confidence = ?, "
                            "snippet = ?, match_type = 'vision_auto' "
                            "WHERE document_id = ? AND person_id = ?",
                            (round(conf, 3), snippet, doc_id, pid)
                        )
                else:
                    conn.execute(
                        "INSERT OR REPLACE INTO document_match "
                        "(document_id, person_id, match_type, confidence, snippet, verified) "
                        "VALUES (?,?,?,?,?,0)",
                        (doc_id, pid, "vision_auto", round(conf, 3), snippet)
                    )
                    new_matches += 1

        if i % 10 == 0:
            conn.commit()

    conn.commit()
    # Clear progress bar line
    sys.stdout.write("\r" + " " * shutil.get_terminal_size((80, 20)).columns + "\r")
    sys.stdout.flush()
    elapsed = time.time() - t0
    print(f"  Phase 3 done: {analyzed} analyzed, {new_matches} new matches, "
          f"{upgraded_types} doc types upgraded, {errors} errors, "
          f"{skipped} skipped ({elapsed:.0f}s)")

    # Show sample analyses
    samples = conn.execute(
        "SELECT d.filename, d.vision_text FROM document d "
        "WHERE d.vision_text IS NOT NULL AND d.vision_text != '' "
        "ORDER BY RANDOM() LIMIT 5"
    ).fetchall()
    if samples:
        print(f"\n{'='*60}")
        print(f"VISION AI SAMPLE ANALYSES ({len(samples)} random)")
        print(f"{'='*60}")
        for fname, vtext in samples:
            print(f"\n  {fname}")
            for line in vtext.split("\n")[:8]:
                line = line.strip()
                if line:
                    print(f"    {line}")
            if len(vtext.split("\n")) > 8:
                print(f"    ...")


# ---------------------------------------------------------------------------
# TEXT EXTRACTION — pull names and dates from OCR output
# ---------------------------------------------------------------------------

def extract_years(text):
    """Extract 4-digit years from text."""
    years = set()
    for m in re.finditer(r"\b(1[7-9]\d{2}|20[0-3]\d)\b", text):
        years.add(int(m.group(1)))
    return years


def extract_potential_names(text):
    """Extract sequences that look like personal names from OCR text."""
    noise_words = {
        "COUNTY", "TOWNSHIP", "STATE", "CERTIFICATE", "DEPARTMENT",
        "REGISTRAR", "BUREAU", "VITAL", "STATISTICS", "RECORD",
        "HEREBY", "CERTIFY", "ISSUED", "FILED", "PAGE", "VOLUME",
        "DISTRICT", "PRECINCT", "WARD", "RESIDENCE", "OCCUPATION",
        "WITNESS", "CHURCH", "CEMETERY", "FUNERAL", "HOSPITAL",
        "BORN", "DIED", "MARRIED", "BAPTIZED", "BURIED",
        "FATHER", "MOTHER", "HUSBAND", "WIFE", "CHILD", "SON", "DAUGHTER",
        "NAME", "DATE", "PLACE", "BIRTH", "DEATH", "MARRIAGE",
        "NEWSPAPERS", "NEWS", "PRESS", "TIMES", "STANDARD", "COLUMBIA",
    }
    names = []

    # Title case: "Firstname [Middle] Lastname"
    for m in re.finditer(
        r"\b([A-Z][a-z]{1,20}(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]{1,20}(?:\s+[A-Z][a-z]{1,20})?)\b",
        text
    ):
        candidate = m.group(1).strip()
        words = candidate.split()
        if any(w.upper() in noise_words for w in words):
            continue
        if len([w for w in words if len(w) > 1]) >= 2:
            names.append(candidate)

    # ALL-CAPS names
    for m in re.finditer(
        r"\b([A-Z]{2,20}\s+[A-Z]\.?\s+[A-Z]{2,20}|[A-Z]{2,20}\s+[A-Z]{2,20})\b",
        text
    ):
        candidate = m.group(1).strip()
        words = candidate.split()
        if any(w.upper() in noise_words for w in words):
            continue
        if len(words) >= 2:
            names.append(candidate.title())

    return list(set(names))


# ---------------------------------------------------------------------------
# MATCHING
# ---------------------------------------------------------------------------

def name_similarity(name1, name2):
    """Similarity between two names (0.0-1.0)."""
    n1 = name1.lower().strip()
    n2 = name2.lower().strip()
    if n1 == n2:
        return 1.0
    return SequenceMatcher(None, n1, n2).ratio()


def extract_year_from_str(date_str):
    """Extract a 4-digit year from a date string."""
    if not date_str:
        return None
    m = re.search(r"(\d{4})", str(date_str))
    return int(m.group(1)) if m else None


def match_names_to_people(names_found, people, years_in_doc, threshold=NAME_MATCH_THRESHOLD):
    """Match extracted names against database people.

    Returns list of (person_id, confidence, snippet, match_type).
    """
    matches = []
    seen_pids = set()

    for person in people:
        pid = person["id"]
        full_name = f"{person['given_name'] or ''} {person['surname'] or ''}".strip()
        if not full_name or len(full_name) < 3:
            continue

        given = person["given_name"] or ""
        surname = person["surname"] or ""

        best_score = 0.0
        best_snippet = ""

        for extracted_name in names_found:
            # Full name match
            sim = name_similarity(full_name, extracted_name)
            if sim > best_score:
                best_score = sim
                best_snippet = extracted_name

            # Given + surname component match
            parts = extracted_name.split()
            if given and surname and len(parts) >= 2:
                given_sim = name_similarity(given, parts[0])
                sur_sim = name_similarity(surname, parts[-1])
                combined = given_sim * 0.4 + sur_sim * 0.6
                if combined > best_score and given_sim > 0.7 and sur_sim > 0.7:
                    best_score = combined
                    best_snippet = extracted_name

            # Surname-only match: lower confidence
            if surname and len(surname) > 2 and best_score < 0.5:
                for word in parts:
                    sur_sim = name_similarity(surname, word)
                    if sur_sim > 0.9:
                        best_score = max(best_score, sur_sim * 0.45)
                        best_snippet = extracted_name

        # Year boost
        year_boost = 0
        if years_in_doc:
            birth_year = extract_year_from_str(person.get("birth_date"))
            death_year = extract_year_from_str(person.get("death_date"))
            if birth_year and birth_year in years_in_doc:
                year_boost += 0.12
            if death_year and death_year in years_in_doc:
                year_boost += 0.08

        final_score = min(best_score + year_boost, 1.0)

        if final_score >= threshold and pid not in seen_pids:
            matches.append((pid, final_score, best_snippet, "auto"))
            seen_pids.add(pid)

    matches.sort(key=lambda x: x[1], reverse=True)
    return matches[:MAX_OCR_MATCHES_PER_DOC]


# ---------------------------------------------------------------------------
# DB HELPERS
# ---------------------------------------------------------------------------

def ensure_tables(conn):
    """Create document tables if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS document (
            id          INTEGER PRIMARY KEY,
            filename    TEXT,
            filepath    TEXT UNIQUE,
            doc_type    TEXT,
            ocr_text    TEXT,
            ocr_date    TEXT,
            file_hash   TEXT,
            description TEXT,
            has_thumb   INTEGER DEFAULT 0,
            seq_num     INTEGER
        );
        CREATE TABLE IF NOT EXISTS document_match (
            id          INTEGER PRIMARY KEY,
            document_id INTEGER REFERENCES document(id),
            person_id   INTEGER REFERENCES person(id),
            match_type  TEXT,
            confidence  REAL,
            snippet     TEXT,
            verified    INTEGER DEFAULT 0,
            UNIQUE(document_id, person_id)
        );
        CREATE INDEX IF NOT EXISTS idx_doc_match_person ON document_match(person_id);
        CREATE INDEX IF NOT EXISTS idx_doc_match_doc    ON document_match(document_id);
    """)
    # Add columns if missing (upgrade path)
    for col, typedef in [("description", "TEXT"), ("has_thumb", "INTEGER DEFAULT 0"),
                         ("seq_num", "INTEGER"), ("vision_text", "TEXT"), ("vision_date", "TEXT")]:
        try:
            conn.execute(f"SELECT {col} FROM document LIMIT 0")
        except sqlite3.OperationalError:
            conn.execute(f"ALTER TABLE document ADD COLUMN {col} {typedef}")


def load_people(conn):
    """Load all people from the database for matching."""
    rows = conn.execute(
        "SELECT id, given_name, surname, birth_date, death_date FROM person"
    ).fetchall()
    return [
        {"id": r[0], "given_name": r[1], "surname": r[2],
         "birth_date": r[3], "death_date": r[4]}
        for r in rows
    ]


# ---------------------------------------------------------------------------
# THUMBNAILS
# ---------------------------------------------------------------------------

def make_thumbnail(src_path, thumb_path):
    """Create a web-optimized JPEG thumbnail (longest edge = THUMB_WIDTH)."""
    try:
        from PIL import Image
    except ImportError:
        print("ERROR: pip install Pillow")
        return False
    try:
        img = Image.open(src_path)
        if img.mode in ("RGBA", "P", "LA"):
            img = img.convert("RGB")
        elif img.mode not in ("L", "RGB"):
            img = img.convert("RGB")
        img.thumbnail((THUMB_WIDTH, THUMB_WIDTH), Image.LANCZOS)
        os.makedirs(os.path.dirname(thumb_path), exist_ok=True)
        img.save(thumb_path, "JPEG", quality=THUMB_QUALITY, optimize=True)
        return True
    except Exception as e:
        print(f"  Thumb error ({Path(src_path).name}): {e}")
        return False


# ---------------------------------------------------------------------------
# FILE COLLECTOR
# ---------------------------------------------------------------------------

def collect_files(folder):
    """Collect scannable files, skipping _dupes and other junk dirs."""
    files = []
    for root, dirs, filenames in os.walk(folder):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for fn in filenames:
            ext = Path(fn).suffix.lower()
            if ext in SUPPORTED_EXTS:
                files.append(Path(root) / fn)
    return sorted(files)


# ---------------------------------------------------------------------------
# PHASE 1: FILENAME MATCHING (fast, no OCR)
# ---------------------------------------------------------------------------

def scan_filenames(folder, conn, people, rescan=False):
    """Match files to people based on filename analysis only. Also generates thumbnails."""
    folder = Path(folder)
    files = collect_files(folder)
    print(f"\n--- Phase 1: Filename matching + thumbnails ({len(files)} files) ---")
    os.makedirs(THUMB_DIR, exist_ok=True)

    existing_hashes = set()
    if not rescan:
        for row in conn.execute("SELECT file_hash FROM document WHERE file_hash IS NOT NULL"):
            existing_hashes.add(row[0])

    matched = 0
    new_docs = 0
    skipped = 0
    thumbs = 0

    for i, filepath in enumerate(files, 1):
        fn = filepath.name
        rel_path = str(filepath)
        ext = filepath.suffix.lower()

        if not rescan:
            existing = conn.execute(
                "SELECT id FROM document WHERE filepath = ?", (rel_path,)
            ).fetchone()
            if existing:
                skipped += 1
                continue

        parsed = parse_ancestry_filename(fn)
        fhash = file_hash(filepath)

        if not rescan and fhash in existing_hashes:
            skipped += 1
            continue

        # --- Thumbnail ---
        has_thumb = 0
        if ext in SUPPORTED_EXTS - {".pdf", ".doc", ".docx"}:
            thumb_name = filepath.stem + ".jpg"
            thumb_path = THUMB_DIR / thumb_name
            if not thumb_path.exists() or rescan:
                if make_thumbnail(str(filepath), str(thumb_path)):
                    has_thumb = 1
                    thumbs += 1
            else:
                has_thumb = 1

        # --- Extract seq_num from filename ---
        seq_num = None
        seq_m = re.match(r"^(\d{4})_", fn)
        if seq_m:
            seq_num = int(seq_m.group(1))

        conn.execute(
            "INSERT OR REPLACE INTO document "
            "(filename, filepath, doc_type, ocr_text, ocr_date, file_hash, description, has_thumb, seq_num) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (fn, rel_path, parsed["doc_hint"], "",
             datetime.now().isoformat(), fhash, parsed["description"],
             has_thumb, seq_num)
        )
        new_docs += 1
        existing_hashes.add(fhash)

        doc_id = conn.execute(
            "SELECT id FROM document WHERE filepath = ?", (rel_path,)
        ).fetchone()[0]

        if parsed["names_found"]:
            matches = match_names_to_people(
                parsed["names_found"], people, parsed["years_found"],
                threshold=FILENAME_MATCH_THRESHOLD
            )
            for pid, conf, snippet, _ in matches:
                conn.execute(
                    "INSERT OR REPLACE INTO document_match "
                    "(document_id, person_id, match_type, confidence, snippet, verified) "
                    "VALUES (?,?,?,?,?,0)",
                    (doc_id, pid, "filename", round(conf, 3), snippet)
                )
                matched += 1

        if i % 200 == 0:
            conn.commit()
            print(f"  [{i}/{len(files)}] {new_docs} new, {matched} filename matches...")

    conn.commit()
    print(f"  Phase 1 done: {new_docs} new docs, {skipped} skipped, {matched} filename matches, {thumbs} thumbnails")
    return new_docs


# ---------------------------------------------------------------------------
# PHASE 2: OCR SCAN (slow)
# ---------------------------------------------------------------------------

def progress_bar(current, total, t0, width=40, extra=""):
    """Render a terminal progress bar with ETA."""
    pct = current / total if total else 1
    filled = int(width * pct)
    bar = "█" * filled + "░" * (width - filled)
    elapsed = time.time() - t0
    rate = current / elapsed if elapsed > 0 else 0
    remaining = (total - current) / rate if rate > 0 else 0
    if remaining >= 3600:
        eta_str = f"{remaining/3600:.1f}h"
    elif remaining >= 60:
        eta_str = f"{remaining/60:.0f}m{int(remaining%60):02d}s"
    else:
        eta_str = f"{remaining:.0f}s"
    cols = shutil.get_terminal_size((80, 20)).columns
    line = f"\r  |{bar}| {pct:5.1%}  [{current}/{total}]  {rate:.1f}/s  ETA {eta_str}"
    if extra:
        line += f"  {extra}"
    line = line[:cols]
    sys.stdout.write(line)
    sys.stdout.flush()


def scan_ocr(folder, conn, people, rescan=False, filename_only=False):
    """OCR documents and find additional matches."""
    if filename_only:
        print("\n--- Skipping Phase 2 (--filename-only) ---")
        return

    if rescan:
        docs = conn.execute(
            "SELECT id, filepath, filename FROM document"
        ).fetchall()
    else:
        docs = conn.execute(
            "SELECT id, filepath, filename FROM document WHERE ocr_text = '' OR ocr_text IS NULL"
        ).fetchall()

    if not docs:
        print("\n--- Phase 2: No documents need OCR ---")
        return

    print(f"\n--- Phase 2: OCR scanning ({len(docs)} documents) ---")
    t0 = time.time()
    ocr_count = 0
    new_matches = 0
    errors = 0
    skipped = 0
    # keep samples for accuracy check
    ocr_samples = []

    for i, (doc_id, filepath, filename) in enumerate(docs, 1):
        if not os.path.exists(filepath):
            skipped += 1
            progress_bar(i, len(docs), t0, extra=f"OCR:{ocr_count} match:{new_matches} err:{errors}")
            continue
        ext = Path(filepath).suffix.lower()
        if ext in (".doc", ".docx"):
            skipped += 1
            progress_bar(i, len(docs), t0, extra=f"OCR:{ocr_count} match:{new_matches} err:{errors}")
            continue

        progress_bar(i, len(docs), t0, extra=f"OCR:{ocr_count} match:{new_matches} err:{errors}")

        try:
            text = ocr_image(filepath)
        except Exception as e:
            errors += 1
            continue

        if text:
            conn.execute(
                "UPDATE document SET ocr_text = ?, ocr_date = ? WHERE id = ?",
                (text, datetime.now().isoformat(), doc_id)
            )
            ocr_count += 1

            # Collect samples for accuracy check (every ~100th doc, up to 20)
            if len(ocr_samples) < 20 and (ocr_count % max(1, len(docs)//20) == 0 or ocr_count <= 3):
                ocr_samples.append((filename, text[:300]))

            if len(text) >= MIN_TEXT_LENGTH:
                years = extract_years(text)
                ocr_names = extract_potential_names(text)

                if ocr_names:
                    matches = match_names_to_people(ocr_names, people, years)
                    for pid, conf, snippet, _ in matches:
                        existing = conn.execute(
                            "SELECT confidence, match_type FROM document_match "
                            "WHERE document_id = ? AND person_id = ?",
                            (doc_id, pid)
                        ).fetchone()
                        if existing:
                            if conf > existing[0]:
                                conn.execute(
                                    "UPDATE document_match SET confidence = ?, "
                                    "snippet = ?, match_type = 'ocr_auto' "
                                    "WHERE document_id = ? AND person_id = ?",
                                    (round(conf, 3), snippet, doc_id, pid)
                                )
                        else:
                            conn.execute(
                                "INSERT OR REPLACE INTO document_match "
                                "(document_id, person_id, match_type, confidence, snippet, verified) "
                                "VALUES (?,?,?,?,?,0)",
                                (doc_id, pid, "ocr_auto", round(conf, 3), snippet)
                            )
                            new_matches += 1

        if i % 50 == 0:
            conn.commit()

    conn.commit()
    # Clear progress bar line
    sys.stdout.write("\r" + " " * shutil.get_terminal_size((80, 20)).columns + "\r")
    sys.stdout.flush()
    elapsed = time.time() - t0
    print(f"  Phase 2 done: {ocr_count} OCR'd, {new_matches} new matches, "
          f"{errors} errors, {skipped} skipped ({elapsed:.0f}s)")

    # --- Accuracy spot-check ---
    if ocr_samples:
        print(f"\n{'='*60}")
        print(f"OCR ACCURACY SPOT-CHECK ({len(ocr_samples)} samples)")
        print(f"{'='*60}")
        for fname, text_preview in ocr_samples:
            clean = text_preview.replace("\n", " ").strip()
            if len(clean) > 120:
                clean = clean[:120] + "..."
            readable_chars = sum(1 for c in clean if c.isalnum() or c.isspace())
            ratio = readable_chars / max(len(clean), 1)
            quality = "GOOD" if ratio > 0.75 else ("OK" if ratio > 0.5 else "POOR")
            print(f"\n  [{quality}] {fname}")
            print(f"       {clean}")


# ---------------------------------------------------------------------------
# SCAN ENTRY POINT
# ---------------------------------------------------------------------------

def scan_folder(folder, rescan=False, filename_only=False, vision=False):
    """Multi-phase scan: filename matching, OCR, then optional vision AI."""
    folder = Path(folder)
    if not folder.exists():
        print(f"ERROR: Folder not found: {folder}")
        sys.exit(1)

    if not os.path.exists(DB_PATH):
        print(f"ERROR: Database not found: {DB_PATH}")
        print("Run import_gedcom.py first to create the database.")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)
    people = load_people(conn)
    print(f"Loaded {len(people)} people from database")

    if not vision:
        scan_filenames(folder, conn, people, rescan=rescan)
        scan_ocr(folder, conn, people, rescan=rescan, filename_only=filename_only)
    else:
        # Vision-only mode: skip Phase 1 & 2 if docs already exist
        existing = conn.execute("SELECT COUNT(*) FROM document").fetchone()[0]
        if existing == 0:
            scan_filenames(folder, conn, people, rescan=rescan)
        scan_vision(folder, conn, people, rescan=rescan)

    # Summary
    total_docs = conn.execute("SELECT COUNT(*) FROM document").fetchone()[0]
    total_matches = conn.execute("SELECT COUNT(*) FROM document_match").fetchone()[0]
    people_with_docs = conn.execute(
        "SELECT COUNT(DISTINCT person_id) FROM document_match"
    ).fetchone()[0]
    by_match_type = conn.execute(
        "SELECT match_type, COUNT(*) FROM document_match GROUP BY match_type"
    ).fetchall()

    print(f"\n{'='*60}")
    print(f"SCAN COMPLETE")
    print(f"{'='*60}")
    print(f"  Documents in DB:     {total_docs}")
    print(f"  Total matches:       {total_matches}")
    print(f"  People with docs:    {people_with_docs} / {len(people)}")
    print(f"  Match breakdown:")
    for mtype, count in by_match_type:
        print(f"    {mtype:15s}  {count}")

    top = conn.execute(
        "SELECT p.given_name, p.surname, COUNT(*) as doc_count, "
        "       MAX(dm.confidence) as best_conf "
        "FROM document_match dm JOIN person p ON dm.person_id = p.id "
        "GROUP BY dm.person_id ORDER BY doc_count DESC LIMIT 15"
    ).fetchall()
    if top:
        print(f"\n  Most documented people:")
        for given, sur, count, best in top:
            print(f"    {given or ''} {sur or '':20s}  {count:3d} doc(s)  best={best:.0%}")

    conn.close()

    # Export JSON for site viewer
    export_json()


# ---------------------------------------------------------------------------
# EXPORT — generate documents.json for the site viewer
# ---------------------------------------------------------------------------

def export_json():
    """Export documents.json with all docs, OCR text, matches, and thumb paths."""
    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)

    docs = conn.execute(
        "SELECT d.id, d.filename, d.filepath, d.doc_type, d.description, d.ocr_text, "
        "d.has_thumb, d.seq_num, d.vision_text "
        "FROM document d ORDER BY d.seq_num, d.id"
    ).fetchall()

    # Build match map
    match_rows = conn.execute(
        "SELECT dm.document_id, dm.person_id, dm.match_type, dm.confidence, "
        "dm.snippet, dm.verified, p.given_name, p.surname "
        "FROM document_match dm JOIN person p ON dm.person_id = p.id"
    ).fetchall()

    match_map = {}
    for doc_id, pid, mtype, conf, snippet, verified, given, surname in match_rows:
        if doc_id not in match_map:
            match_map[doc_id] = []
        match_map[doc_id].append({
            "person_id": pid,
            "name": f"{given or ''} {surname or ''}".strip(),
            "confidence": round(conf, 3),
            "match_type": mtype,
            "verified": bool(verified),
        })

    out = []
    for doc_id, filename, filepath, doc_type, description, ocr_text, has_thumb, seq_num, vision_text in docs:
        thumb_file = Path(filename).stem + ".jpg" if has_thumb else None
        entry = {
            "id": doc_id,
            "filename": filename,
            "title": description or filename,
            "type": doc_type,
            "ocr_text": (ocr_text or "")[:5000],   # cap for JSON size
            "thumb": f"data/thumbs/{thumb_file}" if thumb_file else None,
            "matches": match_map.get(doc_id, []),
            "seq": seq_num,
        }
        if vision_text:
            entry["vision"] = vision_text[:3000]
        out.append(entry)

    out_path = SCRIPT_DIR / "data" / "documents.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=None, separators=(",", ":"))

    total_matches = sum(len(d["matches"]) for d in out)
    with_ocr = sum(1 for d in out if len(d["ocr_text"]) > MIN_TEXT_LENGTH)
    with_thumb = sum(1 for d in out if d["thumb"])

    conn.close()
    print(f"\nExported {len(out)} documents to {out_path}")
    print(f"  With OCR text:   {with_ocr}")
    print(f"  With thumbnails: {with_thumb}")
    print(f"  Total matches:   {total_matches}")
    return out


# ---------------------------------------------------------------------------
# STATS
# ---------------------------------------------------------------------------

def show_stats():
    """Print document and match statistics."""
    if not os.path.exists(DB_PATH):
        print("No database found.")
        return

    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)

    total_docs = conn.execute("SELECT COUNT(*) FROM document").fetchone()[0]
    total_matches = conn.execute("SELECT COUNT(*) FROM document_match").fetchone()[0]
    verified = conn.execute(
        "SELECT COUNT(*) FROM document_match WHERE verified = 1"
    ).fetchone()[0]
    unverified = total_matches - verified

    by_type = conn.execute(
        "SELECT doc_type, COUNT(*) FROM document GROUP BY doc_type ORDER BY COUNT(*) DESC"
    ).fetchall()
    by_match_type = conn.execute(
        "SELECT match_type, COUNT(*) FROM document_match GROUP BY match_type ORDER BY COUNT(*) DESC"
    ).fetchall()
    people_with_docs = conn.execute(
        "SELECT COUNT(DISTINCT person_id) FROM document_match"
    ).fetchone()[0]
    avg_conf = conn.execute(
        "SELECT AVG(confidence) FROM document_match"
    ).fetchone()[0]
    unmatched = conn.execute(
        "SELECT COUNT(*) FROM document d WHERE NOT EXISTS "
        "(SELECT 1 FROM document_match dm WHERE dm.document_id = d.id)"
    ).fetchone()[0]

    print(f"\n{'='*60}")
    print(f"DOCUMENT STATS")
    print(f"{'='*60}")
    print(f"  Total documents:  {total_docs}  ({unmatched} unmatched)")
    print(f"  Total matches:    {total_matches}  ({verified} verified, {unverified} unverified)")
    print(f"  People with docs: {people_with_docs}")
    if avg_conf:
        print(f"  Avg match conf:   {avg_conf:.1%}")
    else:
        print(f"  No matches yet")
    print(f"\n  By document type:")
    for dtype, count in by_type:
        print(f"    {dtype or 'unknown':15s}  {count}")
    if by_match_type:
        print(f"\n  By match method:")
        for mtype, count in by_match_type:
            print(f"    {mtype or 'unknown':15s}  {count}")

    top = conn.execute(
        "SELECT p.given_name, p.surname, p.birth_date, COUNT(*) as doc_count, "
        "       SUM(CASE WHEN dm.verified = 1 THEN 1 ELSE 0 END) as verified_count "
        "FROM document_match dm JOIN person p ON dm.person_id = p.id "
        "GROUP BY dm.person_id ORDER BY doc_count DESC LIMIT 15"
    ).fetchall()
    if top:
        print(f"\n  Most documented people:")
        for given, sur, birth, count, ver in top:
            check = f" ({ver} verified)" if ver else ""
            print(f"    {given or ''} {sur or '':20s} b.{birth or '?':10s}  {count} doc(s){check}")

    if unmatched > 0:
        unm_sample = conn.execute(
            "SELECT filename FROM document d WHERE NOT EXISTS "
            "(SELECT 1 FROM document_match dm WHERE dm.document_id = d.id) "
            "ORDER BY filename LIMIT 10"
        ).fetchall()
        print(f"\n  Sample unmatched documents:")
        for (fn,) in unm_sample:
            print(f"    {fn}")

    conn.close()


# ---------------------------------------------------------------------------
# REVIEW
# ---------------------------------------------------------------------------

def review_matches():
    """Interactive review of unverified matches."""
    if not os.path.exists(DB_PATH):
        print("No database found.")
        return

    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)

    unverified = conn.execute(
        "SELECT dm.id, d.filename, p.given_name, p.surname, p.birth_date, "
        "dm.confidence, dm.snippet, dm.match_type "
        "FROM document_match dm "
        "JOIN document d ON dm.document_id = d.id "
        "JOIN person p ON dm.person_id = p.id "
        "WHERE dm.verified = 0 "
        "ORDER BY dm.confidence DESC"
    ).fetchall()

    if not unverified:
        print("No unverified matches to review!")
        conn.close()
        return

    print(f"\n{len(unverified)} unverified matches to review.")
    print("For each, type: y=correct, n=wrong, s=skip, q=quit\n")

    reviewed = 0
    accepted = 0
    rejected = 0

    for mid, filename, given, surname, birth, conf, snippet, mtype in unverified:
        name = f"{given or ''} {surname or ''}".strip()
        print(f"  [{reviewed+1}/{len(unverified)}] Document: {filename}")
        print(f"  Person:   {name} (b. {birth or '?'})")
        print(f"  Match:    {conf:.0%} via {mtype} — \"{snippet}\"")

        while True:
            ans = input("  [y/n/s/q] > ").strip().lower()
            if ans in ("y", "n", "s", "q"):
                break
            print("  Please enter y, n, s, or q")

        if ans == "q":
            break
        elif ans == "y":
            conn.execute("UPDATE document_match SET verified = 1 WHERE id = ?", (mid,))
            accepted += 1
        elif ans == "n":
            conn.execute("DELETE FROM document_match WHERE id = ?", (mid,))
            rejected += 1

        reviewed += 1
        print()

    conn.commit()
    conn.close()
    print(f"\nReviewed {reviewed}: {accepted} accepted, {rejected} rejected")


# ---------------------------------------------------------------------------
# BOOST CONFIDENCE
# ---------------------------------------------------------------------------

def boost_confidence():
    """Recalculate confidence including document match bonuses.

    Each verified document match:   +5 points
    Each unverified match:          +2 points
    Capped at +15 total bonus from documents.
    """
    if not os.path.exists(DB_PATH):
        print("No database found.")
        return

    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)

    doc_scores = conn.execute(
        "SELECT person_id, "
        "       SUM(CASE WHEN verified = 1 THEN 5 ELSE 2 END) as bonus "
        "FROM document_match "
        "GROUP BY person_id"
    ).fetchall()

    if not doc_scores:
        print("No document matches to boost from.")
        conn.close()
        return

    updated = 0
    for pid, bonus in doc_scores:
        capped_bonus = min(bonus, 15)
        current = conn.execute(
            "SELECT confidence FROM person WHERE id = ?", (pid,)
        ).fetchone()
        if current:
            new_score = min(current[0] + capped_bonus, 100)
            if new_score >= 80:
                tier = "high"
            elif new_score >= 50:
                tier = "medium"
            elif new_score >= 20:
                tier = "low"
            else:
                tier = "speculative"
            conn.execute(
                "UPDATE person SET confidence = ?, confidence_tier = ? WHERE id = ?",
                (new_score, tier, pid)
            )
            updated += 1

    conn.commit()
    conn.close()
    print(f"Boosted confidence for {updated} people based on {len(doc_scores)} document matches")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    args = sys.argv[1:]

    if "--stats" in args:
        show_stats()
    elif "--review" in args:
        review_matches()
    elif "--boost" in args:
        boost_confidence()
    elif "--export-only" in args:
        export_json()
    elif "--thumbnails-only" in args:
        # Just regenerate thumbnails for all existing files
        folder = next((a for a in args if not a.startswith("--")), str(RAW_DIR))
        conn = sqlite3.connect(DB_PATH)
        ensure_tables(conn)
        files = collect_files(folder)
        os.makedirs(THUMB_DIR, exist_ok=True)
        count = 0
        for filepath in files:
            ext = filepath.suffix.lower()
            if ext in SUPPORTED_EXTS - {".pdf", ".doc", ".docx"}:
                thumb_path = THUMB_DIR / (filepath.stem + ".jpg")
                if make_thumbnail(str(filepath), str(thumb_path)):
                    count += 1
                    conn.execute(
                        "UPDATE document SET has_thumb = 1 WHERE filename = ?",
                        (filepath.name,)
                    )
        conn.commit()
        conn.close()
        print(f"Generated {count} thumbnails")
        export_json()
    else:
        folder = next((a for a in args if not a.startswith("--")), str(RAW_DIR))
        rescan = "--rescan" in args
        filename_only = "--filename-only" in args
        vision = "--vision" in args
        scan_folder(folder, rescan=rescan, filename_only=filename_only, vision=vision)


if __name__ == "__main__":
    main()
