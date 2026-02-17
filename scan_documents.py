#!/usr/bin/env python3
"""
scan_documents.py — OCR family documents and match them to people in lineage.db.

Walks a folder of photos/scans (JPG, PNG, TIFF, PDF), runs Tesseract OCR on each,
extracts names and dates from the text, and fuzzy-matches them to people in the
database.  Matched documents boost a person's confidence score.

Usage:
    python scan_documents.py <folder>              # scan a folder
    python scan_documents.py <folder> --rescan      # re-OCR everything
    python scan_documents.py --review               # review unverified matches
    python scan_documents.py --stats                # show document stats
    python scan_documents.py --boost                # recalculate confidence with doc bonus

Requires:
    pip install Pillow pytesseract pdf2image
    Tesseract OCR installed at "C:\Program Files\Tesseract-OCR\tesseract.exe"
"""

import hashlib, json, os, re, sqlite3, sys
from datetime import datetime
from pathlib import Path
from difflib import SequenceMatcher

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

TESSERACT_CMD = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
SUPPORTED_EXTS = {".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp", ".gif", ".webp", ".pdf"}
SCRIPT_DIR = Path(__file__).parent
DB_PATH = str(SCRIPT_DIR / "lineage.db")

# Minimum similarity ratio to consider a name match (0.0-1.0)
NAME_MATCH_THRESHOLD = 0.78
# Minimum OCR text length to bother processing
MIN_TEXT_LENGTH = 10


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
    if any(w in lower for w in ("birth", "born")):
        return "certificate"
    if any(w in lower for w in ("death", "died", "obit")):
        return "obituary"
    if any(w in lower for w in ("marriage", "wedding", "marr")):
        return "certificate"
    if any(w in lower for w in ("census",)):
        return "census"
    if any(w in lower for w in ("military", "draft", "service")):
        return "military"
    if any(w in lower for w in ("letter", "correspondence")):
        return "letter"
    if any(w in lower for w in (".jpg", ".jpeg", ".png", ".gif", ".webp")):
        return "photo"
    return "other"


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

    img = Image.open(filepath)
    # Convert to RGB if needed (handles RGBA, palette, etc.)
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
        print("ERROR: Install required packages: pip install pytesseract pdf2image")
        print("       Also need poppler: https://github.com/oschwartz10612/poppler-windows/releases")
        sys.exit(1)

    pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

    pages = convert_from_path(filepath, dpi=300)
    texts = []
    for page in pages:
        texts.append(pytesseract.image_to_string(page, lang="eng"))
    return "\n".join(texts).strip()


# ---------------------------------------------------------------------------
# TEXT EXTRACTION — pull names and dates from OCR output
# ---------------------------------------------------------------------------

# Common date patterns found in genealogical documents
DATE_PATTERNS = [
    # "January 15, 1892" / "Jan 15 1892"
    r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
    r"\.?\s+\d{1,2},?\s+\d{4}",
    # "15 January 1892" / "15 Jan 1892"
    r"\d{1,2}\s+(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
    r"\.?,?\s+\d{4}",
    # "01/15/1892" or "1/15/1892"
    r"\d{1,2}/\d{1,2}/\d{4}",
    # "1892-01-15" ISO
    r"\d{4}-\d{2}-\d{2}",
    # bare 4-digit year (1700-2030)
    r"\b1[7-9]\d{2}\b|\b20[0-3]\d\b",
]


def extract_dates(text):
    """Extract date-like strings from OCR text."""
    dates = []
    for pattern in DATE_PATTERNS:
        for m in re.finditer(pattern, text, re.IGNORECASE):
            dates.append(m.group(0).strip())
    return dates


def extract_years(text):
    """Extract 4-digit years from text."""
    years = set()
    for m in re.finditer(r"\b(1[7-9]\d{2}|20[0-3]\d)\b", text):
        years.add(int(m.group(1)))
    return years


def extract_potential_names(text):
    """Extract sequences that look like personal names from OCR text.

    Heuristic: sequences of 2-4 capitalised words that don't look like
    addresses or common phrases.
    """
    # Kill obvious non-name lines
    noise_words = {
        "COUNTY", "TOWNSHIP", "STATE", "CERTIFICATE", "DEPARTMENT",
        "REGISTRAR", "BUREAU", "VITAL", "STATISTICS", "RECORD",
        "HEREBY", "CERTIFY", "ISSUED", "FILED", "PAGE", "VOLUME",
        "DISTRICT", "PRECINCT", "WARD", "RESIDENCE", "OCCUPATION",
        "WITNESS", "CHURCH", "CEMETERY", "FUNERAL", "HOSPITAL",
        "BORN", "DIED", "MARRIED", "BAPTIZED", "BURIED",
        "FATHER", "MOTHER", "HUSBAND", "WIFE", "CHILD", "SON", "DAUGHTER",
        "NAME", "DATE", "PLACE", "BIRTH", "DEATH", "MARRIAGE",
    }

    names = []
    # Look for "Firstname [Middle] Lastname" patterns
    # Capital letter followed by lowercase, repeated 2-4 times
    for m in re.finditer(
        r"\b([A-Z][a-z]{1,20}(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]{1,20}(?:\s+[A-Z][a-z]{1,20})?)\b",
        text
    ):
        candidate = m.group(1).strip()
        words = candidate.split()
        # Skip if any word is a noise word
        if any(w.upper() in noise_words for w in words):
            continue
        # Must have at least 2 real words (not just initials)
        real_words = [w for w in words if len(w) > 1]
        if len(real_words) >= 2:
            names.append(candidate)

    # Also try ALL-CAPS names (common in official docs)
    for m in re.finditer(
        r"\b([A-Z]{2,20}\s+[A-Z]\.?\s+[A-Z]{2,20}|[A-Z]{2,20}\s+[A-Z]{2,20})\b",
        text
    ):
        candidate = m.group(1).strip()
        words = candidate.split()
        if any(w.upper() in noise_words for w in words):
            continue
        if len(words) >= 2:
            # Convert to title case for matching
            names.append(candidate.title())

    return list(set(names))


# ---------------------------------------------------------------------------
# MATCHING — compare OCR extracts against the database
# ---------------------------------------------------------------------------

def name_similarity(name1, name2):
    """Compute similarity between two names (0.0-1.0)."""
    n1 = name1.lower().strip()
    n2 = name2.lower().strip()
    if n1 == n2:
        return 1.0
    return SequenceMatcher(None, n1, n2).ratio()


def match_document(ocr_text, people, years_in_doc):
    """Match OCR text against database people. Returns list of (person_id, confidence, snippet, match_type)."""
    matches = []
    names_found = extract_potential_names(ocr_text)

    for person in people:
        pid = person["id"]
        full_name = f"{person['given_name'] or ''} {person['surname'] or ''}".strip()
        if not full_name or len(full_name) < 3:
            continue

        given = person["given_name"] or ""
        surname = person["surname"] or ""

        best_score = 0.0
        best_snippet = ""
        match_type = "ocr_auto"

        # Check each extracted name against this person
        for extracted_name in names_found:
            sim = name_similarity(full_name, extracted_name)
            if sim > best_score:
                best_score = sim
                best_snippet = extracted_name

            # Also try surname match (shorter names get partial credit)
            if surname and len(surname) > 2:
                sur_sim = name_similarity(surname, extracted_name.split()[-1]) if extracted_name.split() else 0
                if sur_sim > 0.9 and best_score < 0.5:
                    # Surname-only match: lower confidence
                    best_score = max(best_score, sur_sim * 0.5)
                    best_snippet = extracted_name

        # Boost score if birth/death year appears in the document
        year_boost = 0
        if years_in_doc:
            birth_year = extract_year_from_str(person.get("birth_date"))
            death_year = extract_year_from_str(person.get("death_date"))
            if birth_year and birth_year in years_in_doc:
                year_boost += 0.15
            if death_year and death_year in years_in_doc:
                year_boost += 0.10

        final_score = min(best_score + year_boost, 1.0)

        if final_score >= NAME_MATCH_THRESHOLD:
            matches.append((pid, final_score, best_snippet, match_type))

    # Sort by confidence descending
    matches.sort(key=lambda x: x[1], reverse=True)
    return matches


def extract_year_from_str(date_str):
    """Extract a 4-digit year from a date string."""
    if not date_str:
        return None
    m = re.search(r"(\d{4})", str(date_str))
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# MAIN ACTIONS
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
            file_hash   TEXT
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
        CREATE INDEX IF NOT EXISTS idx_doc_match_doc ON document_match(document_id);
    """)


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


def scan_folder(folder, rescan=False):
    """Walk folder, OCR each file, match to people in lineage.db."""
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

    # Collect files
    files = []
    for root, dirs, filenames in os.walk(folder):
        for fn in filenames:
            ext = Path(fn).suffix.lower()
            if ext in SUPPORTED_EXTS:
                files.append(Path(root) / fn)

    print(f"Found {len(files)} scannable files in {folder}")
    if not files:
        conn.close()
        return

    # Track existing hashes to skip duplicates
    existing_hashes = set()
    if not rescan:
        for row in conn.execute("SELECT file_hash FROM document WHERE file_hash IS NOT NULL"):
            existing_hashes.add(row[0])

    total_matches = 0
    new_docs = 0
    skipped = 0

    for i, filepath in enumerate(files, 1):
        rel_path = str(filepath)
        fn = filepath.name

        # Check if already scanned
        if not rescan:
            existing = conn.execute(
                "SELECT id FROM document WHERE filepath = ?", (rel_path,)
            ).fetchone()
            if existing:
                skipped += 1
                continue

        # Check hash for duplicate files
        fhash = file_hash(filepath)
        if not rescan and fhash in existing_hashes:
            print(f"  [{i}/{len(files)}] SKIP (duplicate file): {fn}")
            skipped += 1
            continue

        print(f"  [{i}/{len(files)}] OCR: {fn}...", end=" ", flush=True)

        try:
            text = ocr_image(str(filepath))
        except Exception as e:
            print(f"FAILED: {e}")
            continue

        if len(text) < MIN_TEXT_LENGTH:
            print(f"too little text ({len(text)} chars)")
            # Still store the document so we don't re-scan it
            conn.execute(
                "INSERT OR REPLACE INTO document (filename, filepath, doc_type, ocr_text, ocr_date, file_hash) "
                "VALUES (?,?,?,?,?,?)",
                (fn, rel_path, guess_doc_type(fn), text, datetime.now().isoformat(), fhash)
            )
            conn.commit()
            new_docs += 1
            continue

        doc_type = guess_doc_type(fn)
        conn.execute(
            "INSERT OR REPLACE INTO document (filename, filepath, doc_type, ocr_text, ocr_date, file_hash) "
            "VALUES (?,?,?,?,?,?)",
            (fn, rel_path, doc_type, text, datetime.now().isoformat(), fhash)
        )
        conn.commit()
        new_docs += 1

        doc_id = conn.execute(
            "SELECT id FROM document WHERE filepath = ?", (rel_path,)
        ).fetchone()[0]

        existing_hashes.add(fhash)

        # Extract and match
        years = extract_years(text)
        matches = match_document(text, people, years)

        if matches:
            print(f"{len(matches)} match(es)")
            for pid, conf, snippet, mtype in matches[:5]:  # top 5 per doc
                person = next((p for p in people if p["id"] == pid), None)
                pname = f"{person['given_name'] or ''} {person['surname'] or ''}".strip() if person else f"#{pid}"
                print(f"       → {pname} ({conf:.0%}) snippet: \"{snippet}\"")
                conn.execute(
                    "INSERT OR REPLACE INTO document_match "
                    "(document_id, person_id, match_type, confidence, snippet, verified) "
                    "VALUES (?,?,?,?,?,0)",
                    (doc_id, pid, mtype, round(conf, 3), snippet)
                )
                total_matches += 1
        else:
            names = extract_potential_names(text)
            print(f"no matches (found names: {names[:3]})")

        conn.commit()

    conn.close()
    print(f"\n{'='*60}")
    print(f"Scan complete:")
    print(f"  New documents: {new_docs}")
    print(f"  Skipped (already scanned): {skipped}")
    print(f"  Total matches found: {total_matches}")
    print(f"{'='*60}")


def show_stats():
    """Print document and match statistics."""
    if not os.path.exists(DB_PATH):
        print("No database found.")
        return

    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)

    total_docs = conn.execute("SELECT COUNT(*) FROM document").fetchone()[0]
    total_matches = conn.execute("SELECT COUNT(*) FROM document_match").fetchone()[0]
    verified = conn.execute("SELECT COUNT(*) FROM document_match WHERE verified = 1").fetchone()[0]
    unverified = total_matches - verified

    by_type = conn.execute(
        "SELECT doc_type, COUNT(*) FROM document GROUP BY doc_type ORDER BY COUNT(*) DESC"
    ).fetchall()

    people_with_docs = conn.execute(
        "SELECT COUNT(DISTINCT person_id) FROM document_match"
    ).fetchone()[0]

    avg_conf = conn.execute(
        "SELECT AVG(confidence) FROM document_match"
    ).fetchone()[0]

    print(f"\n{'='*60}")
    print(f"DOCUMENT STATS")
    print(f"{'='*60}")
    print(f"  Total documents:  {total_docs}")
    print(f"  Total matches:    {total_matches}  ({verified} verified, {unverified} unverified)")
    print(f"  People with docs: {people_with_docs}")
    print(f"  Avg match conf:   {avg_conf:.1%}" if avg_conf else "  No matches yet")
    print(f"\n  By document type:")
    for dtype, count in by_type:
        print(f"    {dtype or 'unknown':15s}  {count}")

    # Top matched people
    top = conn.execute(
        "SELECT p.given_name, p.surname, COUNT(*) as doc_count "
        "FROM document_match dm JOIN person p ON dm.person_id = p.id "
        "GROUP BY dm.person_id ORDER BY doc_count DESC LIMIT 10"
    ).fetchall()
    if top:
        print(f"\n  Most documented people:")
        for given, sur, count in top:
            print(f"    {given or ''} {sur or '':20s}  {count} doc(s)")

    conn.close()


def review_matches():
    """Interactive review of unverified matches."""
    if not os.path.exists(DB_PATH):
        print("No database found.")
        return

    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)

    unverified = conn.execute(
        "SELECT dm.id, d.filename, p.given_name, p.surname, p.birth_date, "
        "dm.confidence, dm.snippet "
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

    for mid, filename, given, surname, birth, conf, snippet in unverified:
        name = f"{given or ''} {surname or ''}".strip()
        print(f"  Document: {filename}")
        print(f"  Person:   {name} (b. {birth or '?'})")
        print(f"  Match:    {conf:.0%} — snippet: \"{snippet}\"")

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
        else:
            pass  # skip

        reviewed += 1
        print()

    conn.commit()
    conn.close()
    print(f"\nReviewed {reviewed}: {accepted} accepted, {rejected} rejected")


def boost_confidence():
    """Recalculate confidence scores including document match bonuses.

    Each verified document match adds up to +5 points.
    Each unverified match adds up to +2 points.
    Capped at +15 total bonus from documents.
    """
    if not os.path.exists(DB_PATH):
        print("No database found.")
        return

    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)

    # Get document match counts per person
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

    # Re-run base confidence first (import_gedcom's compute_confidence handles this)
    # Here we just add the document bonus ON TOP of existing scores
    updated = 0
    for pid, bonus in doc_scores:
        capped_bonus = min(bonus, 15)
        current = conn.execute(
            "SELECT confidence FROM person WHERE id = ?", (pid,)
        ).fetchone()
        if current:
            new_score = min(current[0] + capped_bonus, 100)
            # Recalculate tier
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
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)

    if sys.argv[1] == "--stats":
        show_stats()
    elif sys.argv[1] == "--review":
        review_matches()
    elif sys.argv[1] == "--boost":
        boost_confidence()
    else:
        folder = sys.argv[1]
        rescan = "--rescan" in sys.argv
        scan_folder(folder, rescan=rescan)


if __name__ == "__main__":
    main()
