#!/usr/bin/env python3
"""
review_server.py — Lightweight API server for the OCR review system.

Serves the lack-lineage site + provides REST endpoints for:
  - Listing documents with OCR results from multiple engines
  - Verifying / rejecting person matches
  - Editing OCR text corrections
  - Adding manual person links
  - Running OCR engines on demand

Usage:
    python review_server.py                # start on port 8080
    python review_server.py --port 9090    # custom port
"""

import json, os, re, sqlite3, sys, time, base64, io, urllib.request
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, parse_qs

SCRIPT_DIR = Path(__file__).parent
DB_PATH = str(SCRIPT_DIR / "lineage.db")
OLLAMA_URL = "http://127.0.0.1:11434"

# OCR engine registry
OCR_ENGINES = {
    "tesseract": {
        "name": "Tesseract",
        "type": "local",
        "description": "Fast local OCR — good for printed text",
    },
    "glm-ocr": {
        "name": "GLM-OCR",
        "type": "ollama",
        "model": "glm-ocr",
        "description": "AI OCR — accurate text extraction, 2.2GB",
        "safe_dims": (512, 1024),  # glm-ocr needs specific dimensions
    },
    "minicpm-v": {
        "name": "MiniCPM-V",
        "type": "ollama",
        "model": "minicpm-v",
        "description": "Vision model — document understanding, 5.5GB",
    },
    "numarkdown": {
        "name": "NuMarkdown-Thinking",
        "type": "ollama",
        "model": "Maternion/NuMarkdown-Thinking",
        "description": "Markdown extraction — good for structured docs, 6GB",
    },
}

VISION_PROMPT = """Analyze this genealogy document image. Extract ALL text and information:

1. PEOPLE: Every person name. Format: "PERSON: Firstname Lastname"
2. DATES: Every date. Format: "DATE: YYYY-MM-DD description"
3. PLACES: Locations. Format: "PLACE: City, State/Country"
4. RELATIONSHIPS: Family relationships. Format: "REL: Person1 is [relationship] of Person2"
5. DOCTYPE: One of: certificate, obituary, census, military, newspaper, letter, photo, record, other
6. SUMMARY: One sentence describing the document.

Be thorough. Extract every name and date visible."""

OCR_PROMPT = "Extract ALL text visible in this document exactly as written. Include every name, date, place, and any other text."


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    ensure_review_tables(conn)
    return conn


def ensure_review_tables(conn):
    """Create review-system tables if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ocr_result (
            id          INTEGER PRIMARY KEY,
            document_id INTEGER REFERENCES document(id),
            engine      TEXT NOT NULL,
            raw_text    TEXT,
            run_date    TEXT,
            run_time_ms INTEGER,
            error       TEXT,
            UNIQUE(document_id, engine)
        );
        CREATE TABLE IF NOT EXISTS review_edit (
            id          INTEGER PRIMARY KEY,
            document_id INTEGER REFERENCES document(id),
            reviewer    TEXT DEFAULT 'reviewer',
            field       TEXT NOT NULL,
            old_value   TEXT,
            new_value   TEXT,
            edit_date   TEXT,
            notes       TEXT
        );
        CREATE TABLE IF NOT EXISTS review_status (
            document_id INTEGER PRIMARY KEY REFERENCES document(id),
            status      TEXT DEFAULT 'pending',
            reviewer    TEXT,
            review_date TEXT,
            notes       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_ocr_result_doc ON ocr_result(document_id);
        CREATE INDEX IF NOT EXISTS idx_review_edit_doc ON review_edit(document_id);
    """)
    # Add corrected_text column to document if not there
    try:
        conn.execute("SELECT corrected_text FROM document LIMIT 0")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE document ADD COLUMN corrected_text TEXT")
    try:
        conn.execute("SELECT review_status FROM document LIMIT 0")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE document ADD COLUMN review_status TEXT DEFAULT 'pending'")
    conn.commit()


def encode_image(filepath, max_dim=1200, safe_dims=None):
    """Load and encode image for Ollama API."""
    from PIL import Image
    img = Image.open(filepath)
    if img.mode not in ("L", "RGB"):
        img = img.convert("RGB")
    if safe_dims:
        img = img.resize(safe_dims, Image.LANCZOS)
    else:
        w, h = img.size
        if max(w, h) > max_dim:
            r = max_dim / max(w, h)
            img = img.resize((int(w * r), int(h * r)), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def run_tesseract(filepath):
    """Run Tesseract OCR on an image."""
    import pytesseract
    from PIL import Image
    pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    img = Image.open(filepath)
    if img.mode not in ("L", "RGB"):
        img = img.convert("RGB")
    return pytesseract.image_to_string(img, lang="eng").strip()


def run_ollama_ocr(filepath, model, prompt, safe_dims=None):
    """Run an Ollama vision model on an image."""
    enc = encode_image(filepath, safe_dims=safe_dims)
    payload = json.dumps({
        "model": model,
        "prompt": prompt,
        "stream": False,
        "images": [enc],
    }).encode("utf-8")
    req = urllib.request.Request(
        f"{OLLAMA_URL}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=300) as resp:
        result = json.loads(resp.read().decode("utf-8"))
        return result.get("response", "").strip()


def run_engine(engine_id, filepath):
    """Run a specific OCR engine. Returns (text, error)."""
    eng = OCR_ENGINES.get(engine_id)
    if not eng:
        return None, f"Unknown engine: {engine_id}"
    try:
        if eng["type"] == "local":
            text = run_tesseract(filepath)
            return text, None
        elif eng["type"] == "ollama":
            prompt = VISION_PROMPT if engine_id == "minicpm-v" else OCR_PROMPT
            safe_dims = eng.get("safe_dims")
            text = run_ollama_ocr(filepath, eng["model"], prompt, safe_dims=safe_dims)
            return text, None
    except Exception as e:
        return None, str(e)


# ---------------------------------------------------------------------------
# API Handlers
# ---------------------------------------------------------------------------

def api_documents(qs):
    """GET /api/documents — list documents with review status."""
    conn = get_db()
    page = int(qs.get("page", ["1"])[0])
    per_page = int(qs.get("per_page", ["50"])[0])
    status_filter = qs.get("status", [None])[0]
    search = qs.get("q", [None])[0]
    sort = qs.get("sort", ["seq"])[0]

    where = []
    params = []
    if status_filter and status_filter != "all":
        where.append("COALESCE(d.review_status, 'pending') = ?")
        params.append(status_filter)
    if search:
        where.append("(d.filename LIKE ? OR d.description LIKE ? OR d.ocr_text LIKE ?)")
        params.extend([f"%{search}%"] * 3)

    where_sql = " WHERE " + " AND ".join(where) if where else ""

    sort_map = {"seq": "d.seq_num, d.id", "name": "d.filename",
                "type": "d.doc_type, d.filename", "matches": "match_count DESC"}
    order = sort_map.get(sort, "d.seq_num, d.id")

    total = conn.execute(f"SELECT COUNT(*) FROM document d {where_sql}", params).fetchone()[0]

    rows = conn.execute(f"""
        SELECT d.id, d.filename, d.filepath, d.doc_type, d.description,
               d.has_thumb, d.seq_num, COALESCE(d.review_status, 'pending') as review_status,
               d.corrected_text,
               LENGTH(COALESCE(d.ocr_text, '')) as ocr_len,
               (SELECT COUNT(*) FROM document_match dm WHERE dm.document_id = d.id) as match_count,
               (SELECT COUNT(*) FROM document_match dm WHERE dm.document_id = d.id AND dm.verified = 1) as verified_count,
               (SELECT COUNT(*) FROM ocr_result r WHERE r.document_id = d.id) as engine_count
        FROM document d {where_sql}
        ORDER BY {order}
        LIMIT ? OFFSET ?
    """, params + [per_page, (page - 1) * per_page]).fetchall()

    docs = []
    for r in rows:
        thumb_file = Path(r["filename"]).stem + ".jpg" if r["has_thumb"] else None
        docs.append({
            "id": r["id"],
            "filename": r["filename"],
            "filepath": r["filepath"],
            "type": r["doc_type"],
            "title": r["description"] or r["filename"],
            "thumb": f"data/thumbs/{thumb_file}" if thumb_file else None,
            "review_status": r["review_status"],
            "has_corrected": bool(r["corrected_text"]),
            "ocr_length": r["ocr_len"],
            "match_count": r["match_count"],
            "verified_count": r["verified_count"],
            "engine_count": r["engine_count"],
            "seq": r["seq_num"],
        })

    # Stats
    stats = {}
    for st in ["pending", "approved", "rejected", "needs_review"]:
        stats[st] = conn.execute(
            "SELECT COUNT(*) FROM document WHERE COALESCE(review_status, 'pending') = ?", (st,)
        ).fetchone()[0]
    stats["total"] = total

    conn.close()
    return {"documents": docs, "total": total, "page": page, "per_page": per_page, "stats": stats}


def api_document_detail(doc_id):
    """GET /api/documents/<id> — full document with all OCR results and matches."""
    conn = get_db()

    doc = conn.execute("""
        SELECT d.*, LENGTH(COALESCE(d.ocr_text, '')) as ocr_len
        FROM document d WHERE d.id = ?
    """, (doc_id,)).fetchone()
    if not doc:
        return {"error": "Document not found"}, 404

    # All OCR results from different engines
    ocr_results = conn.execute("""
        SELECT engine, raw_text, run_date, run_time_ms, error
        FROM ocr_result WHERE document_id = ?
        ORDER BY engine
    """, (doc_id,)).fetchall()

    # Person matches
    matches = conn.execute("""
        SELECT dm.id, dm.person_id, dm.match_type, dm.confidence, dm.snippet, dm.verified,
               p.given_name, p.surname, p.birth_date, p.death_date
        FROM document_match dm
        JOIN person p ON dm.person_id = p.id
        WHERE dm.document_id = ?
        ORDER BY dm.confidence DESC
    """, (doc_id,)).fetchall()

    # Review edits history
    edits = conn.execute("""
        SELECT field, old_value, new_value, edit_date, reviewer, notes
        FROM review_edit WHERE document_id = ?
        ORDER BY edit_date DESC
    """, (doc_id,)).fetchall()

    thumb_file = Path(doc["filename"]).stem + ".jpg" if doc["has_thumb"] else None

    result = {
        "id": doc["id"],
        "filename": doc["filename"],
        "filepath": doc["filepath"],
        "type": doc["doc_type"],
        "title": doc["description"] or doc["filename"],
        "thumb": f"data/thumbs/{thumb_file}" if thumb_file else None,
        "ocr_text": doc["ocr_text"] or "",
        "corrected_text": doc["corrected_text"] or "",
        "vision_text": doc["vision_text"] or "",
        "review_status": doc["review_status"] or "pending",
        "seq": doc["seq_num"],
        "ocr_results": [
            {
                "engine": r["engine"],
                "text": r["raw_text"] or "",
                "date": r["run_date"],
                "time_ms": r["run_time_ms"],
                "error": r["error"],
            }
            for r in ocr_results
        ],
        "matches": [
            {
                "id": m["id"],
                "person_id": m["person_id"],
                "name": f"{m['given_name'] or ''} {m['surname'] or ''}".strip(),
                "birth": m["birth_date"],
                "death": m["death_date"],
                "match_type": m["match_type"],
                "confidence": round(m["confidence"], 3),
                "snippet": m["snippet"],
                "verified": bool(m["verified"]),
            }
            for m in matches
        ],
        "edits": [
            {
                "field": e["field"],
                "old_value": e["old_value"],
                "new_value": e["new_value"],
                "date": e["edit_date"],
                "reviewer": e["reviewer"],
                "notes": e["notes"],
            }
            for e in edits
        ],
    }
    conn.close()
    return result


def api_run_ocr(doc_id, body):
    """POST /api/documents/<id>/ocr — run OCR engine(s) on a document."""
    engine_id = body.get("engine", "all")
    conn = get_db()

    doc = conn.execute("SELECT filepath FROM document WHERE id = ?", (doc_id,)).fetchone()
    if not doc:
        conn.close()
        return {"error": "Document not found"}, 404

    filepath = doc["filepath"]
    if not os.path.exists(filepath):
        conn.close()
        return {"error": f"File not found: {filepath}"}, 404

    engines_to_run = [engine_id] if engine_id != "all" else list(OCR_ENGINES.keys())
    results = []

    for eid in engines_to_run:
        t0 = time.time()
        text, error = run_engine(eid, filepath)
        elapsed_ms = int((time.time() - t0) * 1000)

        conn.execute("""
            INSERT OR REPLACE INTO ocr_result (document_id, engine, raw_text, run_date, run_time_ms, error)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (doc_id, eid, text, datetime.now().isoformat(), elapsed_ms, error))

        results.append({
            "engine": eid,
            "text": text or "",
            "time_ms": elapsed_ms,
            "error": error,
        })

    conn.commit()
    conn.close()
    return {"results": results}


def api_verify_match(match_id, body):
    """POST /api/matches/<id>/verify — verify or reject a match."""
    action = body.get("action", "verify")  # "verify" or "reject"
    conn = get_db()

    match = conn.execute("SELECT * FROM document_match WHERE id = ?", (match_id,)).fetchone()
    if not match:
        conn.close()
        return {"error": "Match not found"}, 404

    if action == "verify":
        conn.execute("UPDATE document_match SET verified = 1 WHERE id = ?", (match_id,))
    elif action == "reject":
        conn.execute("DELETE FROM document_match WHERE id = ?", (match_id,))

    conn.commit()
    conn.close()
    return {"ok": True, "action": action}


def api_add_match(doc_id, body):
    """POST /api/documents/<id>/match — manually add a person match."""
    person_id = body.get("person_id")
    if not person_id:
        return {"error": "person_id required"}, 400

    conn = get_db()

    existing = conn.execute(
        "SELECT id FROM document_match WHERE document_id = ? AND person_id = ?",
        (doc_id, person_id)
    ).fetchone()

    if existing:
        conn.execute(
            "UPDATE document_match SET verified = 1, match_type = 'manual', confidence = 1.0 WHERE id = ?",
            (existing["id"],)
        )
    else:
        conn.execute("""
            INSERT INTO document_match (document_id, person_id, match_type, confidence, snippet, verified)
            VALUES (?, ?, 'manual', 1.0, 'Manual link', 1)
        """, (doc_id, person_id))

    conn.commit()
    conn.close()
    return {"ok": True}


def api_save_correction(doc_id, body):
    """POST /api/documents/<id>/correct — save corrected OCR text."""
    corrected = body.get("corrected_text", "")
    notes = body.get("notes", "")
    reviewer = body.get("reviewer", "reviewer")

    conn = get_db()
    old = conn.execute("SELECT corrected_text, ocr_text FROM document WHERE id = ?", (doc_id,)).fetchone()
    if not old:
        conn.close()
        return {"error": "Document not found"}, 404

    old_text = old["corrected_text"] or old["ocr_text"] or ""

    conn.execute("UPDATE document SET corrected_text = ? WHERE id = ?", (corrected, doc_id))

    conn.execute("""
        INSERT INTO review_edit (document_id, reviewer, field, old_value, new_value, edit_date, notes)
        VALUES (?, ?, 'corrected_text', ?, ?, ?, ?)
    """, (doc_id, reviewer, old_text[:500], corrected[:500], datetime.now().isoformat(), notes))

    conn.commit()
    conn.close()
    return {"ok": True}


def api_set_review_status(doc_id, body):
    """POST /api/documents/<id>/status — set review status."""
    status = body.get("status", "pending")
    reviewer = body.get("reviewer", "reviewer")
    notes = body.get("notes", "")

    if status not in ("pending", "approved", "rejected", "needs_review"):
        return {"error": "Invalid status"}, 400

    conn = get_db()
    conn.execute("UPDATE document SET review_status = ? WHERE id = ?", (status, doc_id))

    conn.execute("""
        INSERT OR REPLACE INTO review_status (document_id, status, reviewer, review_date, notes)
        VALUES (?, ?, ?, ?, ?)
    """, (doc_id, status, reviewer, datetime.now().isoformat(), notes))

    conn.commit()
    conn.close()
    return {"ok": True}


def api_people_search(qs):
    """GET /api/people — search people for manual matching."""
    q = qs.get("q", [""])[0]
    conn = get_db()

    if q:
        rows = conn.execute("""
            SELECT id, given_name, surname, birth_date, death_date
            FROM person
            WHERE given_name LIKE ? OR surname LIKE ?
            ORDER BY surname, given_name
            LIMIT 50
        """, (f"%{q}%", f"%{q}%")).fetchall()
    else:
        rows = conn.execute("""
            SELECT id, given_name, surname, birth_date, death_date
            FROM person ORDER BY surname, given_name LIMIT 100
        """).fetchall()

    people = [
        {
            "id": r["id"],
            "name": f"{r['given_name'] or ''} {r['surname'] or ''}".strip(),
            "birth": r["birth_date"],
            "death": r["death_date"],
        }
        for r in rows
    ]
    conn.close()
    return {"people": people}


def api_engines():
    """GET /api/engines — list available OCR engines."""
    # Check which Ollama models are available
    available = {}
    try:
        resp = urllib.request.urlopen(f"{OLLAMA_URL}/api/tags", timeout=5)
        tags = json.loads(resp.read().decode("utf-8"))
        models = [m["name"] for m in tags.get("models", [])]
    except Exception:
        models = []

    result = []
    for eid, eng in OCR_ENGINES.items():
        ready = True
        if eng["type"] == "ollama":
            ready = any(eng["model"] in m for m in models)
        result.append({
            "id": eid,
            "name": eng["name"],
            "description": eng["description"],
            "ready": ready,
        })
    return {"engines": result}


def api_stats():
    """GET /api/stats — review statistics."""
    conn = get_db()

    total = conn.execute("SELECT COUNT(*) FROM document").fetchone()[0]
    by_status = {}
    for r in conn.execute("SELECT COALESCE(review_status, 'pending') as s, COUNT(*) FROM document GROUP BY s"):
        by_status[r[0]] = r[1]

    total_matches = conn.execute("SELECT COUNT(*) FROM document_match").fetchone()[0]
    verified = conn.execute("SELECT COUNT(*) FROM document_match WHERE verified = 1").fetchone()[0]
    total_edits = conn.execute("SELECT COUNT(*) FROM review_edit").fetchone()[0]
    engines_run = conn.execute("SELECT engine, COUNT(*) FROM ocr_result GROUP BY engine").fetchall()

    conn.close()
    return {
        "total_documents": total,
        "by_status": by_status,
        "total_matches": total_matches,
        "verified_matches": verified,
        "total_edits": total_edits,
        "engines": {r[0]: r[1] for r in engines_run},
    }


def api_batch_status(body):
    """POST /api/batch/status — set review status for multiple documents."""
    doc_ids = body.get("doc_ids", [])
    status = body.get("status", "approved")
    reviewer = body.get("reviewer", "reviewer")

    conn = get_db()
    for doc_id in doc_ids:
        conn.execute("UPDATE document SET review_status = ? WHERE id = ?", (status, doc_id))
    conn.commit()
    conn.close()
    return {"ok": True, "count": len(doc_ids)}


# ---------------------------------------------------------------------------
# Admin / Dad's Desk API
# ---------------------------------------------------------------------------

def api_admin_person(person_id):
    """GET /api/admin/person/<id> — full person detail for editing."""
    conn = get_db()
    p = conn.execute("""
        SELECT id, xref, given_name, surname, suffix, sex,
               birth_date, birth_place, death_date, death_place,
               source_count, confidence, confidence_tier
        FROM person WHERE id = ?
    """, (person_id,)).fetchone()
    if not p:
        conn.close()
        return {"error": "Person not found"}, 404

    result = dict(p)
    result["name"] = f"{p['given_name'] or ''} {p['surname'] or ''}".strip()

    # Family
    parents = conn.execute("""
        SELECT p.id, p.given_name, p.surname, p.sex, p.birth_date, p.death_date
        FROM person p JOIN relationship r ON p.id = r.person1_id
        WHERE r.person2_id = ? AND r.rel_type = 'parent_child'
    """, (person_id,)).fetchall()
    result["parents"] = [dict(r) for r in parents]

    spouses = conn.execute("""
        SELECT p.id, p.given_name, p.surname, p.sex, p.birth_date, p.death_date
        FROM person p JOIN relationship r ON
            (r.person1_id = ? AND r.person2_id = p.id AND r.rel_type = 'spouse')
            OR (r.person2_id = ? AND r.person1_id = p.id AND r.rel_type = 'spouse')
    """, (person_id, person_id)).fetchall()
    result["spouses"] = [dict(r) for r in spouses]

    children = conn.execute("""
        SELECT p.id, p.given_name, p.surname, p.sex, p.birth_date, p.death_date
        FROM person p JOIN relationship r ON p.id = r.person2_id
        WHERE r.person1_id = ? AND r.rel_type = 'parent_child'
    """, (person_id,)).fetchall()
    result["children"] = [dict(r) for r in children]

    siblings = conn.execute("""
        SELECT DISTINCT p.id, p.given_name, p.surname, p.sex, p.birth_date, p.death_date
        FROM person p
        JOIN relationship r1 ON r1.person2_id = p.id AND r1.rel_type = 'parent_child'
        JOIN relationship r2 ON r2.person1_id = r1.person1_id AND r2.rel_type = 'parent_child'
        WHERE r2.person2_id = ? AND p.id != ?
    """, (person_id, person_id)).fetchall()
    result["siblings"] = [dict(r) for r in siblings]

    # Documents
    docs = conn.execute("""
        SELECT d.id, d.filename, d.filepath, d.doc_type, d.description,
               d.ocr_text, d.vision_text, d.corrected_text, d.review_status,
               dm.confidence as match_confidence, dm.match_type, dm.verified
        FROM document d
        JOIN document_match dm ON dm.document_id = d.id
        WHERE dm.person_id = ?
        ORDER BY d.id
    """, (person_id,)).fetchall()
    result["documents"] = [dict(r) for r in docs]

    # Notes (from review_edit for this person)
    notes = conn.execute("""
        SELECT id, field, old_value, new_value, edit_date, notes, reviewer
        FROM review_edit
        WHERE document_id IN (
            SELECT document_id FROM document_match WHERE person_id = ?
        ) OR notes LIKE ?
        ORDER BY edit_date DESC LIMIT 50
    """, (person_id, f"%person:{person_id}%")).fetchall()
    result["edit_history"] = [dict(r) for r in notes]

    # Admin notes
    ensure_admin_tables(conn)
    admin_notes = conn.execute("""
        SELECT id, note, created_date, reviewer FROM admin_note
        WHERE person_id = ? ORDER BY created_date DESC
    """, (person_id,)).fetchall()
    result["notes"] = [dict(r) for r in admin_notes]

    conn.close()
    return result


def api_admin_update_person(person_id, body):
    """POST /api/admin/person/<id> — update person fields."""
    conn = get_db()
    ensure_admin_tables(conn)

    old = conn.execute("SELECT * FROM person WHERE id = ?", (person_id,)).fetchone()
    if not old:
        conn.close()
        return {"error": "Person not found"}, 404

    editable = ["given_name", "surname", "suffix", "sex",
                "birth_date", "birth_place", "death_date", "death_place"]
    changes = []
    for field in editable:
        if field in body and body[field] != old[field]:
            changes.append((field, old[field], body[field]))
            conn.execute(f"UPDATE person SET {field} = ? WHERE id = ?",
                         (body[field], person_id))

    # Log changes
    for field, old_val, new_val in changes:
        conn.execute("""
            INSERT INTO admin_edit (person_id, field, old_value, new_value, edit_date, reviewer)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (person_id, field, old_val, new_val, datetime.now().isoformat(),
              body.get("reviewer", "Dad")))

    conn.commit()
    conn.close()
    return {"ok": True, "changes": len(changes)}


def api_admin_add_note(person_id, body):
    """POST /api/admin/person/<id>/note — add a note about a person."""
    note = body.get("note", "").strip()
    if not note:
        return {"error": "Note text required"}, 400

    conn = get_db()
    ensure_admin_tables(conn)
    conn.execute("""
        INSERT INTO admin_note (person_id, note, created_date, reviewer)
        VALUES (?, ?, ?, ?)
    """, (person_id, note, datetime.now().isoformat(), body.get("reviewer", "Dad")))
    conn.commit()
    conn.close()
    return {"ok": True}


def api_admin_delete_note(note_id):
    """POST /api/admin/note/<id>/delete — delete a note."""
    conn = get_db()
    ensure_admin_tables(conn)
    conn.execute("DELETE FROM admin_note WHERE id = ?", (note_id,))
    conn.commit()
    conn.close()
    return {"ok": True}


def api_admin_upload(person_id, filepath, filename, doc_type, description, data_bytes):
    """Handle file upload for a person — save image to raw-data/ and link."""
    import hashlib

    conn = get_db()
    ensure_admin_tables(conn)

    # Check person exists
    p = conn.execute("SELECT id FROM person WHERE id = ?", (person_id,)).fetchone()
    if not p:
        conn.close()
        return {"error": "Person not found"}, 404

    # Save file to raw-data/uploads/
    upload_dir = SCRIPT_DIR / "raw-data" / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)

    # Dedupe filename
    ext = Path(filename).suffix or ".jpg"
    safe_name = re.sub(r'[^\w\-.]', '_', Path(filename).stem)
    ts = int(time.time())
    dest_name = f"{safe_name}_{ts}{ext}"
    dest_path = upload_dir / dest_name

    with open(dest_path, "wb") as f:
        f.write(data_bytes)

    file_hash = hashlib.sha256(data_bytes).hexdigest()

    # Check for duplicate
    dup = conn.execute("SELECT id FROM document WHERE file_hash = ?", (file_hash,)).fetchone()
    if dup:
        os.remove(dest_path)
        conn.close()
        return {"error": f"Duplicate file — already exists as document #{dup['id']}"}, 409

    # Insert document
    rel_path = str(dest_path.relative_to(SCRIPT_DIR)).replace("\\", "/")
    conn.execute("""
        INSERT INTO document (filename, filepath, doc_type, description, file_hash)
        VALUES (?, ?, ?, ?, ?)
    """, (dest_name, rel_path, doc_type or "photo", description or "", file_hash))
    doc_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Link to person
    conn.execute("""
        INSERT INTO document_match (document_id, person_id, match_type, confidence, snippet, verified)
        VALUES (?, ?, 'manual', 1.0, 'Uploaded by Dad', 1)
    """, (doc_id, person_id))

    conn.commit()
    conn.close()
    return {"ok": True, "doc_id": doc_id, "filepath": rel_path}


def api_admin_people_list(qs):
    """GET /api/admin/people — paginated people list with counts."""
    q = qs.get("q", [""])[0]
    sort = qs.get("sort", ["name"])[0]
    tier = qs.get("tier", [""])[0]
    page = int(qs.get("page", ["1"])[0])
    per_page = 50

    conn = get_db()
    where = []
    params = []

    if q:
        words = q.strip().split()
        for word in words:
            where.append("(given_name LIKE ? OR surname LIKE ?)")
            params += [f"%{word}%", f"%{word}%"]
    if tier:
        where.append("confidence_tier = ?")
        params.append(tier)

    where_str = " WHERE " + " AND ".join(where) if where else ""

    order = {
        "name": "surname, given_name",
        "confidence": "confidence DESC",
        "id": "id",
        "birth": "birth_date",
    }.get(sort, "surname, given_name")

    total = conn.execute(f"SELECT COUNT(*) FROM person{where_str}", params).fetchone()[0]

    rows = conn.execute(f"""
        SELECT id, given_name, surname, suffix, sex,
               birth_date, death_date, confidence, confidence_tier
        FROM person{where_str}
        ORDER BY {order}
        LIMIT ? OFFSET ?
    """, params + [per_page, (page - 1) * per_page]).fetchall()

    # Doc counts per person
    people = []
    for r in rows:
        pid = r["id"]
        doc_count = conn.execute(
            "SELECT COUNT(*) FROM document_match WHERE person_id = ?", (pid,)
        ).fetchone()[0]
        p = dict(r)
        p["name"] = f"{r['given_name'] or ''} {r['surname'] or ''}".strip()
        p["doc_count"] = doc_count
        people.append(p)

    conn.close()
    return {
        "people": people,
        "total": total,
        "page": page,
        "pages": (total + per_page - 1) // per_page,
    }


def api_admin_stats():
    """GET /api/admin/stats — dashboard stats for admin."""
    conn = get_db()
    ensure_admin_tables(conn)

    total_people = conn.execute("SELECT COUNT(*) FROM person").fetchone()[0]
    total_docs = conn.execute("SELECT COUNT(*) FROM document").fetchone()[0]
    total_matches = conn.execute("SELECT COUNT(*) FROM document_match").fetchone()[0]
    verified = conn.execute("SELECT COUNT(*) FROM document_match WHERE verified = 1").fetchone()[0]
    total_notes = conn.execute("SELECT COUNT(*) FROM admin_note").fetchone()[0]
    total_edits = conn.execute("SELECT COUNT(*) FROM admin_edit").fetchone()[0]

    tiers = {}
    for r in conn.execute("SELECT confidence_tier, COUNT(*) FROM person GROUP BY confidence_tier"):
        tiers[r[0] or "unknown"] = r[1]

    recent_edits = conn.execute("""
        SELECT ae.person_id, ae.field, ae.old_value, ae.new_value, ae.edit_date,
               p.given_name, p.surname
        FROM admin_edit ae JOIN person p ON p.id = ae.person_id
        ORDER BY ae.edit_date DESC LIMIT 10
    """).fetchall()

    conn.close()
    return {
        "total_people": total_people,
        "total_documents": total_docs,
        "total_matches": total_matches,
        "verified_matches": verified,
        "total_notes": total_notes,
        "total_edits": total_edits,
        "tiers": tiers,
        "recent_edits": [dict(r) for r in recent_edits],
    }


def ensure_admin_tables(conn):
    """Create admin-specific tables if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS admin_note (
            id          INTEGER PRIMARY KEY,
            person_id   INTEGER REFERENCES person(id),
            note        TEXT,
            created_date TEXT,
            reviewer    TEXT DEFAULT 'Dad'
        );
        CREATE TABLE IF NOT EXISTS admin_edit (
            id          INTEGER PRIMARY KEY,
            person_id   INTEGER REFERENCES person(id),
            field       TEXT NOT NULL,
            old_value   TEXT,
            new_value   TEXT,
            edit_date   TEXT,
            reviewer    TEXT DEFAULT 'Dad'
        );
    """)


# ---------------------------------------------------------------------------
# HTTP Server
# ---------------------------------------------------------------------------

class ReviewHandler(SimpleHTTPRequestHandler):
    """Serves static files + API endpoints."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(SCRIPT_DIR), **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path == "/api/documents":
            self.json_response(api_documents(qs))
        elif re.match(r"^/api/documents/(\d+)$", path):
            doc_id = int(re.match(r"^/api/documents/(\d+)$", path).group(1))
            self.json_response(api_document_detail(doc_id))
        elif path == "/api/people":
            self.json_response(api_people_search(qs))
        elif path == "/api/engines":
            self.json_response(api_engines())
        elif path == "/api/stats":
            self.json_response(api_stats())
        elif re.match(r"^/api/documents/(\d+)/image$", path):
            doc_id = int(re.match(r"^/api/documents/(\d+)/image$", path).group(1))
            self.serve_document_image(doc_id)
            return
        # ── Admin API ──
        elif path == "/api/admin/people":
            self.json_response(api_admin_people_list(qs))
        elif path == "/api/admin/stats":
            self.json_response(api_admin_stats())
        elif re.match(r"^/api/admin/person/(\d+)$", path):
            pid = int(re.match(r"^/api/admin/person/(\d+)$", path).group(1))
            result = api_admin_person(pid)
            self.json_response(result if not isinstance(result, tuple) else result[0],
                             status=result[1] if isinstance(result, tuple) else 200)
        elif path.startswith("/raw-data/"):
            # Serve raw images
            super().do_GET()
        else:
            super().do_GET()

    def do_POST(self):
        path = urlparse(self.path).path
        content_type = self.headers.get("Content-Type", "")

        # Handle multipart upload (file upload)
        m = re.match(r"^/api/admin/person/(\d+)/upload$", path)
        if m and "multipart/form-data" in content_type:
            self._handle_upload(int(m.group(1)))
            return

        content_len = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(content_len)) if content_len else {}

        m = re.match(r"^/api/documents/(\d+)/ocr$", path)
        if m:
            result = api_run_ocr(int(m.group(1)), body)
            self.json_response(result if not isinstance(result, tuple) else result[0],
                             status=result[1] if isinstance(result, tuple) else 200)
            return

        m = re.match(r"^/api/documents/(\d+)/match$", path)
        if m:
            result = api_add_match(int(m.group(1)), body)
            self.json_response(result if not isinstance(result, tuple) else result[0],
                             status=result[1] if isinstance(result, tuple) else 200)
            return

        m = re.match(r"^/api/documents/(\d+)/correct$", path)
        if m:
            result = api_save_correction(int(m.group(1)), body)
            self.json_response(result if not isinstance(result, tuple) else result[0],
                             status=result[1] if isinstance(result, tuple) else 200)
            return

        m = re.match(r"^/api/documents/(\d+)/status$", path)
        if m:
            result = api_set_review_status(int(m.group(1)), body)
            self.json_response(result if not isinstance(result, tuple) else result[0],
                             status=result[1] if isinstance(result, tuple) else 200)
            return

        m = re.match(r"^/api/matches/(\d+)/verify$", path)
        if m:
            result = api_verify_match(int(m.group(1)), body)
            self.json_response(result if not isinstance(result, tuple) else result[0],
                             status=result[1] if isinstance(result, tuple) else 200)
            return

        if path == "/api/batch/status":
            self.json_response(api_batch_status(body))
            return

        # ── Admin POST routes ──
        m = re.match(r"^/api/admin/person/(\d+)$", path)
        if m:
            result = api_admin_update_person(int(m.group(1)), body)
            self.json_response(result if not isinstance(result, tuple) else result[0],
                             status=result[1] if isinstance(result, tuple) else 200)
            return

        m = re.match(r"^/api/admin/person/(\d+)/note$", path)
        if m:
            result = api_admin_add_note(int(m.group(1)), body)
            self.json_response(result if not isinstance(result, tuple) else result[0],
                             status=result[1] if isinstance(result, tuple) else 200)
            return

        m = re.match(r"^/api/admin/note/(\d+)/delete$", path)
        if m:
            result = api_admin_delete_note(int(m.group(1)))
            self.json_response(result if not isinstance(result, tuple) else result[0],
                             status=result[1] if isinstance(result, tuple) else 200)
            return

        self.send_error(404)

    def json_response(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def _handle_upload(self, person_id):
        """Parse multipart form data and upload file."""
        import cgi
        form = cgi.FieldStorage(
            fp=self.rfile, headers=self.headers,
            environ={"REQUEST_METHOD": "POST",
                     "CONTENT_TYPE": self.headers["Content-Type"]}
        )

        file_item = form["file"] if "file" in form else None
        if not file_item or not file_item.file:
            self.json_response({"error": "No file provided"}, status=400)
            return

        filename = file_item.filename or "upload.jpg"
        data = file_item.file.read()
        doc_type = form.getvalue("doc_type", "photo")
        description = form.getvalue("description", "")

        result = api_admin_upload(person_id, None, filename, doc_type, description, data)
        self.json_response(result if not isinstance(result, tuple) else result[0],
                         status=result[1] if isinstance(result, tuple) else 200)

    def serve_document_image(self, doc_id):
        """Serve the raw image file for a document by id."""
        conn = get_db()
        row = conn.execute("SELECT filepath FROM document WHERE id = ?", (doc_id,)).fetchone()
        conn.close()
        if not row or not row["filepath"] or not os.path.exists(row["filepath"]):
            self.send_error(404, "Image not found")
            return
        filepath = row["filepath"]
        ext = Path(filepath).suffix.lower()
        ct_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
                  ".gif": "image/gif", ".tif": "image/tiff", ".tiff": "image/tiff",
                  ".bmp": "image/bmp", ".pdf": "application/pdf"}
        ct = ct_map.get(ext, "application/octet-stream")
        with open(filepath, "rb") as f:
            data = f.read()
        self.send_response(200)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "public, max-age=3600")
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format, *args):
        if "/api/" in str(args[0]):
            print(f"  API: {args[0]}")


def main():
    port = 8080
    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--port" and i + 2 < len(sys.argv):
            port = int(sys.argv[i + 2])

    # Ensure tables exist
    conn = get_db()
    conn.close()

    server = HTTPServer(("0.0.0.0", port), ReviewHandler)
    print(f"Review server running at http://localhost:{port}")
    print(f"  API:     http://localhost:{port}/api/documents")
    print(f"  Review:  http://localhost:{port}/review.html")
    print(f"  Admin:   http://localhost:{port}/admin.html")
    print(f"  DB:      {DB_PATH}")
    print(f"  Press Ctrl+C to stop\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.server_close()


if __name__ == "__main__":
    main()
