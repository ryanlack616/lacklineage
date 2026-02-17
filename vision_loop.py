"""
Vision OCR continuous runner with auto-deploy.

Processes images through vision AI in 30-minute batches,
then exports JSON and pushes to GitHub Pages.

Usage:  python vision_loop.py
Stop:   Ctrl+C (gracefully finishes current image, exports, and pushes)
"""

import sqlite3
import subprocess
import sys
import os
import time
import signal
import json
import base64
import io
import shutil
from pathlib import Path
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────────────
SCRIPT_DIR  = Path(__file__).parent.resolve()
DB_PATH     = str(SCRIPT_DIR / "lineage.db")
DATA_DIR    = str(SCRIPT_DIR / "data")
BATCH_MINUTES = 30
OLLAMA_URL  = "http://127.0.0.1:11434"
VISION_MODEL = "minicpm-v:latest"
VISION_MAX_PX = 1800

VISION_PROMPT = """Analyze this genealogy document image. Extract ALL of the following information you can find:

1. PEOPLE: List every person name mentioned (first, middle, last). Format each as "PERSON: Firstname Lastname"
2. DATES: List every date found. Format as "DATE: YYYY-MM-DD description" (e.g. "DATE: 1923-05-14 birth")
3. PLACES: List every location mentioned. Format as "PLACE: City, State"
4. DOCUMENT TYPE: One of: photo, certificate, obituary, census, letter, newspaper, military, headstone, other
5. DESCRIPTION: One sentence describing the image."""

stop_requested = False

def handle_signal(sig, frame):
    global stop_requested
    print("\n\n  >>> Ctrl+C received — finishing current image, then export + push <<<")
    stop_requested = True

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGBREAK, handle_signal)  # Windows Ctrl+Break


# ── Vision AI call ──────────────────────────────────────────────────────────

def vision_analyze(filepath):
    """Send an image to vision model via Ollama. Returns analysis text."""
    import urllib.request
    from PIL import Image

    ext = Path(filepath).suffix.lower()
    if ext in (".pdf", ".doc", ".docx"):
        return ""

    try:
        img = Image.open(filepath)
        if img.mode not in ("L", "RGB"):
            img = img.convert("RGB")
        w, h = img.size
        if max(w, h) > VISION_MAX_PX:
            ratio = VISION_MAX_PX / max(w, h)
            img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)
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
    with urllib.request.urlopen(req, timeout=60) as resp:
        result = json.loads(resp.read().decode("utf-8"))
        return result.get("response", "").strip()


# ── Name matching (simplified from scan_documents.py) ───────────────────────

def load_people(conn):
    rows = conn.execute(
        "SELECT id, given_name, surname FROM person"
    ).fetchall()
    people = {}
    for pid, given, sur in rows:
        people[pid] = {
            "given": (given or "").strip(),
            "surname": (sur or "").strip(),
            "full": f"{(given or '').strip()} {(sur or '').strip()}".strip().lower(),
        }
    return people


def match_vision_to_people(vision_text, people):
    """Extract PERSON: lines and fuzzy match against DB people."""
    matches = []
    text_lower = vision_text.lower()

    for pid, p in people.items():
        if not p["surname"]:
            continue
        full = p["full"]
        surname = p["surname"].lower()
        given = p["given"].lower()

        # Full name match
        if full and len(full) > 3 and full in text_lower:
            matches.append((pid, 0.95, f"vision: {p['given']} {p['surname']}"))
            continue

        # Surname + first name initial
        if surname in text_lower and given and given[0] in text_lower:
            # Check for PERSON: lines containing the surname
            for line in vision_text.split("\n"):
                if "PERSON:" in line.upper() and surname in line.lower():
                    matches.append((pid, 0.80, f"vision: {line.strip()[:80]}"))
                    break

    return matches


# ── Export + Deploy ─────────────────────────────────────────────────────────

def export_and_push(batch_analyzed, batch_matches):
    """Export JSON data and push to GitHub."""
    print(f"\n{'─'*60}")
    print(f"  Exporting data and deploying...")

    # 1. Export documents.json via scan_documents.py
    subprocess.run(
        [sys.executable, str(SCRIPT_DIR / "scan_documents.py"), "--export-only"],
        cwd=str(SCRIPT_DIR),
        capture_output=True,
    )

    # 2. Export people/graph JSON via import_gedcom.py
    sys.path.insert(0, str(SCRIPT_DIR))
    from import_gedcom import export_json as export_people_json
    key_ids = {"george_id": 1, "ryan_id": 58, "total_sources": 0}
    export_people_json(DB_PATH, DATA_DIR, key_ids)

    # 3. Git commit + push
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    msg = f"auto: vision OCR +{batch_analyzed} docs, +{batch_matches} matches ({now})"
    subprocess.run(["git", "add", "-u"], cwd=str(SCRIPT_DIR), capture_output=True)
    subprocess.run(["git", "add", "data/"], cwd=str(SCRIPT_DIR), capture_output=True)
    result = subprocess.run(
        ["git", "commit", "-m", msg],
        cwd=str(SCRIPT_DIR), capture_output=True, text=True,
    )
    if "nothing to commit" in (result.stdout + result.stderr):
        print("  No changes to commit.")
    else:
        push = subprocess.run(
            ["git", "push"], cwd=str(SCRIPT_DIR),
            capture_output=True, text=True,
        )
        if push.returncode == 0:
            print(f"  Pushed: {msg}")
        else:
            print(f"  Push failed: {push.stderr[:200]}")

    print(f"{'─'*60}\n")


# ── Progress bar ────────────────────────────────────────────────────────────

def progress_bar(current, total, t0, batch_done, batch_match, width=40):
    pct = current / total if total else 1
    filled = int(width * pct)
    bar = "#" * filled + "-" * (width - filled)
    elapsed = time.time() - t0
    rate = batch_done / elapsed if elapsed > 0 and batch_done > 0 else 0
    remaining = (total - current) / rate if rate > 0 else 0
    if remaining >= 3600:
        eta = f"{remaining/3600:.1f}h"
    elif remaining >= 60:
        eta = f"{remaining/60:.0f}m{int(remaining%60):02d}s"
    else:
        eta = f"{remaining:.0f}s"
    mins_left = max(0, BATCH_MINUTES - elapsed / 60)
    cols = shutil.get_terminal_size((80, 20)).columns
    line = (f"\r  |{bar}| {pct:5.1%}  [{current}/{total}]  "
            f"AI:{batch_done} match:{batch_match}  ETA {eta}  "
            f"deploy in {mins_left:.0f}m")
    sys.stdout.write(line[:cols])
    sys.stdout.flush()


# ── Main loop ───────────────────────────────────────────────────────────────

def main():
    global stop_requested
    os.chdir(SCRIPT_DIR)

    print(f"{'='*60}")
    print(f"  VISION OCR CONTINUOUS RUNNER")
    print(f"  Model: {VISION_MODEL}")
    print(f"  Deploy every: {BATCH_MINUTES} minutes")
    print(f"  Press Ctrl+C to stop gracefully")
    print(f"{'='*60}")

    cycle = 0
    total_analyzed = 0
    total_matches = 0

    while not stop_requested:
        cycle += 1
        conn = sqlite3.connect(DB_PATH)
        people = load_people(conn)

        # Get remaining docs
        docs = conn.execute(
            "SELECT id, filepath, filename FROM document "
            "WHERE (vision_text IS NULL OR vision_text = '') "
            "ORDER BY id"
        ).fetchall()

        # Filter to processable images
        processable = []
        for doc_id, fp, fn in docs:
            ext = Path(fn).suffix.lower()
            if ext in (".pdf", ".doc", ".docx"):
                continue
            if os.path.exists(fp):
                processable.append((doc_id, fp, fn))

        if not processable:
            print("\n  All documents have been analyzed! Nothing left to do.")
            export_and_push(0, 0)
            break

        done_total = conn.execute(
            "SELECT COUNT(*) FROM document WHERE vision_text IS NOT NULL AND vision_text != ''"
        ).fetchone()[0]

        print(f"\n  Cycle {cycle}: {len(processable)} remaining "
              f"({done_total} already done, {len(people)} people)")

        batch_analyzed = 0
        batch_matches = 0
        batch_errors = 0
        t0 = time.time()

        for i, (doc_id, filepath, filename) in enumerate(processable):
            if stop_requested:
                break

            # Check time limit
            elapsed_min = (time.time() - t0) / 60
            if elapsed_min >= BATCH_MINUTES:
                print(f"\n  Batch time limit reached ({BATCH_MINUTES}m)")
                break

            progress_bar(i, len(processable), t0, batch_analyzed, batch_matches)

            try:
                text = vision_analyze(filepath)
            except Exception as e:
                batch_errors += 1
                err_msg = str(e)[:120]
                print(f"\n  ERROR on {filename}: {err_msg}")
                # If we get 3+ consecutive errors, Ollama is probably dead
                if batch_errors >= 3 and batch_analyzed == 0:
                    print(f"\n  !!! {batch_errors} consecutive errors with 0 successes — Ollama appears down")
                    print(f"  Attempting to restart Ollama...")
                    try:
                        subprocess.run(["taskkill", "/F", "/IM", "ollama.exe"],
                                       capture_output=True, timeout=10)
                        time.sleep(3)
                        subprocess.Popen(["ollama", "serve"],
                                         creationflags=0x00000008)  # DETACHED_PROCESS
                        time.sleep(15)  # Give it time to start
                        # Quick health check
                        import urllib.request as ur
                        ur.urlopen(f"{OLLAMA_URL}/api/tags", timeout=5)
                        print(f"  Ollama restarted successfully — continuing")
                        batch_errors = 0  # Reset error count
                    except Exception as restart_err:
                        print(f"  Ollama restart failed: {restart_err}")
                        print(f"  Stopping vision loop — fix Ollama and rerun")
                        conn.commit()
                        conn.close()
                        return
                continue

            if not text:
                continue

            # Save vision text
            conn.execute(
                "UPDATE document SET vision_text = ?, vision_date = ? WHERE id = ?",
                (text, datetime.now().isoformat(), doc_id)
            )
            batch_analyzed += 1

            # Match names
            matches = match_vision_to_people(text, people)
            for pid, conf, snippet in matches:
                existing = conn.execute(
                    "SELECT confidence FROM document_match "
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
                        "INSERT INTO document_match "
                        "(document_id, person_id, match_type, confidence, snippet, verified) "
                        "VALUES (?,?,?,?,?,0)",
                        (doc_id, pid, "vision_auto", round(conf, 3), snippet)
                    )
                    batch_matches += 1

            # Commit every 5 images
            if batch_analyzed % 5 == 0:
                conn.commit()

        # End of batch
        conn.commit()
        conn.close()

        total_analyzed += batch_analyzed
        total_matches += batch_matches

        # Clear progress line
        sys.stdout.write("\r" + " " * shutil.get_terminal_size((80, 20)).columns + "\r")
        elapsed = time.time() - t0
        print(f"  Batch done: {batch_analyzed} analyzed, {batch_matches} matches, "
              f"{batch_errors} errors ({elapsed:.0f}s)")
        print(f"  Running total: {total_analyzed} analyzed, {total_matches} matches")

        # Export and push
        export_and_push(batch_analyzed, batch_matches)

        if stop_requested:
            break

        # If batch was fast with 0 results, or all errors — stop
        if batch_analyzed == 0:
            if batch_errors > 0:
                print("  All-error batch detected. Stopping to avoid wasting time.")
            else:
                print("  No new images processed. Stopping.")
            break

    print(f"\n{'='*60}")
    print(f"  SESSION COMPLETE")
    print(f"  Total analyzed: {total_analyzed}")
    print(f"  Total matches:  {total_matches}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
