#!/usr/bin/env python3
# Parse a GEDCOM 5.5.1 file into lineage.db (SQLite) and export JSON files
# for the Lack Lineage static site.
#
# Usage:
#   python import_gedcom.py "C:\Users\PC\Desktop\Lack Family Tree.ged"

import json, os, re, sqlite3, sys
from collections import defaultdict, deque
from pathlib import Path

# ---------------------------------------------------------------------------
# 1.  GEDCOM PARSER
# ---------------------------------------------------------------------------

def parse_gedcom(path):
    """Return (individuals, families) dicts keyed by GEDCOM xref."""
    individuals = {}   # xref -> dict
    families = {}      # xref -> dict
    sources = {}       # xref -> dict (light, just for count)

    current = None
    current_type = None
    sub_tag = None      # e.g. BIRT, DEAT, MARR, NAME, RESI, EVEN

    with open(path, "r", encoding="utf-8-sig") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n\r")
            if not line:
                continue

            # Parse GEDCOM level / tag / value
            m = re.match(r"^(\d+)\s+(@\S+@\s+)?(.+)$", line)
            if not m:
                continue
            level = int(m.group(1))
            xref = m.group(2).strip() if m.group(2) else None
            rest = m.group(3)

            # rest might be "TAG value" or just "TAG"
            parts = rest.split(None, 1)
            tag = parts[0]
            value = parts[1] if len(parts) > 1 else ""

            # --- Level 0: record starts ---
            if level == 0:
                sub_tag = None
                if xref and tag == "INDI":
                    current_type = "INDI"
                    current = {
                        "xref": xref,
                        "given_name": "", "surname": "", "suffix": "",
                        "sex": None,
                        "birth_date": None, "birth_place": None,
                        "death_date": None, "death_place": None,
                        "famc": [],   # family xrefs where this person is a child
                        "fams": [],   # family xrefs where this person is a spouse
                    }
                    individuals[xref] = current
                elif xref and tag == "FAM":
                    current_type = "FAM"
                    current = {
                        "xref": xref,
                        "husb": None, "wife": None,
                        "children": [],
                        "marr_date": None, "marr_place": None,
                    }
                    families[xref] = current
                elif xref and tag == "SOUR":
                    current_type = "SOUR"
                    sources[xref] = {"xref": xref}
                    current = sources[xref]
                else:
                    current_type = None
                    current = None
                continue

            if current is None:
                continue

            # --- Level 1 tags ---
            if level == 1:
                sub_tag = tag
                if current_type == "INDI":
                    if tag == "SEX":
                        current["sex"] = value.strip() or None
                    elif tag == "FAMC":
                        current["famc"].append(value.strip())
                    elif tag == "FAMS":
                        current["fams"].append(value.strip())
                    elif tag in ("BIRT", "DEAT", "NAME", "RESI", "EVEN"):
                        pass  # handled at level 2
                elif current_type == "FAM":
                    if tag == "HUSB":
                        current["husb"] = value.strip()
                    elif tag == "WIFE":
                        current["wife"] = value.strip()
                    elif tag == "CHIL":
                        current["children"].append(value.strip())
                    elif tag == "MARR":
                        pass  # sub-details at level 2
                continue

            # --- Level 2 tags ---
            if level == 2 and sub_tag:
                if current_type == "INDI":
                    if sub_tag == "NAME":
                        if tag == "GIVN":
                            current["given_name"] = value.strip()
                        elif tag == "SURN":
                            current["surname"] = value.strip()
                        elif tag == "NSFX":
                            current["suffix"] = value.strip()
                    elif sub_tag == "BIRT":
                        if tag == "DATE":
                            current["birth_date"] = value.strip()
                        elif tag == "PLAC":
                            current["birth_place"] = value.strip()
                    elif sub_tag == "DEAT":
                        if tag == "DATE":
                            current["death_date"] = value.strip()
                        elif tag == "PLAC":
                            current["death_place"] = value.strip()
                elif current_type == "FAM":
                    if sub_tag == "MARR":
                        if tag == "DATE":
                            if current["marr_date"] is None:
                                current["marr_date"] = value.strip()
                        elif tag == "PLAC":
                            if current["marr_place"] is None:
                                current["marr_place"] = value.strip()

    return individuals, families, sources


# ---------------------------------------------------------------------------
# 2.  NORMALISE DATES  (best-effort → ISO-ish string or raw)
# ---------------------------------------------------------------------------

MONTHS = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}

def normalise_date(raw):
    """Best-effort normalise a GEDCOM date to ISO-ish or a cleaned string."""
    if not raw:
        return None
    raw = raw.strip()

    # Strip modifiers: ABT, BEF, AFT, CAL, EST, FROM, TO, BET ... AND ...
    cleaned = re.sub(
        r"^(ABT\.?|ABT|BEF\.?|BEF|AFT\.?|AFT|CAL|EST|FROM|TO|INT|BET)\s+",
        "", raw, flags=re.IGNORECASE
    ).strip()
    # Strip "AND ..." from BET...AND
    cleaned = re.sub(r"\s+AND\s+.*$", "", cleaned, flags=re.IGNORECASE).strip()

    # Try "DD Mon YYYY"
    m = re.match(r"^(\d{1,2})\s+(\w{3})\s+(\d{4})$", cleaned)
    if m:
        day, mon, year = m.groups()
        mo = MONTHS.get(mon.upper())
        if mo:
            return f"{year}-{mo}-{day.zfill(2)}"

    # Try "Mon YYYY"
    m = re.match(r"^(\w{3})\s+(\d{4})$", cleaned)
    if m:
        mon, year = m.groups()
        mo = MONTHS.get(mon.upper())
        if mo:
            return f"{year}-{mo}"

    # Try bare year "YYYY"
    m = re.match(r"^(\d{4})$", cleaned)
    if m:
        return m.group(1)

    # Try "O8 11 1949" style (typo for 08)
    m = re.match(r"^O?(\d{1,2})\s+(\d{1,2})\s+(\d{4})$", cleaned)
    if m:
        p1, p2, year = m.groups()
        # Ambiguous — assume MM DD YYYY
        return f"{year}-{p1.zfill(2)}-{p2.zfill(2)}"

    # Try "DD MM YYYY" all-numeric
    m = re.match(r"^(\d{1,2})\s+(\d{1,2})\s+(\d{4})$", cleaned)
    if m:
        d, mo, y = m.groups()
        return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"

    # Try MM/DD/YYYY or M/D/YYYY
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", cleaned)
    if m:
        mo, d, y = m.groups()
        return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"

    # Return raw if nothing matched (but not empty)
    return raw if raw else None


def extract_year(date_str):
    """Extract a 4-digit year from a date string, or None."""
    if not date_str:
        return None
    m = re.search(r"(\d{4})", str(date_str))
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# 3.  BUILD DATABASE
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS person (
    id          INTEGER PRIMARY KEY,
    xref        TEXT UNIQUE,
    given_name  TEXT,
    surname     TEXT,
    suffix      TEXT,
    sex         TEXT,
    birth_date  TEXT,
    birth_place TEXT,
    death_date  TEXT,
    death_place TEXT
);

CREATE TABLE IF NOT EXISTS family (
    id          INTEGER PRIMARY KEY,
    xref        TEXT UNIQUE,
    husb_id     INTEGER REFERENCES person(id),
    wife_id     INTEGER REFERENCES person(id),
    marr_date   TEXT,
    marr_place  TEXT
);

CREATE TABLE IF NOT EXISTS family_child (
    family_id   INTEGER REFERENCES family(id),
    child_id    INTEGER REFERENCES person(id),
    PRIMARY KEY (family_id, child_id)
);

CREATE TABLE IF NOT EXISTS relationship (
    id          INTEGER PRIMARY KEY,
    person1_id  INTEGER REFERENCES person(id),
    person2_id  INTEGER REFERENCES person(id),
    rel_type    TEXT   -- 'parent_child' or 'spouse'
);

CREATE INDEX IF NOT EXISTS idx_rel_p1 ON relationship(person1_id);
CREATE INDEX IF NOT EXISTS idx_rel_p2 ON relationship(person2_id);
CREATE INDEX IF NOT EXISTS idx_person_surname ON person(surname);
"""


def build_db(db_path, individuals, families, sources):
    """Create/reset lineage.db and populate it from parsed GEDCOM data."""
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)

    # --- Map GEDCOM xrefs to sequential integer IDs ---
    xref_to_id = {}
    pid = 0
    for xref, indi in individuals.items():
        pid += 1
        xref_to_id[xref] = pid
        conn.execute(
            "INSERT INTO person (id, xref, given_name, surname, suffix, sex, "
            "birth_date, birth_place, death_date, death_place) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                pid, xref,
                indi["given_name"] or None,
                indi["surname"] or None,
                indi["suffix"] or None,
                indi["sex"],
                normalise_date(indi["birth_date"]),
                indi["birth_place"],
                normalise_date(indi["death_date"]),
                indi["death_place"],
            ),
        )

    fid = 0
    relationships = []
    for xref, fam in families.items():
        fid += 1
        husb_id = xref_to_id.get(fam["husb"])
        wife_id = xref_to_id.get(fam["wife"])

        conn.execute(
            "INSERT INTO family (id, xref, husb_id, wife_id, marr_date, marr_place) "
            "VALUES (?,?,?,?,?,?)",
            (fid, xref, husb_id, wife_id,
             normalise_date(fam["marr_date"]), fam["marr_place"]),
        )

        # Spouse relationship
        if husb_id and wife_id:
            relationships.append((husb_id, wife_id, "spouse"))

        # Parent-child relationships
        for ch_xref in fam["children"]:
            ch_id = xref_to_id.get(ch_xref)
            if ch_id is None:
                continue
            conn.execute(
                "INSERT OR IGNORE INTO family_child (family_id, child_id) VALUES (?,?)",
                (fid, ch_id),
            )
            if husb_id:
                relationships.append((husb_id, ch_id, "parent_child"))
            if wife_id:
                relationships.append((wife_id, ch_id, "parent_child"))

    for i, (p1, p2, rt) in enumerate(relationships, 1):
        conn.execute(
            "INSERT INTO relationship (id, person1_id, person2_id, rel_type) VALUES (?,?,?,?)",
            (i, p1, p2, rt),
        )

    conn.commit()

    # Report key people
    george = conn.execute(
        "SELECT id, given_name, surname FROM person WHERE given_name LIKE '%George%' AND surname='Lack' AND birth_date LIKE '%1949%'"
    ).fetchone()
    ryan = conn.execute(
        "SELECT id, given_name, surname FROM person WHERE given_name LIKE '%Ryan%' AND surname='Lack'"
    ).fetchone()

    stats = {
        "total_people": conn.execute("SELECT COUNT(*) FROM person").fetchone()[0],
        "total_relationships": conn.execute("SELECT COUNT(*) FROM relationship").fetchone()[0],
        "total_families": conn.execute("SELECT COUNT(*) FROM family").fetchone()[0],
        "total_sources": len(sources),
        "george_id": george[0] if george else None,
        "george_name": f"{george[1]} {george[2]}" if george else None,
        "ryan_id": ryan[0] if ryan else None,
        "ryan_name": f"{ryan[1]} {ryan[2]}" if ryan else None,
    }

    conn.close()
    return stats, xref_to_id


# ---------------------------------------------------------------------------
# 4.  EXPORT JSON FILES
# ---------------------------------------------------------------------------

def export_json(db_path, out_dir, key_ids):
    """Export all JSON data files from the database."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    os.makedirs(out_dir, exist_ok=True)

    george_id = key_ids.get("george_id")
    ryan_id = key_ids.get("ryan_id")

    # --- people.json ---
    rows = conn.execute(
        "SELECT id, given_name, surname, sex, birth_date, birth_place, death_date, death_place "
        "FROM person ORDER BY id"
    ).fetchall()
    people = []
    for r in rows:
        people.append({
            "id": r["id"],
            "given_name": r["given_name"],
            "surname": r["surname"],
            "sex": r["sex"],
            "birth_date": r["birth_date"],
            "birth_place": r["birth_place"],
            "death_date": r["death_date"],
            "death_place": r["death_place"],
        })
    write_json(os.path.join(out_dir, "people.json"), people)
    print(f"  people.json: {len(people)} records")

    # --- Build graph structures ---
    nodes_all = []
    node_ids = set()
    for r in rows:
        birth_year = extract_year(r["birth_date"])
        name_parts = []
        if r["given_name"]:
            name_parts.append(r["given_name"])
        if r["surname"]:
            name_parts.append(r["surname"])
        name = " ".join(name_parts) if name_parts else f"Person {r['id']}"

        nodes_all.append({
            "id": r["id"],
            "name": name,
            "sex": r["sex"],
            "birth_year": birth_year,
            "birth_place": r["birth_place"],
            "death_date": r["death_date"],
            "surname": r["surname"],
        })
        node_ids.add(r["id"])

    rels = conn.execute(
        "SELECT person1_id, person2_id, rel_type FROM relationship"
    ).fetchall()
    links_all = []
    for r in rels:
        links_all.append({
            "source": r["person1_id"],
            "target": r["person2_id"],
            "type": r["rel_type"],
        })

    # --- graph-all.json (every person) ---
    write_json(os.path.join(out_dir, "graph-all.json"),
               {"nodes": nodes_all, "links": links_all})
    print(f"  graph-all.json: {len(nodes_all)} nodes, {len(links_all)} links")

    # --- graph.json (only people involved in at least one relationship) ---
    connected_ids = set()
    for link in links_all:
        connected_ids.add(link["source"])
        connected_ids.add(link["target"])
    nodes_connected = [n for n in nodes_all if n["id"] in connected_ids]
    write_json(os.path.join(out_dir, "graph.json"),
               {"nodes": nodes_connected, "links": links_all})
    print(f"  graph.json: {len(nodes_connected)} nodes, {len(links_all)} links")

    # --- graph-ryan.json (BFS from Ryan, depth ~3 hops) ---
    if ryan_id:
        adj = defaultdict(set)
        link_set = set()
        for link in links_all:
            adj[link["source"]].add(link["target"])
            adj[link["target"]].add(link["source"])
            link_set.add((link["source"], link["target"], link["type"]))

        visited = set()
        queue = deque([(ryan_id, 0)])
        visited.add(ryan_id)
        while queue:
            nid, depth = queue.popleft()
            if depth >= 3:
                continue
            for nb in adj[nid]:
                if nb not in visited:
                    visited.add(nb)
                    queue.append((nb, depth + 1))

        ryan_nodes = [n for n in nodes_all if n["id"] in visited]
        ryan_links = [l for l in links_all
                      if l["source"] in visited and l["target"] in visited]
        write_json(os.path.join(out_dir, "graph-ryan.json"),
                   {"nodes": ryan_nodes, "links": ryan_links})
        print(f"  graph-ryan.json: {len(ryan_nodes)} nodes, {len(ryan_links)} links")
    else:
        # fallback: copy graph.json
        write_json(os.path.join(out_dir, "graph-ryan.json"),
                   {"nodes": nodes_connected[:50], "links": []})
        print("  graph-ryan.json: fallback (no Ryan found)")

    # --- stats.json ---
    unique_surnames = conn.execute(
        "SELECT COUNT(DISTINCT surname) FROM person WHERE surname IS NOT NULL"
    ).fetchone()[0]
    earliest = conn.execute(
        "SELECT birth_date FROM person WHERE birth_date IS NOT NULL "
        "ORDER BY CAST(SUBSTR(birth_date,1,4) AS INTEGER) ASC LIMIT 1"
    ).fetchone()
    latest = conn.execute(
        "SELECT birth_date FROM person WHERE birth_date IS NOT NULL "
        "ORDER BY CAST(SUBSTR(birth_date,1,4) AS INTEGER) DESC LIMIT 1"
    ).fetchone()
    # Count events (marriages)
    total_events = conn.execute(
        "SELECT COUNT(*) FROM family WHERE marr_date IS NOT NULL"
    ).fetchone()[0]
    with_rels = len(connected_ids)

    stats_obj = {
        "total_people": len(people),
        "total_relationships": len(links_all),
        "total_events": total_events,
        "total_sources": key_ids.get("total_sources", 0),
        "unique_surnames": unique_surnames,
        "earliest_birth": earliest[0] if earliest else None,
        "latest_birth": latest[0] if latest else None,
        "with_relationships": with_rels,
    }
    write_json(os.path.join(out_dir, "stats.json"), stats_obj)
    print(f"  stats.json: {stats_obj}")

    # --- places.json ---
    places = conn.execute(
        "SELECT birth_place AS place, COUNT(*) AS count FROM person "
        "WHERE birth_place IS NOT NULL GROUP BY birth_place "
        "ORDER BY count DESC"
    ).fetchall()
    write_json(os.path.join(out_dir, "places.json"),
               [{"place": r["place"], "count": r["count"]} for r in places])
    print(f"  places.json: {len(places)} places")

    # --- surnames.json ---
    surnames = conn.execute(
        "SELECT surname, COUNT(*) AS count FROM person "
        "WHERE surname IS NOT NULL GROUP BY surname "
        "ORDER BY count DESC"
    ).fetchall()
    write_json(os.path.join(out_dir, "surnames.json"),
               [{"surname": r["surname"], "count": r["count"]} for r in surnames])
    print(f"  surnames.json: {len(surnames)} surnames")

    # --- timeline.json (people sorted by birth year) ---
    timeline = conn.execute(
        "SELECT id, given_name, surname, birth_date, death_date, birth_place, sex "
        "FROM person WHERE birth_date IS NOT NULL "
        "ORDER BY CAST(SUBSTR(birth_date,1,4) AS INTEGER) ASC"
    ).fetchall()
    write_json(os.path.join(out_dir, "timeline.json"),
               [{
                   "id": r["id"],
                   "given_name": r["given_name"],
                   "surname": r["surname"],
                   "birth_date": r["birth_date"],
                   "death_date": r["death_date"],
                   "birth_place": r["birth_place"],
                   "sex": r["sex"],
               } for r in timeline])
    print(f"  timeline.json: {len(timeline)} entries")

    conn.close()
    return george_id, ryan_id


def write_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))


# ---------------------------------------------------------------------------
# 5.  MAIN
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print("Usage: python import_gedcom.py <path-to-ged>")
        sys.exit(1)

    ged_path = sys.argv[1]
    script_dir = Path(__file__).parent
    db_path = str(Path(r"C:\Users\PC\monospacepoetry\poems\lineage.db"))
    out_dir = str(script_dir / "data")

    print(f"Parsing GEDCOM: {ged_path}")
    individuals, families, sources = parse_gedcom(ged_path)
    print(f"  Found {len(individuals)} individuals, {len(families)} families, {len(sources)} sources")

    print(f"\nBuilding database: {db_path}")
    stats, xref_to_id = build_db(db_path, individuals, families, sources)
    print(f"  {stats['total_people']} people, {stats['total_relationships']} relationships, {stats['total_families']} families")
    print(f"  George l Lack → id {stats['george_id']}  ({stats['george_name']})")
    print(f"  Ryan Lack     → id {stats['ryan_id']}  ({stats['ryan_name']})")

    print(f"\nExporting JSON to: {out_dir}")
    key_ids = {
        "george_id": stats["george_id"],
        "ryan_id": stats["ryan_id"],
        "total_sources": len(sources),
    }
    george_id, ryan_id = export_json(db_path, out_dir, key_ids)

    print(f"\n{'='*60}")
    print(f"DONE!")
    print(f"  Database: {db_path} ({os.path.getsize(db_path):,} bytes)")
    print(f"  JSON dir: {out_dir}")
    print(f"\n  KEY IDS (update tree.html / ancestry.html):")
    print(f"    George l Lack  DEFAULT_ROOT = {george_id}")
    print(f"    Ryan Lack      id = {ryan_id}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
