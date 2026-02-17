"""
Generate research.json — actionable research leads for the genealogy database.

Produces:
  data/research.json — structured findings for the Research Hub page.

Run:  python generate_research.py
"""
import sqlite3, json, re, os
from collections import defaultdict

DB = 'lineage.db'
OUT = os.path.join('data', 'research.json')

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

def yr(s):
    if not s: return None
    m = re.search(r'(\d{4})', s)
    return int(m.group(1)) if m else None

def person_stub(row):
    return {
        'id': row['id'],
        'name': f"{row['given_name'] or ''} {row['surname'] or ''}".strip(),
        'birth_date': row['birth_date'],
        'death_date': row['death_date'] if 'death_date' in row.keys() else None,
        'birth_place': row['birth_place'] if 'birth_place' in row.keys() else None,
        'confidence_tier': row['confidence_tier'] if 'confidence_tier' in row.keys() else None,
    }

# ── 1. Summary Stats ──
total = conn.execute('SELECT COUNT(*) FROM person').fetchone()[0]
stats = {
    'total_people': total,
    'no_birth_date': conn.execute("SELECT COUNT(*) FROM person WHERE birth_date IS NULL OR birth_date = ''").fetchone()[0],
    'no_death_date': conn.execute("SELECT COUNT(*) FROM person WHERE death_date IS NULL OR death_date = ''").fetchone()[0],
    'no_birth_place': conn.execute("SELECT COUNT(*) FROM person WHERE birth_place IS NULL OR birth_place = ''").fetchone()[0],
    'no_death_place': conn.execute("SELECT COUNT(*) FROM person WHERE death_place IS NULL OR death_place = ''").fetchone()[0],
    'no_documents': conn.execute("SELECT COUNT(*) FROM person WHERE id NOT IN (SELECT DISTINCT person_id FROM document_match)").fetchone()[0],
    'no_parents': conn.execute("SELECT COUNT(*) FROM person WHERE id NOT IN (SELECT child_id FROM family_child)").fetchone()[0],
    'no_spouse': conn.execute("""SELECT COUNT(*) FROM person WHERE id NOT IN (
        SELECT person1_id FROM relationship WHERE rel_type='spouse'
        UNION SELECT person2_id FROM relationship WHERE rel_type='spouse')""").fetchone()[0],
    'single_source': conn.execute("SELECT COUNT(*) FROM person WHERE source_count = 1").fetchone()[0],
    'zero_sources': conn.execute("SELECT COUNT(*) FROM person WHERE source_count = 0 OR source_count IS NULL").fetchone()[0],
    'unverified_matches': conn.execute("SELECT COUNT(*) FROM document_match WHERE verified = 0 OR verified IS NULL").fetchone()[0],
    'unmatched_docs': conn.execute("SELECT COUNT(*) FROM document WHERE id NOT IN (SELECT DISTINCT document_id FROM document_match)").fetchone()[0],
    'total_docs': conn.execute('SELECT COUNT(*) FROM document').fetchone()[0],
    'pending_review': conn.execute("SELECT COUNT(*) FROM document WHERE review_status = 'pending' OR review_status IS NULL").fetchone()[0],
}

# ── 2. Potential Duplicates (Soundex) ──
def soundex(name):
    name = name.upper()
    if not name: return ''
    codes = {'B':'1','F':'1','P':'1','V':'1',
             'C':'2','G':'2','J':'2','K':'2','Q':'2','S':'2','X':'2','Z':'2',
             'D':'3','T':'3','L':'4','M':'5','N':'5','R':'6'}
    result = name[0]
    prev = codes.get(name[0],'')
    for c in name[1:]:
        code = codes.get(c,'')
        if code and code != prev:
            result += code
        prev = code if code else prev
    return (result + '000')[:4]

people = conn.execute('''
    SELECT id, given_name, surname, birth_date, death_date, birth_place, confidence_tier
    FROM person WHERE surname IS NOT NULL AND given_name IS NOT NULL
''').fetchall()

sx_groups = defaultdict(list)
for p in people:
    sx = soundex(p['surname']) + '_' + soundex(p['given_name'])
    sx_groups[sx].append(p)

duplicates = []
seen_pairs = set()
for sx, members in sx_groups.items():
    if len(members) < 2: continue
    for i, p1 in enumerate(members):
        for p2 in members[i+1:]:
            if p1['surname'].lower() == p2['surname'].lower() and p1['given_name'].lower() == p2['given_name'].lower():
                continue  # exact match, skip
            # Check if birth dates are within 5 years or both unknown
            y1, y2 = yr(p1['birth_date']), yr(p2['birth_date'])
            date_close = (y1 and y2 and abs(y1-y2) <= 5) or (not y1 or not y2)
            if not date_close: continue
            key = tuple(sorted([p1['id'], p2['id']]))
            if key in seen_pairs: continue
            seen_pairs.add(key)
            # Confidence score for the match
            score = 0
            if y1 and y2 and y1 == y2: score += 3
            elif y1 and y2 and abs(y1-y2) <= 2: score += 2
            if soundex(p1['surname']) == soundex(p2['surname']): score += 2
            if soundex(p1['given_name']) == soundex(p2['given_name']): score += 2
            bp1 = (p1['birth_place'] or '').lower()
            bp2 = (p2['birth_place'] or '').lower()
            if bp1 and bp2 and (bp1 in bp2 or bp2 in bp1): score += 2
            duplicates.append({
                'person1': person_stub(p1),
                'person2': person_stub(p2),
                'score': score,
                'reason': f"Soundex: {p1['surname']}/{p2['surname']}, {p1['given_name']}/{p2['given_name']}"
            })

duplicates.sort(key=lambda x: -x['score'])
duplicates = duplicates[:100]

# ── 3. Surname Variants ──
rows = conn.execute('''
    SELECT surname, COUNT(*) as cnt FROM person
    WHERE surname IS NOT NULL AND surname != ''
    GROUP BY surname ORDER BY surname
''').fetchall()

sgroups = defaultdict(list)
for name, cnt in rows:
    sx = soundex(name)
    sgroups[sx].append({'name': name, 'count': cnt})

surname_variants = []
for sx, variants in sorted(sgroups.items()):
    # Only groups with truly different spellings
    unique_lower = set(v['name'].lower().split()[0] for v in variants)
    if len(unique_lower) > 1 and sum(v['count'] for v in variants) >= 3:
        surname_variants.append({
            'soundex': sx,
            'variants': sorted(variants, key=lambda v: -v['count']),
            'total': sum(v['count'] for v in variants)
        })
surname_variants.sort(key=lambda x: -x['total'])

# ── 4. Data Anomalies (impossible dates) ──
anomalies = []
gaps = conn.execute('''
    SELECT p.id as pid, p.given_name, p.surname, p.birth_date, p.confidence_tier,
           c.id as cid, c.given_name as cgn, c.surname as csn, c.birth_date as cbd, c.confidence_tier as ctier
    FROM family f
    JOIN family_child fc ON f.id = fc.family_id
    JOIN person p ON (f.husb_id = p.id OR f.wife_id = p.id)
    JOIN person c ON fc.child_id = c.id
    WHERE p.birth_date IS NOT NULL AND p.birth_date != ''
    AND c.birth_date IS NOT NULL AND c.birth_date != ''
''').fetchall()

for g in gaps:
    py, cy = yr(g['birth_date']), yr(g['cbd'])
    if py and cy:
        gap = cy - py
        if gap > 55 or gap < 12:
            anomalies.append({
                'type': 'age_gap',
                'severity': 'high' if gap < 0 or gap > 70 else 'medium',
                'parent': {'id': g['pid'], 'name': f"{g['given_name']} {g['surname']}", 'birth_date': g['birth_date'], 'confidence_tier': g['confidence_tier']},
                'child': {'id': g['cid'], 'name': f"{g['cgn']} {g['csn']}", 'birth_date': g['cbd'], 'confidence_tier': g['ctier']},
                'gap_years': gap,
                'description': f"Parent age at birth: {gap} years"
            })

# Born after death
bad_dates = conn.execute('''
    SELECT id, given_name, surname, birth_date, death_date, confidence_tier
    FROM person
    WHERE birth_date IS NOT NULL AND death_date IS NOT NULL
    AND birth_date != '' AND death_date != ''
''').fetchall()
for p in bad_dates:
    by, dy = yr(p['birth_date']), yr(p['death_date'])
    if by and dy and dy < by:
        anomalies.append({
            'type': 'death_before_birth',
            'severity': 'high',
            'person': person_stub(p),
            'description': f"Death ({p['death_date']}) before birth ({p['birth_date']})"
        })
    elif by and dy and (dy - by) > 120:
        anomalies.append({
            'type': 'impossible_age',
            'severity': 'medium',
            'person': person_stub(p),
            'description': f"Lived {dy-by} years ({p['birth_date']} – {p['death_date']})"
        })

anomalies.sort(key=lambda x: 0 if x['severity']=='high' else 1)

# ── 5. Missing Data (people needing research) ──
missing_data = []
rows = conn.execute('''
    SELECT p.id, p.given_name, p.surname, p.birth_date, p.birth_place,
           p.death_date, p.death_place, p.confidence_tier, p.source_count,
           (SELECT COUNT(*) FROM document_match dm WHERE dm.person_id = p.id) as doc_count,
           (SELECT COUNT(*) FROM family_child fc WHERE fc.child_id = p.id) as has_parents,
           (SELECT COUNT(*) FROM relationship r WHERE (r.person1_id = p.id OR r.person2_id = p.id) AND r.rel_type='spouse') as has_spouse
    FROM person p
    ORDER BY p.confidence, p.surname, p.given_name
''').fetchall()

for p in rows:
    gaps = []
    if not p['birth_date']: gaps.append('birth_date')
    if not p['death_date']: gaps.append('death_date')
    if not p['birth_place']: gaps.append('birth_place')
    if not p['death_place']: gaps.append('death_place')
    if p['doc_count'] == 0: gaps.append('no_documents')
    if p['has_parents'] == 0: gaps.append('no_parents')
    if p['has_spouse'] == 0: gaps.append('no_spouse')
    if (p['source_count'] or 0) <= 1: gaps.append('single_source')
    if gaps:
        missing_data.append({
            'id': p['id'],
            'name': f"{p['given_name'] or ''} {p['surname'] or ''}".strip(),
            'birth_date': p['birth_date'],
            'birth_place': p['birth_place'],
            'death_date': p['death_date'],
            'confidence_tier': p['confidence_tier'],
            'doc_count': p['doc_count'],
            'gaps': gaps,
            'gap_count': len(gaps),
        })
missing_data.sort(key=lambda x: -x['gap_count'])

# ── 6. Unmatched Documents with OCR text ──
unmatched_docs = []
rows = conn.execute('''
    SELECT d.id, d.filename, d.filepath, d.doc_type,
           SUBSTR(COALESCE(d.vision_text, d.ocr_text, ''), 1, 300) as text_preview,
           CASE WHEN d.vision_text IS NOT NULL AND d.vision_text != '' THEN 'vision'
                WHEN d.ocr_text IS NOT NULL AND d.ocr_text != '' THEN 'ocr'
                ELSE 'none' END as text_source
    FROM document d
    WHERE d.id NOT IN (SELECT DISTINCT document_id FROM document_match)
    AND (d.ocr_text IS NOT NULL AND d.ocr_text != ''
         OR d.vision_text IS NOT NULL AND d.vision_text != '')
    ORDER BY d.doc_type, d.filename
''').fetchall()
for d in rows:
    unmatched_docs.append({
        'id': d['id'],
        'filename': d['filename'],
        'doc_type': d['doc_type'],
        'text_preview': d['text_preview'],
        'text_source': d['text_source'],
    })

# ── 7. Census & Obituary Candidates ──
census_candidates = conn.execute('''
    SELECT p.id, p.given_name, p.surname, p.birth_date, p.birth_place,
           p.death_date, p.confidence_tier
    FROM person p
    WHERE p.birth_date IS NOT NULL AND p.birth_date != ''
    AND p.birth_place IS NOT NULL AND p.birth_place != ''
    AND p.id NOT IN (
        SELECT dm.person_id FROM document_match dm
        JOIN document d ON dm.document_id = d.id
        WHERE d.doc_type = 'census'
    )
    ORDER BY p.surname, p.given_name
''').fetchall()
census_list = [person_stub(r) for r in census_candidates]

obit_candidates = conn.execute('''
    SELECT p.id, p.given_name, p.surname, p.birth_date, p.death_date,
           p.birth_place, p.death_place as birth_place, p.confidence_tier
    FROM person p
    WHERE p.death_date IS NOT NULL AND p.death_date != ''
    AND p.id NOT IN (
        SELECT dm.person_id FROM document_match dm
        JOIN document d ON dm.document_id = d.id
        WHERE d.doc_type = 'obituary'
    )
    ORDER BY p.surname, p.given_name
''').fetchall()
obit_list = [person_stub(r) for r in obit_candidates]

# ── 8. Migration Patterns ──
migrations = conn.execute('''
    SELECT birth_place, death_place, COUNT(*) as cnt
    FROM person
    WHERE birth_place IS NOT NULL AND birth_place != ''
    AND death_place IS NOT NULL AND death_place != ''
    AND LOWER(birth_place) != LOWER(death_place)
    GROUP BY LOWER(birth_place), LOWER(death_place)
    HAVING cnt >= 2
    ORDER BY cnt DESC
    LIMIT 30
''').fetchall()
migration_list = [{'from': m['birth_place'], 'to': m['death_place'], 'count': m['cnt']} for m in migrations]

# ── 9. Research Priorities (ranked) ──
priorities = [
    {
        'rank': 1,
        'title': 'Verify Document Matches',
        'count': stats['unverified_matches'],
        'description': f"All {stats['unverified_matches']:,} document-to-person matches are unverified. Review and confirm these connections.",
        'action': 'review',
        'impact': 'high',
    },
    {
        'rank': 2,
        'title': 'Match Unlinked Documents',
        'count': len(unmatched_docs),
        'description': f"{len(unmatched_docs)} documents with readable text aren't connected to anyone. These may contain names, dates, or places for existing or new people.",
        'action': 'documents',
        'impact': 'high',
    },
    {
        'rank': 3,
        'title': 'Resolve Potential Duplicates',
        'count': len(duplicates),
        'description': f"{len(duplicates)} pairs of people have similar names and dates — they may be the same person entered twice.",
        'action': 'duplicates',
        'impact': 'high',
    },
    {
        'rank': 4,
        'title': 'Find Census Records',
        'count': len(census_list),
        'description': f"{len(census_list):,} people have dates and places but no census record. US Census records (1790-1950) could fill major gaps.",
        'action': 'census',
        'impact': 'medium',
    },
    {
        'rank': 5,
        'title': 'Find Missing Parents',
        'count': stats['no_parents'],
        'description': f"{stats['no_parents']:,} people ({stats['no_parents']*100//total}%) have no parents linked. Extending these lines backward is the core of genealogy.",
        'action': 'missing',
        'impact': 'medium',
    },
    {
        'rank': 6,
        'title': 'Find Obituaries',
        'count': len(obit_list),
        'description': f"{len(obit_list):,} people with death dates have no obituary. Obituaries often name parents, children, and siblings.",
        'action': 'obituaries',
        'impact': 'medium',
    },
    {
        'rank': 7,
        'title': 'Fix Data Anomalies',
        'count': len(anomalies),
        'description': f"{len(anomalies)} records have suspicious dates — impossible parent ages, deaths before births, or people living 120+ years.",
        'action': 'anomalies',
        'impact': 'medium',
    },
    {
        'rank': 8,
        'title': 'Consolidate Surname Spellings',
        'count': len(surname_variants),
        'description': f"{len(surname_variants)} surname groups have multiple spellings (Fry/Frye, Dunkleberger/Dunkelberger, etc.). Standardizing improves search.",
        'action': 'surnames',
        'impact': 'low',
    },
    {
        'rank': 9,
        'title': 'Add Missing Documents',
        'count': stats['no_documents'],
        'description': f"{stats['no_documents']:,} people ({stats['no_documents']*100//total}%) have no documents at all. Photos, certificates, or records would strengthen their profiles.",
        'action': 'missing',
        'impact': 'low',
    },
]

# ── Assemble output ──
research = {
    'generated': __import__('datetime').datetime.now().isoformat(),
    'stats': stats,
    'priorities': priorities,
    'duplicates': duplicates,
    'surname_variants': surname_variants[:50],
    'anomalies': anomalies,
    'missing_data': missing_data,
    'unmatched_docs': unmatched_docs,
    'census_candidates': census_list,
    'obituary_candidates': obit_list,
    'migrations': migration_list,
}

os.makedirs('data', exist_ok=True)
with open(OUT, 'w', encoding='utf-8') as f:
    json.dump(research, f, indent=1, ensure_ascii=False)

conn.close()

# Summary
print(f'✓ Generated {OUT}')
print(f'  Priorities:      {len(priorities)}')
print(f'  Duplicates:      {len(duplicates)}')
print(f'  Surname groups:  {len(surname_variants)}')
print(f'  Anomalies:       {len(anomalies)}')
print(f'  Missing data:    {len(missing_data)}')
print(f'  Unmatched docs:  {len(unmatched_docs)}')
print(f'  Census cands:    {len(census_list)}')
print(f'  Obituary cands:  {len(obit_list)}')
print(f'  Migrations:      {len(migration_list)}')
