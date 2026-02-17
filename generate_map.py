"""
generate_map.py — Geocode place names → data/map.json with PRECISION tracking.

Each location is tagged with precision level:
  - pinpointed : geocoded to exact town/village
  - approximate: geocoded after stripping most-specific part
  - regional   : geocoded to state/province level
  - homeland   : geocoded to country only

German heritage places get special attention — many historical Birkenfeld-Oldenburg
principality references are resolved via hard-coded coordinates.

Uses OpenStreetMap Nominatim API (free, no key, 1 req/sec).
Cache: data/_geocode_cache.json

Run:  python generate_map.py
"""
import sqlite3, json, os, time, re, sys
from urllib.request import urlopen, Request
from urllib.parse import quote
from collections import defaultdict

DB = 'lineage.db'
CACHE_FILE = os.path.join('data', '_geocode_cache.json')
OUT_FILE = os.path.join('data', 'map.json')

# ══════════════════════════════════════════════════════════════
# Hard-coded coordinates — country/state centers + historical
# German towns the geocoder can't resolve
# ══════════════════════════════════════════════════════════════

# Countries & states (precision = "homeland" or "regional")
KNOWN_BROAD = {
    'germany': (51.16, 10.45),
    'deutschland': (51.16, 10.45),
    ', germany': (51.16, 10.45),
    'bundesrepublik deutschland, getmany': (51.16, 10.45),
    'hungary': (47.16, 19.50),
    'czechoslovakia': (49.82, 15.47),
    'czech republic': (49.82, 15.47),
    'italy': (42.50, 12.57),
    'wales': (52.13, -3.78),
    'england': (52.36, -1.17),
    'scotland': (56.49, -4.20),
    'switzerland': (46.82, 8.23),
    'france': (46.23, 2.21),
    'austria': (47.52, 14.55),
    'poland': (51.92, 19.15),
    'netherlands': (52.13, 5.29),
    'belgium': (50.50, 4.47),
    'sweden': (60.13, 18.64),
    'norway': (60.47, 8.47),
    'denmark': (56.26, 9.50),
    'ireland': (53.41, -8.24),
    'canada': (56.13, -106.35),
    'usa': (39.83, -98.58),
    'united states': (39.83, -98.58),
    # US states
    'pennsylvania': (40.88, -77.80),
    'pennsylvania, usa': (40.88, -77.80),
    'pennsylvania, united states': (40.88, -77.80),
    'michigan': (43.33, -84.54),
    'michigan, usa': (43.33, -84.54),
    'michigan, united states': (43.33, -84.54),
    'maryland': (39.05, -76.64),
    'maryland, usa': (39.05, -76.64),
    'maryland, united states': (39.05, -76.64),
    'virginia': (37.43, -78.66),
    'virginia, usa': (37.43, -78.66),
    'new york': (42.16, -74.95),
    'new york, usa': (42.16, -74.95),
    'ohio': (40.42, -82.91),
    'ohio, usa': (40.42, -82.91),
    'new jersey': (40.06, -74.41),
    'new jersey, usa': (40.06, -74.41),
    'indiana': (40.27, -86.13),
    'indiana, usa': (40.27, -86.13),
    'west virginia': (38.60, -80.45),
    'west virginia, usa': (38.60, -80.45),
    'connecticut': (41.60, -72.73),
    'connecticut, usa': (41.60, -72.73),
    'massachusetts': (42.41, -71.38),
    'massachusetts, usa': (42.41, -71.38),
    'ontario, canada': (44.50, -79.50),
    # German regions
    'hessen, germany': (50.65, 9.16),
    'bayern, germany': (48.79, 11.50),
    'baden-wuerttemberg, germany': (48.66, 9.35),
    'rheinland-pfalz, germany': (49.91, 7.45),
    'wuerttenberg, germany': (48.70, 9.30),
    'palatinate,prussia germany': (49.40, 7.80),
    'nassau-saarwerde,germany': (48.93, 7.10),
}

# ── Historical German towns — manually resolved ──
# Many are from the Principality of Birkenfeld (an exclave of the Duchy of Oldenburg)
# located in the Nahe region of Rheinland-Pfalz, NOT near Oldenburg city.
KNOWN_GERMAN = {
    # Birkenfeld principality area (Nahe region, ~49.6°N, 7.1°E)
    'birkenfeld, oldenburg, germany':                     (49.6488, 7.1647),
    'birkenfeld, oldenburg, niedersachsen, germany':      (49.6488, 7.1647),  # mislabeled — it's Nahe
    'of birkenfeld, oldenburg, germany':                  (49.6488, 7.1647),
    'birkenfeld, koblenz, rheinland-pfalz, germany':      (49.6488, 7.1647),
    'of achtelsbach, birkenfeld, oldenburg, germany':     (49.6243, 7.0893),
    'darbach, birkenfeld, oldenburg, germany':            (49.6276, 7.1332),  # = Dambach (historical spelling)
    'of buhlenberg, birkenfeld, oldenburg, germany':      (49.6554, 7.1200),
    'buhlenberg, birkenfeld, oldensberg, germany':        (49.6554, 7.1200),
    'buhlenburg, birkenfeld, rheinland-pfalz, germany':   (49.6554, 7.1200),  # typo for Buhlenberg
    'ellenberg, birkenfeld, oldenburg, alemanha':         (49.6628, 7.1422),
    'einschied, oldenberg, germany':                      (49.7100, 7.2000),
    # Solingen area
    'pilghausen, solingen, nordrhein-westfalen, germany':         (51.1900, 7.0800),
    'pilghausen, solingen, north rhine-westphalia, germany':      (51.1900, 7.0800),
    'solingen, solingen, nordrhein-westfalen, germany':           (51.1700, 7.0800),
    # Nassau-Saarwerden (historical county, now France/Germany border)
    'wolfskirchen, nassau-saarwerden, germany':           (48.9500, 7.1500),
    'wolfskirchen, nassau saarwerden, germany':           (48.9500, 7.1500),
    'pisdorf, nassau-saarwerden, germany':                (48.9400, 7.1400),
    'pisdorf, nassau saarwerden':                         (48.9400, 7.1400),
    # Other German towns that failed geocoding
    'wolfhagen, kassel, hesse, germany':                  (51.3200, 9.1700),
    'wolfhagen, hessen-nassau, preussen, germany':        (51.3200, 9.1700),
    'staufenberg, hessen, germany':                       (50.6600, 8.7300),
    'heilbronn, heilbronner stadtkreis, baden-württemberg, germany': (49.1400, 9.2200),
    'durlach, stadt karlsruhe, baden-wuerttemberg, germany':         (49.0000, 8.4700),
    'eggenstein karlsruher stadtkreis, baden-württemberg, germany':  (49.0800, 8.3900),
    'stöffel, pfaffenhofen an der ilm, bayern, germany':             (48.5300, 11.5100),
    'stadtn unter heuchelberg, germany':                  (49.1300, 9.1000),  # = Stetten am Heuchelberg
    'stadtn unter, , heuchelbreg, germany':               (49.1300, 9.1000),
    'prenzlau, preußen, brandenburg, germany':            (53.3200, 13.8600),
    'frankenthal, pfalz, bavern, germany':                (49.5300, 8.3500),
    'sprendlingen, mainz-bingen, rhineland-palatinate, germany':    (49.8700, 7.9900),
    'brunshaupten, bad doberan, mecklenburg-vorpommern, germany':   (54.1400, 11.7400),
    'schauernheim, ludwigshafen, rheinland-pfalz, germany':         (49.4300, 8.3200),
    'sankt clemens, telgte stadt, westfalen, germany':              (51.9800, 7.7900),
    'dollendorf, rheinland-pfalz, germany':               (50.3000, 6.5700),
    'oberrossbach, hessen-nassau, prussia':               (50.7000, 8.3000),
    'framershire, rheinland-pfalz, germany':              (49.7700, 8.1400),  # = Framersheim typo
    'krenznach, germany':                                 (49.8400, 7.8700),  # = Bad Kreuznach
    'langenselbold, stadt, hessen, duitsland':            (50.1700, 8.9700),
    'hannover, niedersachsen, heiliges römisches reich deutscher nation': (52.3700, 9.7400),
    'weiler, pforzheim, baden, germany':                  (48.8800, 8.7200),
    'eggerode, ahaus, borken, nordrhein-westfalen, germany': (52.0800, 7.0500),
    'evangelisch,homberg,oberhessen,hesse-darmstadt':     (50.7400, 8.9900),  # Homberg (Ohm)
    'gehabornhof, hesse-darmstadt, germany':              (50.6500, 8.8000),  # near Gießen area
    'clauberg, baden-wuerttemberg, germany':              (48.7500, 9.1800),  # uncertain
}

# Merge all KNOWN
KNOWN = {**KNOWN_BROAD, **KNOWN_GERMAN}


# ── Country name aliases ──
COUNTRY_ALIASES = {
    'alemanha': 'Germany', 'duitsland': 'Germany', 'getmany': 'Germany',
    'deutschland': 'Germany', 'preussen': 'Germany', 'prussia': 'Germany',
    'heiliges römisches reich deutscher nation': 'Germany',
    'hesse-darmstadt': 'Germany',
    'états-unis': 'USA', 'vereinigte staaten': 'USA',
    'irland': 'Ireland', 'éire': 'Ireland',
    'angleterre': 'England', 'großbritannien': 'England',
    'frankreich': 'France', 'kanada': 'Canada',
    'schweiz': 'Switzerland', 'suisse': 'Switzerland',
    'österreich': 'Austria', 'italia': 'Italy',
    'niederlande': 'Netherlands', 'belgien': 'Belgium',
}

# German-detecting keywords
GERMAN_INDICATORS = {
    'germany', 'deutschland', 'bayern', 'hessen', 'rheinland',
    'pfalz', 'westfalen', 'sachsen', 'oldenburg', 'birkenfeld',
    'nassau', 'baden', 'württemberg', 'wuerttemberg', 'saarland',
    'preussen', 'prussia', 'schwaben', 'thuringen', 'thüringen',
    'brandenburg', 'mecklenburg', 'niedersachsen', 'schleswig',
    'holy roman empire', 'palatinate', 'bavern', 'alemania',
    'alemanha', 'duitsland', 'hesse-darmstadt', 'hessen-nassau',
}


# ══════════════════════════════════════════════════════════════
# Cache
# ══════════════════════════════════════════════════════════════
cache = {}
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, 'r', encoding='utf-8') as f:
        cache = json.load(f)
    print(f'Loaded {len(cache)} cached geocodes')

def save_cache():
    os.makedirs('data', exist_ok=True)
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=1, ensure_ascii=False)


# ══════════════════════════════════════════════════════════════
# Normalization & cleaning
# ══════════════════════════════════════════════════════════════
def normalize_place(p):
    """Clean and normalize a place string."""
    if not p:
        return ''
    p = p.strip().strip(',').strip()
    while p.startswith(','):
        p = p[1:].strip()
    # Remove leading "of " / "Of " (genealogy artifact: "Of Birkenfeld")
    p = re.sub(r'^[Oo]f\s+', '', p)
    # Remove parenthetical prefixes: "(Favarotta) Terrasini" → "Terrasini"
    p = re.sub(r'^\([^)]*\)\s*', '', p)
    # Remove "age XX" suffixes
    p = re.sub(r'\s+age\s+\d+.*$', '', p, flags=re.IGNORECASE)
    # Remove religion prefixes: "Evangelisch,Homberg" → "Homberg"
    p = re.sub(r'^(Evangelisch|Katholisch|Reformed|Lutheran),?\s*', '', p, flags=re.IGNORECASE)
    # Strip again
    p = p.strip().strip(',').strip()
    return p

def is_german_place(place_str):
    """Detect if a place string refers to a German location."""
    if not place_str:
        return False
    low = place_str.lower()
    return any(kw in low for kw in GERMAN_INDICATORS)

def get_region(place_str):
    """Extract country/region from a place string."""
    if not place_str:
        return 'Unknown'
    parts = [p.strip() for p in place_str.split(',') if p.strip()]
    if not parts:
        return 'Unknown'
    last = parts[-1].strip().lower()
    # Check aliases
    if last in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[last]
    # Common mappings
    region_map = {
        'usa': 'USA', 'united states': 'USA', 'u.s.a.': 'USA', 'u.s.': 'USA',
        'us': 'USA', 'america': 'USA',
        'canada': 'Canada', 'can': 'Canada',
        'ireland': 'Ireland', 'ire': 'Ireland',
        'england': 'England', 'wales': 'Wales', 'scotland': 'Scotland',
        'germany': 'Germany', 'deutschland': 'Germany',
        'france': 'France', 'fra': 'France',
        'switzerland': 'Switzerland', 'italy': 'Italy',
        'hungary': 'Hungary', 'austria': 'Austria',
        'poland': 'Poland', 'netherlands': 'Netherlands',
        'czechoslovakia': 'Czechoslovakia', 'czech republic': 'Czech Republic',
        'belgium': 'Belgium', 'sweden': 'Sweden', 'norway': 'Norway',
        'denmark': 'Denmark',
    }
    if last in region_map:
        return region_map[last]
    # Try second-to-last for US states
    if len(parts) >= 2:
        penult = parts[-2].strip().lower()
        us_states = {'pennsylvania', 'michigan', 'ohio', 'virginia', 'new york',
                     'maryland', 'indiana', 'new jersey', 'connecticut',
                     'massachusetts', 'west virginia', 'california', 'iowa',
                     'illinois', 'wisconsin', 'minnesota', 'kentucky',
                     'tennessee', 'north carolina', 'south carolina', 'georgia',
                     'florida', 'texas', 'missouri', 'kansas', 'nebraska'}
        if penult in us_states:
            return 'USA'
    # Check German indicators anywhere  
    if is_german_place(place_str):
        return 'Germany'
    return parts[-1].strip().title()


# ══════════════════════════════════════════════════════════════
# Geocoding with precision tracking
# ══════════════════════════════════════════════════════════════
def geocode_nominatim(place):
    """Geocode via OSM Nominatim. Returns [lat, lng] or None. Uses cache."""
    key = place.lower().strip()
    if key in cache:
        return cache[key]

    if key in KNOWN:
        result = list(KNOWN[key])
        cache[key] = result
        return result

    url = f'https://nominatim.openstreetmap.org/search?q={quote(place)}&format=json&limit=1'
    headers = {'User-Agent': 'LackLineageGenealogy/1.0 (family research project)'}
    try:
        req = Request(url, headers=headers)
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode('utf-8'))
        if data:
            lat = round(float(data[0]['lat']), 5)
            lng = round(float(data[0]['lon']), 5)
            cache[key] = [lat, lng]
            return [lat, lng]
        else:
            cache[key] = None
            return None
    except Exception as e:
        print(f'\n  ⚠ Geocode error for "{place}": {e}')
        # Don't cache network errors — allow retry
        return None

def geocode_with_fallback(place):
    """Try geocoding with progressive fallback. Returns (coords, precision) or (None, None)."""
    if not place:
        return None, None

    norm = normalize_place(place)
    if not norm:
        return None, None

    key = norm.lower().strip()
    parts = [p.strip() for p in norm.split(',') if p.strip()]
    n = len(parts)

    # ── Check KNOWN first ──
    if key in KNOWN_GERMAN:
        coords = list(KNOWN_GERMAN[key])
        cache[key] = coords
        return coords, 'pinpointed'
    if key in KNOWN_BROAD:
        coords = list(KNOWN_BROAD[key])
        cache[key] = coords
        return coords, 'homeland' if n <= 1 else 'regional'

    # ── Exact match in cache ──
    if key in cache:
        if cache[key] is not None:
            prec = 'pinpointed' if n >= 2 else ('regional' if n == 1 else 'homeland')
            return cache[key], prec
        # Was cached as None — try cleaned versions below

    # ── Try Nominatim for exact ──
    result = geocode_nominatim(norm)
    if result:
        prec = 'pinpointed' if n >= 2 else ('regional' if n == 1 else 'homeland')
        return result, prec

    time.sleep(1.1)

    # ── Fallback: remove first (most specific) part → approximate ──
    if n > 1:
        fb = ', '.join(parts[1:])
        fbk = fb.lower().strip()
        if fbk in KNOWN_GERMAN:
            coords = list(KNOWN_GERMAN[fbk])
            cache[fbk] = coords
            return coords, 'approximate'
        if fbk in KNOWN_BROAD:
            coords = list(KNOWN_BROAD[fbk])
            cache[fbk] = coords
            return coords, 'regional'
        result = geocode_nominatim(fb)
        if result:
            return result, 'approximate'
        time.sleep(1.1)

    # ── Fallback: last 2 parts → regional ──
    if n > 2:
        fb2 = ', '.join(parts[-2:])
        fb2k = fb2.lower().strip()
        if fb2k in KNOWN_BROAD:
            return list(KNOWN_BROAD[fb2k]), 'regional'
        result = geocode_nominatim(fb2)
        if result:
            return result, 'regional'
        time.sleep(1.1)

    # ── Fallback: last part only → homeland ──
    if n > 1:
        last = parts[-1].strip()
        lastk = last.lower().strip()
        # Check country aliases
        if lastk in COUNTRY_ALIASES:
            canonical = COUNTRY_ALIASES[lastk].lower()
            if canonical.lower() in KNOWN_BROAD:
                return list(KNOWN_BROAD[canonical.lower()]), 'homeland'
        if lastk in KNOWN_BROAD:
            return list(KNOWN_BROAD[lastk]), 'homeland'
        result = geocode_nominatim(last)
        if result:
            return result, 'homeland'

    return None, None


def resolve_from_cache(place):
    """Re-derive coords + precision from cache (no API calls). Used for already-cached places."""
    norm = normalize_place(place)
    if not norm:
        return None, None
    key = norm.lower().strip()
    parts = [p.strip() for p in norm.split(',') if p.strip()]
    n = len(parts)

    # Check KNOWN_GERMAN first (these are pinpointed historical resolutions)
    if key in KNOWN_GERMAN:
        return list(KNOWN_GERMAN[key]), 'pinpointed'

    # Check KNOWN_BROAD
    if key in KNOWN_BROAD:
        return list(KNOWN_BROAD[key]), 'homeland' if n <= 1 else 'regional'

    # Exact match in cache
    if key in cache and cache[key] is not None:
        prec = 'pinpointed' if n >= 2 else ('regional' if n == 1 else 'homeland')
        return cache[key], prec

    # Fallback: try removing first part
    if n > 1:
        fb = ', '.join(parts[1:]).lower().strip()
        if fb in KNOWN_GERMAN:
            return list(KNOWN_GERMAN[fb]), 'approximate'
        if fb in cache and cache[fb] is not None:
            return cache[fb], 'approximate'
        if fb in KNOWN_BROAD:
            return list(KNOWN_BROAD[fb]), 'regional'

    # Fallback: last 2 parts
    if n > 2:
        fb2 = ', '.join(parts[-2:]).lower().strip()
        if fb2 in cache and cache[fb2] is not None:
            return cache[fb2], 'regional'
        if fb2 in KNOWN_BROAD:
            return list(KNOWN_BROAD[fb2]), 'regional'

    # Fallback: last part
    if n > 1:
        last = parts[-1].lower().strip()
        if last in COUNTRY_ALIASES:
            canonical = COUNTRY_ALIASES[last].lower()
            if canonical in KNOWN_BROAD:
                return list(KNOWN_BROAD[canonical]), 'homeland'
        if last in cache and cache[last] is not None:
            return cache[last], 'homeland'
        if last in KNOWN_BROAD:
            return list(KNOWN_BROAD[last]), 'homeland'

    return None, None


# ══════════════════════════════════════════════════════════════
# Main pipeline
# ══════════════════════════════════════════════════════════════
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

people = conn.execute('''
    SELECT id, given_name, surname, birth_date, death_date,
           birth_place, death_place, confidence_tier, sex
    FROM person
    WHERE (birth_place IS NOT NULL AND birth_place != '')
       OR (death_place IS NOT NULL AND death_place != '')
''').fetchall()

print(f'People with places: {len(people)}')

# Collect unique places
all_places = set()
for p in people:
    bp = normalize_place(p['birth_place'])
    dp = normalize_place(p['death_place'])
    if bp: all_places.add(bp)
    if dp: all_places.add(dp)

print(f'Unique place names: {len(all_places)}')

# ── First pass: resolve from cache + KNOWN (no API calls) ──
resolved = {}  # place → (coords, precision)
need_api = []

for place in sorted(all_places):
    coords, prec = resolve_from_cache(place)
    if coords:
        resolved[place] = (coords, prec)
    else:
        key = normalize_place(place).lower().strip()
        if key in cache and cache[key] is None:
            # Previously failed — try KNOWN_GERMAN which is new
            pass  # already checked in resolve_from_cache
        if key not in cache:
            need_api.append(place)

print(f'Resolved from cache/KNOWN: {len(resolved)}')
print(f'Need API calls: {len(need_api)}')

# ── Second pass: geocode remaining via API ──
if need_api:
    print(f'Geocoding {len(need_api)} places via Nominatim (~{len(need_api) * 1.5:.0f}s)...')

for i, place in enumerate(need_api):
    if i > 0:
        time.sleep(1.1)
    coords, prec = geocode_with_fallback(place)
    if coords:
        resolved[place] = (coords, prec)
        sys.stdout.write(f'\r  ✓ {i+1}/{len(need_api)}: {place[:55]:<55}')
    else:
        sys.stdout.write(f'\r  ✗ {i+1}/{len(need_api)}: {place[:55]:<55}')
    sys.stdout.flush()
    if i % 20 == 0:
        save_cache()

if need_api:
    print()
    save_cache()

# ── Third pass: try to resolve previously-failed cache entries via new KNOWN ──
newly_resolved = 0
for place in sorted(all_places):
    if place in resolved:
        continue
    coords, prec = resolve_from_cache(place)
    if coords:
        resolved[place] = (coords, prec)
        newly_resolved += 1

if newly_resolved:
    print(f'Resolved {newly_resolved} previously-failed places via KNOWN entries')

total = len(all_places)
found = len(resolved)
failed = total - found
print(f'\nTotal resolved: {found}/{total} ({failed} unresolvable)')

# Precision breakdown
prec_counts = defaultdict(int)
for _, (_, prec) in resolved.items():
    prec_counts[prec] += 1
for p in ['pinpointed', 'approximate', 'regional', 'homeland']:
    print(f'  {p:12s}: {prec_counts.get(p, 0):4d}')


# ══════════════════════════════════════════════════════════════
# Build map.json
# ══════════════════════════════════════════════════════════════

def yr(d):
    if not d: return None
    m = re.search(r'(\d{4})', str(d))
    return int(m.group(1)) if m else None

PRECISION_RANK = {'pinpointed': 0, 'approximate': 1, 'regional': 2, 'homeland': 3}

birth_locs = defaultdict(lambda: {'people': [], 'place_names': set(), 'precisions': []})
death_locs = defaultdict(lambda: {'people': [], 'place_names': set(), 'precisions': []})
migrations = []
research_opps = []  # places we couldn't resolve

for p in people:
    bp = normalize_place(p['birth_place'])
    dp = normalize_place(p['death_place'])
    bp_res = resolved.get(bp)
    dp_res = resolved.get(dp)

    stub = {
        'id': p['id'],
        'name': f"{p['given_name'] or ''} {p['surname'] or ''}".strip(),
        'birth_date': p['birth_date'],
        'death_date': p['death_date'],
        'birth_place': p['birth_place'],
        'death_place': p['death_place'],
        'sex': p['sex'],
        'tier': p['confidence_tier'],
        'birth_year': yr(p['birth_date']),
    }

    if bp and bp_res:
        coords, prec = bp_res
        key = f"{coords[0]},{coords[1]}"
        entry = {**stub, 'precision': prec}
        birth_locs[key]['people'].append(entry)
        birth_locs[key]['place_names'].add(bp)
        birth_locs[key]['precisions'].append(prec)
        if 'coords' not in birth_locs[key]:
            birth_locs[key]['coords'] = coords
    elif bp and not bp_res:
        research_opps.append({'place': p['birth_place'], 'type': 'birth',
                              'id': p['id'], 'name': stub['name']})

    if dp and dp_res:
        coords, prec = dp_res
        key = f"{coords[0]},{coords[1]}"
        entry = {**stub, 'precision': prec}
        death_locs[key]['people'].append(entry)
        death_locs[key]['place_names'].add(dp)
        death_locs[key]['precisions'].append(prec)
        if 'coords' not in death_locs[key]:
            death_locs[key]['coords'] = coords

    # Migration
    if bp_res and dp_res and bp_res[0] != dp_res[0]:
        migrations.append({
            'id': p['id'], 'name': stub['name'],
            'from': bp_res[0], 'to': dp_res[0],
            'from_place': p['birth_place'],
            'to_place': p['death_place'],
            'birth_year': stub['birth_year'],
            'from_precision': bp_res[1],
            'to_precision': dp_res[1],
        })


def build_markers(loc_dict):
    markers = []
    for key, loc in loc_dict.items():
        # Best precision at this location
        best_prec = min(loc['precisions'], key=lambda x: PRECISION_RANK.get(x, 9))
        # Check if German
        german = any(is_german_place(pn) for pn in loc['place_names'])
        # Region
        regions = set(get_region(pn) for pn in loc['place_names'])
        region = regions.pop() if len(regions) == 1 else ', '.join(sorted(regions))

        markers.append({
            'lat': loc['coords'][0],
            'lng': loc['coords'][1],
            'places': sorted(loc['place_names']),
            'count': len(loc['people']),
            'precision': best_prec,
            'isGerman': german,
            'region': region,
            'people': sorted(loc['people'], key=lambda x: x.get('birth_year') or 9999),
        })
    markers.sort(key=lambda x: -x['count'])
    return markers


birth_markers = build_markers(birth_locs)
death_markers = build_markers(death_locs)

# Migration routes (group by from→to)
route_map = defaultdict(lambda: {'from': None, 'to': None, 'people': []})
for m in migrations:
    rk = f"{m['from'][0]},{m['from'][1]}->{m['to'][0]},{m['to'][1]}"
    route_map[rk]['from'] = m['from']
    route_map[rk]['to'] = m['to']
    route_map[rk]['people'].append({
        'id': m['id'], 'name': m['name'],
        'from_place': m['from_place'], 'to_place': m['to_place'],
        'birth_year': m['birth_year'],
    })

migration_routes = []
for rk, route in route_map.items():
    migration_routes.append({
        'from': route['from'], 'to': route['to'],
        'count': len(route['people']),
        'people': sorted(route['people'], key=lambda x: x.get('birth_year') or 9999),
    })
migration_routes.sort(key=lambda x: -x['count'])

# Research opportunities — group by place
ro_map = defaultdict(list)
for ro in research_opps:
    ro_map[ro['place']].append({'id': ro['id'], 'name': ro['name'], 'type': ro['type']})
research_list = [{'place': p, 'count': len(ppl), 'people': ppl}
                 for p, ppl in sorted(ro_map.items(), key=lambda x: -len(x[1]))]

# Year range
all_years = [yr(p['birth_date']) for p in people if yr(p['birth_date'])]
min_year = min(all_years) if all_years else 1700
max_year = max(all_years) if all_years else 2000

# Precision summary for the map's legend data
prec_birth = defaultdict(int)
for m in birth_markers:
    prec_birth[m['precision']] += m['count']
german_birth = sum(m['count'] for m in birth_markers if m['isGerman'])

# ── Write output ──
map_data = {
    'generated': __import__('datetime').datetime.now().isoformat(),
    'birth_markers': birth_markers,
    'death_markers': death_markers,
    'migration_routes': migration_routes[:150],
    'research_opportunities': research_list[:50],
    'year_range': [min_year, max_year],
    'stats': {
        'total_places': found,
        'total_failed': failed,
        'total_people_with_places': len(people),
        'birth_clusters': len(birth_markers),
        'death_clusters': len(death_markers),
        'migration_routes': len(migration_routes),
        'migrations_people': len(migrations),
        'german_births': german_birth,
        'precision': {p: prec_birth.get(p, 0) for p in
                      ['pinpointed', 'approximate', 'regional', 'homeland']},
    }
}

with open(OUT_FILE, 'w', encoding='utf-8') as f:
    json.dump(map_data, f, indent=1, ensure_ascii=False)

conn.close()

print(f'\n✓ Generated {OUT_FILE}')
print(f'  Birth clusters: {len(birth_markers)}')
print(f'  Death clusters: {len(death_markers)}')
print(f'  Migration routes: {len(migration_routes)}')
print(f'  German births: {german_birth}')
print(f'  Research opps: {len(research_list)}')
print(f'  Year range: {min_year}–{max_year}')
print(f'  File size: {os.path.getsize(OUT_FILE):,} bytes')
