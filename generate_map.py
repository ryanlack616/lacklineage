"""
generate_map.py — Geocode place names and produce data/map.json for the Places map page.

Uses OpenStreetMap Nominatim API (free, no key needed, 1 req/sec).
Caches results to data/_geocode_cache.json so subsequent runs are instant.

Run:  python generate_map.py
"""
import sqlite3, json, os, time, re, sys
from urllib.request import urlopen, Request
from urllib.parse import quote
from collections import defaultdict

DB = 'lineage.db'
CACHE_FILE = os.path.join('data', '_geocode_cache.json')
OUT_FILE = os.path.join('data', 'map.json')

# ── Hard-coded coordinates for common vague/country/state entries ──
# Saves API calls and provides better placement for broad entries
KNOWN = {
    'pennsylvania': (40.88, -77.80),
    'pennsylvania, usa': (40.88, -77.80),
    'pennsylvania, united states': (40.88, -77.80),
    'michigan': (43.33, -84.54),
    'michigan, usa': (43.33, -84.54),
    'michigan, united states': (43.33, -84.54),
    'ontario, canada': (44.50, -79.50),
    'ireland': (53.41, -8.24),
    'germany': (51.16, 10.45),
    'deutschland': (51.16, 10.45),
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
    'canada': (56.13, -106.35),
    'usa': (39.83, -98.58),
    'united states': (39.83, -98.58),
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
}

# ── Load cache ──
cache = {}
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, 'r', encoding='utf-8') as f:
        cache = json.load(f)
    print(f'Loaded {len(cache)} cached geocodes')

def save_cache():
    os.makedirs('data', exist_ok=True)
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=1, ensure_ascii=False)

def normalize_place(p):
    """Normalize place name for dedup and lookup."""
    if not p:
        return ''
    p = p.strip().strip(',').strip()
    # Remove leading comma artifacts
    while p.startswith(','):
        p = p[1:].strip()
    return p

def geocode_nominatim(place):
    """Geocode a place name using OSM Nominatim. Returns (lat, lng) or None."""
    key = place.lower().strip()
    if key in cache:
        return cache[key]

    # Check hard-coded first
    if key in KNOWN:
        result = list(KNOWN[key])
        cache[key] = result
        return result

    # Try Nominatim
    url = f'https://nominatim.openstreetmap.org/search?q={quote(place)}&format=json&limit=1'
    headers = {'User-Agent': 'LackLineageGenealogy/1.0 (family research project)'}
    try:
        req = Request(url, headers=headers)
        with urlopen(req, timeout=10) as resp:
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
        print(f'  ⚠ Geocode error for "{place}": {e}')
        cache[key] = None
        return None

def geocode_with_fallback(place):
    """Try geocoding; if fails, try progressively less specific versions."""
    if not place:
        return None

    norm = normalize_place(place)
    if not norm:
        return None

    # Try exact
    result = geocode_nominatim(norm)
    if result:
        return result

    # Rate limit
    time.sleep(1.1)

    # Try removing first part (most specific)
    parts = [p.strip() for p in norm.split(',') if p.strip()]
    if len(parts) > 1:
        fallback = ', '.join(parts[1:])
        result = geocode_nominatim(fallback)
        if result:
            return result
        time.sleep(1.1)

    # Try just last 2 parts
    if len(parts) > 2:
        fallback2 = ', '.join(parts[-2:])
        result = geocode_nominatim(fallback2)
        if result:
            return result
        time.sleep(1.1)

    # Try last part only (country)
    if len(parts) > 1:
        result = geocode_nominatim(parts[-1])
        if result:
            return result

    return None

# ── Gather all unique places ──
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# Get all people with places
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

# ── Geocode all places ──
geocoded = {}
uncached = [p for p in all_places if p.lower().strip() not in cache]
print(f'Already cached: {len(all_places) - len(uncached)}')
print(f'Need to geocode: {len(uncached)}')

if uncached:
    print(f'Geocoding {len(uncached)} places via Nominatim (1 req/sec)...')
    print(f'Estimated time: ~{len(uncached) * 1.2:.0f} seconds')

for i, place in enumerate(sorted(all_places)):
    key = place.lower().strip()

    if key in cache and cache[key] is not None:
        geocoded[place] = cache[key]
        continue

    if key in cache and cache[key] is None:
        # Previously failed - skip
        continue

    # Need to geocode
    if i > 0 and place not in [p for p in all_places if p.lower().strip() in KNOWN]:
        time.sleep(1.1)

    result = geocode_with_fallback(place)
    if result:
        geocoded[place] = result
        sys.stdout.write(f'\r  Geocoded {i+1}/{len(all_places)}: {place[:50]:<50}')
        sys.stdout.flush()
    else:
        sys.stdout.write(f'\r  Failed   {i+1}/{len(all_places)}: {place[:50]:<50}')
        sys.stdout.flush()

    # Save cache periodically
    if i % 20 == 0:
        save_cache()

print()
save_cache()

failed = len(all_places) - len(geocoded)
print(f'\nGeocoded: {len(geocoded)}/{len(all_places)} ({failed} failed)')

# ── Build map data ──
# Group people by birth place location
birth_locations = defaultdict(lambda: {'people': [], 'place_names': set()})
death_locations = defaultdict(lambda: {'people': [], 'place_names': set()})
migrations = []

def yr(d):
    if not d: return None
    m = re.search(r'(\d{4})', str(d))
    return int(m.group(1)) if m else None

for p in people:
    bp = normalize_place(p['birth_place'])
    dp = normalize_place(p['death_place'])
    bp_coords = geocoded.get(bp) if bp else None
    dp_coords = geocoded.get(dp) if dp else None

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

    if bp_coords:
        key = f"{bp_coords[0]},{bp_coords[1]}"
        birth_locations[key]['people'].append(stub)
        birth_locations[key]['place_names'].add(bp)
        if 'coords' not in birth_locations[key]:
            birth_locations[key]['coords'] = bp_coords

    if dp_coords:
        key = f"{dp_coords[0]},{dp_coords[1]}"
        death_locations[key]['people'].append({**stub})
        death_locations[key]['place_names'].add(dp)
        if 'coords' not in death_locations[key]:
            death_locations[key]['coords'] = dp_coords

    # Migration: if different birth & death locations
    if bp_coords and dp_coords and bp_coords != dp_coords:
        migrations.append({
            'id': p['id'],
            'name': stub['name'],
            'from': bp_coords,
            'to': dp_coords,
            'from_place': p['birth_place'],
            'to_place': p['death_place'],
            'birth_year': stub['birth_year'],
        })

# Convert to serializable format
birth_markers = []
for key, loc in birth_locations.items():
    birth_markers.append({
        'lat': loc['coords'][0],
        'lng': loc['coords'][1],
        'places': sorted(loc['place_names']),
        'count': len(loc['people']),
        'people': sorted(loc['people'], key=lambda x: x.get('birth_year') or 9999),
    })
birth_markers.sort(key=lambda x: -x['count'])

death_markers = []
for key, loc in death_locations.items():
    death_markers.append({
        'lat': loc['coords'][0],
        'lng': loc['coords'][1],
        'places': sorted(loc['place_names']),
        'count': len(loc['people']),
        'people': sorted(loc['people'], key=lambda x: x.get('birth_year') or 9999),
    })
death_markers.sort(key=lambda x: -x['count'])

# Migration summary (group by route)
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
        'from': route['from'],
        'to': route['to'],
        'count': len(route['people']),
        'people': sorted(route['people'], key=lambda x: x.get('birth_year') or 9999),
    })
migration_routes.sort(key=lambda x: -x['count'])

# Year range for time slider
all_years = [yr(p['birth_date']) for p in people if yr(p['birth_date'])]
min_year = min(all_years) if all_years else 1700
max_year = max(all_years) if all_years else 2000

# ── Write output ──
map_data = {
    'generated': __import__('datetime').datetime.now().isoformat(),
    'birth_markers': birth_markers,
    'death_markers': death_markers,
    'migration_routes': migration_routes[:100],
    'year_range': [min_year, max_year],
    'stats': {
        'total_places': len(geocoded),
        'total_people_with_places': len(people),
        'birth_clusters': len(birth_markers),
        'death_clusters': len(death_markers),
        'migration_routes': len(migration_routes),
        'migrations_people': len(migrations),
    }
}

with open(OUT_FILE, 'w', encoding='utf-8') as f:
    json.dump(map_data, f, indent=1, ensure_ascii=False)

conn.close()

print(f'\n✓ Generated {OUT_FILE}')
print(f'  Birth clusters: {len(birth_markers)}')
print(f'  Death clusters: {len(death_markers)}')
print(f'  Migration routes: {len(migration_routes)}')
print(f'  Year range: {min_year}–{max_year}')
print(f'  File size: {os.path.getsize(OUT_FILE):,} bytes')
