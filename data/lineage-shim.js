// ══════════════════════════════════════════════════════════
// Lineage Shim — intercepts /api/ fetch calls and serves
// responses from static JSON files (graph-all.json + people.json)
// Drop-in replacement for the Python server API
// ══════════════════════════════════════════════════════════

(function() {
  const _realFetch = window.fetch;

  // ── Data store ──
  let _people = null;    // Map: id → person {id, given_name, surname, sex, birth_date, birth_place, death_date, death_place}
  let _parentOf = null;  // Map: childId → [parentId, ...]
  let _childOf = null;   // Map: parentId → [childId, ...]
  let _spouseOf = null;  // Map: personId → [spouseId, ...]
  let _loaded = false;
  let _loading = null;

  async function loadData() {
    if (_loaded) return;
    if (_loading) return _loading;
    _loading = (async () => {
      const [graphResp, peopleResp] = await Promise.all([
        _realFetch('/data/graph-all.json'),
        _realFetch('/data/people.json')
      ]);
      const graph = await graphResp.json();
      const peopleArr = await peopleResp.json();

      _people = new Map();
      for (const p of peopleArr) _people.set(p.id, p);
      // Merge graph nodes (they may have fields people.json lacks)
      for (const n of graph.nodes) {
        if (!_people.has(n.id)) {
          _people.set(n.id, {
            id: n.id, given_name: n.name, surname: n.surname,
            sex: n.sex, birth_date: null, birth_place: n.birth_place,
            death_date: null, death_place: null
          });
        }
      }

      _parentOf = new Map();
      _childOf  = new Map();
      _spouseOf = new Map();

      for (const link of graph.links) {
        if (link.type === 'parent_child') {
          // source = parent, target = child
          if (!_parentOf.has(link.target)) _parentOf.set(link.target, []);
          _parentOf.get(link.target).push(link.source);
          if (!_childOf.has(link.source)) _childOf.set(link.source, []);
          _childOf.get(link.source).push(link.target);
        } else if (link.type === 'spouse') {
          if (!_spouseOf.has(link.source)) _spouseOf.set(link.source, []);
          _spouseOf.get(link.source).push(link.target);
          if (!_spouseOf.has(link.target)) _spouseOf.set(link.target, []);
          _spouseOf.get(link.target).push(link.source);
        }
      }
      _loaded = true;
    })();
    return _loading;
  }

  // ── Helpers ──
  function pName(p) {
    if (!p) return 'Unknown';
    return ((p.given_name || '') + ' ' + (p.surname || '')).trim() || 'Unknown';
  }

  function spouseList(id) {
    return (_spouseOf.get(id) || []).map(sid => {
      const s = _people.get(sid);
      return s ? { id: s.id, name: pName(s) } : null;
    }).filter(Boolean);
  }

  function treeNode(id) {
    const p = _people.get(id);
    if (!p) return null;
    return {
      id: p.id,
      name: pName(p),
      sex: p.sex,
      birth: p.birth_date,
      death: p.death_date,
      birth_place: p.birth_place,
      death_place: p.death_place,
      spouses: spouseList(id),
      confidence: p.confidence || 0,
      confidence_tier: p.confidence_tier || 'speculative'
    };
  }

  // ── Tree builders ──
  function buildAncestors(id, maxD, d) {
    d = d || 0;
    const node = treeNode(id);
    if (!node) return null;
    if (d < maxD) {
      const pids = _parentOf.get(id) || [];
      if (pids.length) node.parents = pids.map(pid => buildAncestors(pid, maxD, d+1)).filter(Boolean);
    }
    return node;
  }

  function buildDescendants(id, maxD, d) {
    d = d || 0;
    const node = treeNode(id);
    if (!node) return null;
    if (d < maxD) {
      const cids = _childOf.get(id) || [];
      if (cids.length) node.children = cids.map(cid => buildDescendants(cid, maxD, d+1)).filter(Boolean);
    }
    return node;
  }

  function buildFullTree(id, maxD, d) {
    d = d || 0;
    const node = treeNode(id);
    if (!node) return null;
    if (d < maxD) {
      const pids = _parentOf.get(id) || [];
      if (pids.length) node.parents = pids.map(pid => buildAncestors(pid, maxD, d+1)).filter(Boolean);
      const cids = _childOf.get(id) || [];
      if (cids.length) node.children = cids.map(cid => buildDescendants(cid, maxD, d+1)).filter(Boolean);
    }
    return node;
  }

  // ── Person detail ──
  function personDetail(id) {
    const p = _people.get(id);
    if (!p) return { error: 'Person not found' };

    const parentIds  = _parentOf.get(id)  || [];
    const childIds   = _childOf.get(id)   || [];
    const spouseIds  = _spouseOf.get(id)  || [];

    const siblingIds = new Set();
    for (const pid of parentIds) {
      for (const sid of (_childOf.get(pid) || [])) {
        if (sid !== id) siblingIds.add(sid);
      }
    }

    const brief = (pid) => {
      const pp = _people.get(pid);
      return pp ? { id: pp.id, given_name: pp.given_name, surname: pp.surname } : null;
    };

    return {
      id: p.id,
      given_name: p.given_name,
      surname: p.surname,
      sex: p.sex,
      birth_date: p.birth_date,
      birth_place: p.birth_place,
      death_date: p.death_date,
      death_place: p.death_place,
      confidence: p.confidence || 0,
      confidence_tier: p.confidence_tier || 'speculative',
      source_count: p.source_count || 0,
      parents:  parentIds.map(brief).filter(Boolean),
      children: childIds.map(brief).filter(Boolean),
      spouses:  spouseIds.map(brief).filter(Boolean),
      siblings: [...siblingIds].map(brief).filter(Boolean),
      events: []
    };
  }

  // ── Search ──
  function searchPeople(q) {
    const lq = q.toLowerCase();
    const results = [];
    for (const [, p] of _people) {
      const name = ((p.given_name || '') + ' ' + (p.surname || '')).toLowerCase();
      if (name.includes(lq)) {
        results.push(p);
        if (results.length >= 20) break;
      }
    }
    return results;
  }

  // ── JSON Response helper ──
  function jsonResponse(data) {
    return new Response(JSON.stringify(data), {
      status: 200,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  // ── Intercept fetch ──
  window.fetch = async function(url, ...args) {
    const u = typeof url === 'string' ? url : url.toString();

    // Only intercept /api/ calls
    if (!u.startsWith('/api/')) return _realFetch.call(window, url, ...args);

    await loadData();

    // /api/ancestors?id=X&depth=N
    if (u.startsWith('/api/ancestors')) {
      const params = new URLSearchParams(u.split('?')[1] || '');
      const id    = parseInt(params.get('id'));
      const depth = parseInt(params.get('depth')) || 6;
      const result = buildAncestors(id, depth);
      return jsonResponse(result || { error: 'Person not found' });
    }

    // /api/descendants?id=X&depth=N
    if (u.startsWith('/api/descendants')) {
      const params = new URLSearchParams(u.split('?')[1] || '');
      const id    = parseInt(params.get('id'));
      const depth = parseInt(params.get('depth')) || 6;
      const result = buildDescendants(id, depth);
      return jsonResponse(result || { error: 'Person not found' });
    }

    // /api/tree?root=X&depth=N
    if (u.startsWith('/api/tree')) {
      const params = new URLSearchParams(u.split('?')[1] || '');
      const id    = parseInt(params.get('root'));
      const depth = parseInt(params.get('depth')) || 6;
      const result = buildFullTree(id, depth);
      return jsonResponse(result || { error: 'Person not found' });
    }

    // /api/person/X
    if (u.match(/^\/api\/person\/(\d+)/)) {
      const id = parseInt(u.match(/^\/api\/person\/(\d+)/)[1]);
      return jsonResponse(personDetail(id));
    }

    // /api/search?q=X
    if (u.startsWith('/api/search')) {
      const params = new URLSearchParams(u.split('?')[1] || '');
      const q = params.get('q') || '';
      return jsonResponse(searchPeople(q));
    }

    // Unknown API endpoint — 404
    return new Response(JSON.stringify({ error: 'Not found' }), {
      status: 404,
      headers: { 'Content-Type': 'application/json' }
    });
  };
})();
