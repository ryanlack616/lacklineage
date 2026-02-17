// ══════════════════════════════════════════════════════════════
// Admin Shim — intercepts /api/admin/ fetch calls and serves
// responses from static JSON (profiles.json, people.json, stats.json)
// Makes Dad's Desk work on GitHub Pages without review_server.py
// Notes persist to localStorage. Edits are local-only.
// ══════════════════════════════════════════════════════════════

(function () {
  const _realFetch = window.fetch;

  // ── Data cache ──
  let _profiles = null;   // Map: id → full profile object
  let _people   = null;   // sorted array of {id, given_name, surname, ...}
  let _stats    = null;
  let _loaded   = false;
  let _loading  = null;

  const NOTES_KEY   = 'lack-admin-notes';    // localStorage key
  const EDITS_KEY   = 'lack-admin-edits';    // localStorage key
  const PAGE_SIZE   = 50;

  // ── Load all data ──
  async function loadData() {
    if (_loaded) return;
    if (_loading) return _loading;
    _loading = (async () => {
      const [profilesResp, peopleResp, statsResp] = await Promise.all([
        _realFetch('/data/profiles.json'),
        _realFetch('/data/people.json'),
        _realFetch('/data/stats.json'),
      ]);
      const profilesObj = await profilesResp.json();
      const peopleArr   = await peopleResp.json();
      _stats            = await statsResp.json();

      // Build profiles map (keyed by numeric id)
      _profiles = new Map();
      for (const [k, v] of Object.entries(profilesObj)) {
        _profiles.set(parseInt(k), v);
      }

      // Build sortable people array from people.json (lighter weight)
      _people = peopleArr.map(p => ({
        id: p.id,
        given_name: p.given_name,
        surname: p.surname,
        sex: p.sex,
        birth_date: p.birth_date,
        death_date: p.death_date,
        confidence: p.confidence || 0,
        confidence_tier: p.confidence_tier || 'speculative',
        source_count: p.source_count || 0,
      }));

      // Merge doc counts from profiles
      for (const p of _people) {
        const prof = _profiles.get(p.id);
        p.doc_count = prof?.documents?.length || 0;
      }

      // Default sort: by surname then given_name
      _people.sort((a, b) => {
        const sa = (a.surname || '').toLowerCase();
        const sb = (b.surname || '').toLowerCase();
        if (sa !== sb) return sa < sb ? -1 : 1;
        const ga = (a.given_name || '').toLowerCase();
        const gb = (b.given_name || '').toLowerCase();
        return ga < gb ? -1 : ga > gb ? 1 : 0;
      });

      // Apply any saved local edits on top
      applyLocalEdits();

      _loaded = true;
    })();
    return _loading;
  }

  // ── localStorage helpers ──
  function getNotes() {
    try { return JSON.parse(localStorage.getItem(NOTES_KEY) || '{}'); }
    catch { return {}; }
  }
  function saveNotes(notes) {
    localStorage.setItem(NOTES_KEY, JSON.stringify(notes));
  }
  function getEdits() {
    try { return JSON.parse(localStorage.getItem(EDITS_KEY) || '{}'); }
    catch { return {}; }
  }
  function saveEdits(edits) {
    localStorage.setItem(EDITS_KEY, JSON.stringify(edits));
  }

  function applyLocalEdits() {
    const edits = getEdits();
    for (const [idStr, fields] of Object.entries(edits)) {
      const id = parseInt(idStr);
      const prof = _profiles.get(id);
      if (!prof) continue;
      for (const [k, v] of Object.entries(fields)) {
        prof[k] = v;
      }
      // Also update the light _people entry
      const pp = _people.find(p => p.id === id);
      if (pp) {
        if (fields.given_name !== undefined) pp.given_name = fields.given_name;
        if (fields.surname !== undefined)    pp.surname = fields.surname;
        if (fields.sex !== undefined)        pp.sex = fields.sex;
        if (fields.birth_date !== undefined) pp.birth_date = fields.birth_date;
        if (fields.death_date !== undefined) pp.death_date = fields.death_date;
      }
    }
  }

  // ── Stats ──
  function adminStats() {
    const allNotes = getNotes();
    let noteCount = 0;
    for (const notes of Object.values(allNotes)) noteCount += notes.length;

    const allEdits = getEdits();
    let editCount = 0;
    for (const fields of Object.values(allEdits)) editCount += Object.keys(fields).length;

    return {
      total_people:     _stats?.total_people || _people.length,
      total_documents:  _stats?.documents || 0,
      verified_matches: _stats?.document_matches || 0,
      total_notes:      noteCount,
      total_edits:      editCount,
    };
  }

  // ── Paginated people list ──
  function adminPeople(params) {
    const q    = (params.get('q') || '').toLowerCase().trim();
    const tier = (params.get('tier') || '').toLowerCase();
    const page = parseInt(params.get('page')) || 1;

    let filtered = _people;

    if (q) {
      filtered = filtered.filter(p => {
        const name = ((p.given_name || '') + ' ' + (p.surname || '')).toLowerCase();
        return name.includes(q);
      });
    }
    if (tier) {
      filtered = filtered.filter(p => (p.confidence_tier || '').toLowerCase() === tier);
    }

    const total = filtered.length;
    const pages = Math.max(1, Math.ceil(total / PAGE_SIZE));
    const safePage = Math.min(Math.max(1, page), pages);
    const start = (safePage - 1) * PAGE_SIZE;
    const slice = filtered.slice(start, start + PAGE_SIZE);

    return {
      people: slice.map(p => ({
        id: p.id,
        name: ((p.given_name || '') + ' ' + (p.surname || '')).trim(),
        given_name: p.given_name,
        surname: p.surname,
        sex: p.sex,
        birth_date: p.birth_date,
        death_date: p.death_date,
        confidence_tier: p.confidence_tier,
        doc_count: p.doc_count || 0,
      })),
      total,
      page: safePage,
      pages,
    };
  }

  // ── Person detail ──
  function adminPerson(id) {
    const prof = _profiles.get(id);
    if (!prof) return { error: 'Person not found' };

    // Merge localStorage notes
    const savedNotes = getNotes();
    const personNotes = (savedNotes[id] || []).map((n, i) => ({
      id: `local-${id}-${i}`,
      note: n.note,
      reviewer: n.reviewer || 'Dad',
      created_date: n.created_date,
    }));

    // Build edit history from localStorage
    const allEdits = getEdits();
    const personEdits = allEdits[id];
    const editHistory = [];
    if (personEdits && personEdits._history) {
      for (const h of personEdits._history) {
        editHistory.push(h);
      }
    }

    // Documents with thumb paths adjusted for static hosting
    const docs = (prof.documents || []).map(d => ({
      id: d.doc_id,
      doc_type: d.doc_type || 'photo',
      description: d.description,
      filename: d.description,
      filepath: d.thumb || null,   // use thumb as filepath for img src
      match_confidence: d.match_confidence,
      verified: d.verified,
    }));

    return {
      id: prof.id,
      given_name: prof.given_name,
      surname: prof.surname,
      suffix: prof.suffix || null,
      sex: prof.sex,
      birth_date: prof.birth_date,
      birth_place: prof.birth_place,
      death_date: prof.death_date,
      death_place: prof.death_place,
      confidence: prof.confidence || 0,
      confidence_tier: prof.confidence_tier || 'speculative',
      parents:  (prof.parents || []).map(r => ({ id: r.id, given_name: r.name?.split(' ').slice(0, -1).join(' ') || r.name, surname: r.name?.split(' ').pop() || '', sex: r.sex, birth_date: r.birth_date, death_date: r.death_date })),
      spouses:  (prof.spouses || []).map(r => ({ id: r.id, given_name: r.name?.split(' ').slice(0, -1).join(' ') || r.name, surname: r.name?.split(' ').pop() || '', sex: r.sex, birth_date: r.birth_date, death_date: r.death_date })),
      children: (prof.children || []).map(r => ({ id: r.id, given_name: r.name?.split(' ').slice(0, -1).join(' ') || r.name, surname: r.name?.split(' ').pop() || '', sex: r.sex, birth_date: r.birth_date, death_date: r.death_date })),
      siblings: (prof.siblings || []).map(r => ({ id: r.id, given_name: r.name?.split(' ').slice(0, -1).join(' ') || r.name, surname: r.name?.split(' ').pop() || '', sex: r.sex, birth_date: r.birth_date, death_date: r.death_date })),
      documents: docs,
      notes: personNotes,
      edit_history: editHistory,
    };
  }

  // ── Save person edits (local only) ──
  function savePerson(id, body) {
    const prof = _profiles.get(id);
    if (!prof) return { error: 'Person not found' };

    const editable = ['given_name', 'surname', 'suffix', 'sex', 'birth_date', 'birth_place', 'death_date', 'death_place'];
    const edits = getEdits();
    if (!edits[id]) edits[id] = { _history: [] };

    let changes = 0;
    for (const field of editable) {
      if (body[field] !== undefined && body[field] !== (prof[field] || null)) {
        const oldVal = prof[field] || null;
        const newVal = body[field];

        edits[id][field] = newVal;
        edits[id]._history.push({
          field,
          old_value: oldVal || '',
          new_value: newVal || '',
          reviewer: body.reviewer || 'Dad',
          edit_date: new Date().toISOString(),
        });

        // Apply immediately to in-memory data
        prof[field] = newVal;
        const pp = _people.find(p => p.id === id);
        if (pp && pp[field] !== undefined) pp[field] = newVal;
        changes++;
      }
    }

    saveEdits(edits);
    return { ok: true, changes };
  }

  // ── Add / delete notes ──
  function addNote(personId, body) {
    const notes = getNotes();
    if (!notes[personId]) notes[personId] = [];
    notes[personId].push({
      note: body.note,
      reviewer: body.reviewer || 'Dad',
      created_date: new Date().toISOString(),
    });
    saveNotes(notes);
    return { ok: true };
  }

  function deleteNote(noteIdStr) {
    // noteIdStr looks like "local-{personId}-{index}"
    const m = noteIdStr.match(/^local-(\d+)-(\d+)$/);
    if (!m) return { error: 'Invalid note id' };
    const personId = parseInt(m[1]);
    const idx = parseInt(m[2]);
    const notes = getNotes();
    const arr = notes[personId] || [];
    if (idx >= 0 && idx < arr.length) {
      arr.splice(idx, 1);
      notes[personId] = arr;
      saveNotes(notes);
    }
    return { ok: true };
  }

  // ── JSON response helper ──
  function jsonResponse(data, status) {
    return new Response(JSON.stringify(data), {
      status: status || 200,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  // ── Intercept fetch ──
  window.fetch = async function (url, opts) {
    const u = typeof url === 'string' ? url : url.toString();

    // Only intercept /api/admin/ calls
    if (!u.startsWith('/api/admin/')) return _realFetch.call(window, url, opts);

    await loadData();
    const method = (opts?.method || 'GET').toUpperCase();

    // GET /api/admin/stats
    if (u === '/api/admin/stats' && method === 'GET') {
      return jsonResponse(adminStats());
    }

    // GET /api/admin/people?...
    if (u.startsWith('/api/admin/people') && method === 'GET') {
      const params = new URLSearchParams(u.split('?')[1] || '');
      return jsonResponse(adminPeople(params));
    }

    // POST /api/admin/person/:id/upload
    const uploadM = u.match(/^\/api\/admin\/person\/(\d+)\/upload$/);
    if (uploadM && method === 'POST') {
      return jsonResponse({ error: 'Upload requires the review server (run review_server.py locally)' }, 400);
    }

    // POST /api/admin/person/:id/note
    const noteM = u.match(/^\/api\/admin\/person\/(\d+)\/note$/);
    if (noteM && method === 'POST') {
      let body = {};
      try { body = JSON.parse(opts?.body || '{}'); } catch {}
      return jsonResponse(addNote(parseInt(noteM[1]), body));
    }

    // POST /api/admin/note/:id/delete
    const delNoteM = u.match(/^\/api\/admin\/note\/([^/]+)\/delete$/);
    if (delNoteM && method === 'POST') {
      return jsonResponse(deleteNote(delNoteM[1]));
    }

    // POST /api/admin/person/:id (save edits)
    const saveM = u.match(/^\/api\/admin\/person\/(\d+)$/);
    if (saveM && method === 'POST') {
      let body = {};
      try { body = JSON.parse(opts?.body || '{}'); } catch {}
      return jsonResponse(savePerson(parseInt(saveM[1]), body));
    }

    // GET /api/admin/person/:id
    const getM = u.match(/^\/api\/admin\/person\/(\d+)$/);
    if (getM && method === 'GET') {
      return jsonResponse(adminPerson(parseInt(getM[1])));
    }

    // Unknown
    return jsonResponse({ error: 'Not found (static mode)' }, 404);
  };

  console.log('[admin-shim] Static admin mode active — edits & notes save to localStorage');
})();
