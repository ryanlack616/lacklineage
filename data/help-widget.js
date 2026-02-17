/* ─────────────────────────────────────────────
   Lack Lineage — Collapsible Help Widget
   Include on any page, then call:
     initHelp({ title, intro, keys:[], sections:[] })
   ───────────────────────────────────────────── */

(function () {
  // ── Inject CSS ──
  const style = document.createElement('style');
  style.textContent = `
/* Help toggle button */
.help-btn {
  position: fixed; bottom: 4.5rem; left: 1.5rem; z-index: 900;
  width: 38px; height: 38px; border-radius: 50%;
  background: #5b4636; color: #fff; border: none; cursor: pointer;
  font-size: 1.2rem; font-weight: 700; font-family: Georgia, serif;
  box-shadow: 0 2px 8px rgba(0,0,0,0.25);
  transition: transform 0.2s, background 0.2s;
  display: flex; align-items: center; justify-content: center;
}
.help-btn:hover { background: #7a5c42; transform: scale(1.08); }
.help-btn.active { background: #e67e22; }

/* Help panel */
.help-panel {
  position: fixed; left: 0; top: 0; bottom: 0; z-index: 899;
  width: min(420px, 92vw); background: #faf8f4;
  box-shadow: 4px 0 24px rgba(0,0,0,0.18);
  transform: translateX(-100%);
  transition: transform 0.3s cubic-bezier(0.4,0,0.2,1);
  display: flex; flex-direction: column;
  font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
  color: #3a2e25;
}
.help-panel.open { transform: translateX(0); }

.help-panel-header {
  display: flex; align-items: center; justify-content: space-between;
  padding: 1.1rem 1.3rem 0.8rem; border-bottom: 1px solid #e0dbd4;
  background: #f3efe8;
}
.help-panel-header h2 {
  margin: 0; font-size: 1.1rem; font-weight: 700; color: #5b4636;
  font-family: Georgia, 'Palatino Linotype', serif;
}
.help-panel-close {
  background: none; border: none; cursor: pointer;
  font-size: 1.4rem; color: #7a5c42; padding: 0 0.3rem;
  line-height: 1; transition: color 0.15s;
}
.help-panel-close:hover { color: #c44; }

.help-panel-body {
  flex: 1; overflow-y: auto; padding: 1rem 1.3rem 2rem;
  scroll-behavior: smooth;
}
.help-panel-body::-webkit-scrollbar { width: 5px; }
.help-panel-body::-webkit-scrollbar-thumb { background: #ccc; border-radius: 3px; }

/* Intro text */
.help-intro {
  font-size: 0.88rem; color: #6b5e52; line-height: 1.55;
  margin-bottom: 1.2rem; padding-bottom: 0.8rem; border-bottom: 1px solid #ece8e1;
}

/* Key map table */
.help-keymap { margin-bottom: 1.3rem; }
.help-keymap h3 {
  font-size: 0.82rem; text-transform: uppercase; letter-spacing: 0.08em;
  color: #7a5c42; margin: 0 0 0.6rem; font-weight: 700;
}
.help-keymap table {
  width: 100%; border-collapse: collapse; font-size: 0.82rem;
}
.help-keymap th {
  text-align: left; font-weight: 600; color: #5b4636;
  padding: 0.35rem 0.6rem; border-bottom: 2px solid #e0dbd4;
  font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em;
}
.help-keymap td {
  padding: 0.35rem 0.6rem; border-bottom: 1px solid #ece8e1;
  vertical-align: top;
}
.help-keymap tr:last-child td { border-bottom: none; }
.help-keymap tr:hover td { background: #f3efe8; }
.help-key {
  display: inline-block; background: #ece8e1; color: #5b4636;
  padding: 0.12rem 0.45rem; border-radius: 4px; font-family: 'Consolas', 'SF Mono', monospace;
  font-size: 0.78rem; font-weight: 600; white-space: nowrap;
  border: 1px solid #d8d2c8;
}
.help-key + .help-key { margin-left: 0.25rem; }

/* Collapsible sections */
.help-section { margin-bottom: 0.5rem; }
.help-section-toggle {
  width: 100%; text-align: left; background: #f3efe8;
  border: 1px solid #e0dbd4; border-radius: 6px; cursor: pointer;
  padding: 0.55rem 0.8rem; font-size: 0.85rem; font-weight: 600;
  color: #5b4636; display: flex; align-items: center; gap: 0.5rem;
  transition: background 0.15s;
}
.help-section-toggle:hover { background: #ece8e1; }
.help-section-toggle::before {
  content: '▸'; font-size: 0.75rem; transition: transform 0.2s;
  display: inline-block; width: 0.9rem; text-align: center;
}
.help-section.open .help-section-toggle::before { transform: rotate(90deg); }
.help-section-toggle .help-section-icon { font-size: 0.9rem; }
.help-section-body {
  max-height: 0; overflow: hidden; transition: max-height 0.3s ease;
  padding: 0 0.8rem;
}
.help-section.open .help-section-body {
  max-height: 2000px; padding: 0.6rem 0.8rem 0.8rem;
}
.help-section-body p {
  margin: 0 0 0.5rem; font-size: 0.82rem; line-height: 1.55; color: #4a3f35;
}
.help-section-body ul {
  margin: 0 0 0.5rem; padding-left: 1.2rem; font-size: 0.82rem; line-height: 1.6;
  color: #4a3f35;
}
.help-section-body li { margin-bottom: 0.2rem; }
.help-section-body strong { color: #5b4636; }
.help-section-body code {
  background: #ece8e1; padding: 0.1rem 0.35rem; border-radius: 3px;
  font-size: 0.78rem; font-family: 'Consolas', monospace;
}

/* Keyboard shortcut hint at bottom */
.help-shortcut-hint {
  margin-top: 1rem; padding-top: 0.6rem; border-top: 1px solid #ece8e1;
  font-size: 0.75rem; color: #9a8c7e; text-align: center;
}
`;
  document.head.appendChild(style);

  // ── Build DOM ──
  window.initHelp = function (config) {
    const { title = 'Help', intro = '', keys = [], keyboard = [], mouse = [], sections = [] } = config;

    // If only keys[] provided (legacy), use it as combined; otherwise use separate arrays
    const hasCategories = keyboard.length || mouse.length;

    // Button
    const btn = document.createElement('button');
    btn.className = 'help-btn';
    btn.setAttribute('title', 'Help (press ?)');
    btn.textContent = '?';
    document.body.appendChild(btn);

    // Panel
    const panel = document.createElement('div');
    panel.className = 'help-panel';
    panel.innerHTML = `
      <div class="help-panel-header">
        <h2>${title}</h2>
        <button class="help-panel-close" title="Close help">&times;</button>
      </div>
      <div class="help-panel-body">
        ${intro ? `<div class="help-intro">${intro}</div>` : ''}
        ${hasCategories ? (keyboard.length ? buildInputMap('⌨ Keyboard', keyboard) : '') : (keys.length ? buildInputMap('⌨ Key & Mouse Map', keys) : '')}
        ${hasCategories && mouse.length ? buildInputMap('🖱 Mouse', mouse) : ''}
        ${sections.map((s, i) => buildSection(s, i)).join('')}
        <div class="help-shortcut-hint">Press <span class="help-key">?</span> to toggle this panel</div>
      </div>
    `;
    document.body.appendChild(panel);

    // Toggle logic
    function toggle() {
      const open = panel.classList.toggle('open');
      btn.classList.toggle('active', open);
    }
    function close() {
      panel.classList.remove('open');
      btn.classList.remove('active');
    }

    btn.addEventListener('click', toggle);
    panel.querySelector('.help-panel-close').addEventListener('click', close);

    // Click outside to close
    document.addEventListener('click', function (e) {
      if (panel.classList.contains('open') && !panel.contains(e.target) && e.target !== btn) {
        close();
      }
    });

    // Section toggles
    panel.querySelectorAll('.help-section-toggle').forEach(tog => {
      tog.addEventListener('click', function () {
        this.parentElement.classList.toggle('open');
      });
    });

    // ? key to toggle (ignore if typing in input)
    document.addEventListener('keydown', function (e) {
      if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.isContentEditable) return;
      if (e.key === '?' && !e.ctrlKey && !e.metaKey && !e.altKey) {
        e.preventDefault();
        toggle();
      }
    });
  };

  function buildInputMap(heading, items) {
    const rows = items.map(k => {
      const keyHtml = k.key.split(/\s*\+\s*/).map(part => {
        return `<span class="help-key">${esc(part)}</span>`;
      }).join(' + ');
      return `<tr><td>${keyHtml}</td><td>${esc(k.action)}</td></tr>`;
    }).join('');
    return `
      <div class="help-keymap">
        <h3>${heading}</h3>
        <table><thead><tr><th>Input</th><th>Action</th></tr></thead>
        <tbody>${rows}</tbody></table>
      </div>`;
  }

  function buildSection(s, idx) {
    const icon = s.icon || '';
    return `
      <div class="help-section${idx === 0 ? ' open' : ''}">
        <button class="help-section-toggle">
          ${icon ? `<span class="help-section-icon">${icon}</span>` : ''}${esc(s.title)}
        </button>
        <div class="help-section-body">${s.html}</div>
      </div>`;
  }

  function esc(s) {
    const el = document.createElement('span');
    el.textContent = s;
    return el.innerHTML;
  }
})();
