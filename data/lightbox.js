/* ═══════════════════════════════════════════════════════════
   Lightbox — shared image preview widget
   Click any thumbnail → big preview overlay
   Click big preview   → opens full image in new tab
   Escape / click backdrop → close
   ═══════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  /* ── Inject CSS ── */
  const css = document.createElement('style');
  css.textContent = `
    .lb-overlay {
      position: fixed; inset: 0; z-index: 9000;
      background: rgba(0,0,0,0.82); backdrop-filter: blur(4px);
      display: flex; align-items: center; justify-content: center;
      opacity: 0; pointer-events: none; transition: opacity 0.2s;
      cursor: zoom-out;
    }
    .lb-overlay.open { opacity: 1; pointer-events: auto; }
    .lb-wrap {
      position: relative; max-width: 92vw; max-height: 90vh;
      display: flex; flex-direction: column; align-items: center;
    }
    .lb-img {
      max-width: 92vw; max-height: 82vh; object-fit: contain;
      border-radius: 6px; box-shadow: 0 8px 40px rgba(0,0,0,0.5);
      cursor: pointer; transition: transform 0.2s;
      background: #1a1a1a;
    }
    .lb-img:hover { transform: scale(1.01); }
    .lb-caption {
      margin-top: 0.6rem; color: #e8e4dc; font-size: 0.82rem;
      font-family: -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
      text-align: center; max-width: 80vw; line-height: 1.4;
    }
    .lb-hint {
      color: rgba(255,255,255,0.5); font-size: 0.7rem; margin-top: 0.25rem;
    }
    .lb-close {
      position: absolute; top: -2rem; right: -0.5rem;
      background: none; border: none; color: #fff; font-size: 1.6rem;
      cursor: pointer; opacity: 0.6; transition: opacity 0.15s;
      line-height: 1; padding: 0.3rem;
    }
    .lb-close:hover { opacity: 1; }
    @media (max-width: 600px) {
      .lb-img { max-width: 96vw; max-height: 75vh; border-radius: 4px; }
      .lb-caption { font-size: 0.75rem; }
    }
  `;
  document.head.appendChild(css);

  /* ── Inject overlay HTML ── */
  const overlay = document.createElement('div');
  overlay.className = 'lb-overlay';
  overlay.id = 'lb-overlay';
  overlay.innerHTML = `
    <div class="lb-wrap">
      <button class="lb-close" id="lb-close">&times;</button>
      <img class="lb-img" id="lb-img" src="" alt="">
      <div class="lb-caption" id="lb-caption"></div>
      <div class="lb-hint">Click image to open full size in new tab</div>
    </div>
  `;
  document.body.appendChild(overlay);

  const lbImg = document.getElementById('lb-img');
  const lbCaption = document.getElementById('lb-caption');

  /* ── Public API ── */
  window.openLightbox = function (src, title) {
    if (!src) return;
    lbImg.src = src;
    lbImg.alt = title || '';
    lbCaption.textContent = title || '';
    overlay.classList.add('open');
    document.body.style.overflow = 'hidden';
  };

  function closeLightbox() {
    overlay.classList.remove('open');
    document.body.style.overflow = '';
  }

  /* ── Click image → open in new tab ── */
  lbImg.addEventListener('click', (e) => {
    e.stopPropagation();
    window.open(lbImg.src, '_blank');
  });

  /* ── Close handlers ── */
  document.getElementById('lb-close').addEventListener('click', (e) => {
    e.stopPropagation();
    closeLightbox();
  });

  overlay.addEventListener('click', (e) => {
    if (e.target === overlay) closeLightbox();
  });

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && overlay.classList.contains('open')) {
      closeLightbox();
    }
  });

  /* ── Auto-attach to any img with data-lightbox ── */
  // Usage: <img src="..." data-lightbox data-lb-title="My Photo">
  // Also auto-attaches via MutationObserver for dynamically rendered images.
  function attachLightbox(img) {
    if (img._lbBound) return;
    img._lbBound = true;
    img.style.cursor = 'zoom-in';
    img.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      // Use data-lb-src for full-size URL, or data-lb-full, or fall back to src
      const src = img.dataset.lbSrc || img.dataset.lbFull || img.src;
      const title = img.dataset.lbTitle || img.alt || '';
      window.openLightbox(src, title);
    });
  }

  function scanImages(root) {
    (root || document).querySelectorAll('img[data-lightbox]').forEach(attachLightbox);
  }

  // Initial scan
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => scanImages());
  } else {
    scanImages();
  }

  // Watch for dynamically added images (gallery renders, etc.)
  const observer = new MutationObserver((mutations) => {
    for (const m of mutations) {
      for (const node of m.addedNodes) {
        if (node.nodeType !== 1) continue;
        if (node.matches && node.matches('img[data-lightbox]')) attachLightbox(node);
        if (node.querySelectorAll) node.querySelectorAll('img[data-lightbox]').forEach(attachLightbox);
      }
    }
  });
  observer.observe(document.body, { childList: true, subtree: true });

})();
