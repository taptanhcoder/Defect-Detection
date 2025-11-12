// ----------- Utilities -----------
function fmtTs(ts) {
  try {
    const d = new Date(String(ts).includes('T') ? ts : (String(ts).replace(' ', 'T') + 'Z'));
    return d.toLocaleString();
  } catch { return ts; }
}
function qsGet(name) {
  const url = new URL(location.href);
  return url.searchParams.get(name) || "";
}

// ----------- Live table -----------
function rowHtml(r) {
  const dec = String(r.aql_final_decision || '').toUpperCase();
  const badge = dec === 'PASS' ? 'text-bg-success' : (dec === 'FAIL' ? 'text-bg-danger' : 'text-bg-secondary');
  const overlay = r.overlay_url ? `<a href="${r.overlay_url}" target="_blank">Open</a>` : (r.image_overlay_url ? `<a href="${r.image_overlay_url}" target="_blank">Open</a>` : '-');
  return `
    <tr>
      <td>${fmtTs(r.ts || r.ts_ms)}</td>
      <td>${r.product_code ?? ''}</td>
      <td>${r.station_id ?? ''}</td>
      <td><span class="badge ${badge}">${dec || '-'}</span></td>
      <td>${r.defect_count ?? ''}</td>
      <td>${overlay}</td>
      <td><a href="/inspections/${r.event_id}">View</a></td>
    </tr>`;
}
async function pollLiveOnce() {
  const statusEl = document.getElementById('live-status');
  const bodyEl = document.getElementById('live-body');
  if (!statusEl || !bodyEl) return;

  try {
    statusEl.textContent = 'Loading…';
    const url = new URL(location.origin + '/api/recent-proxy' + location.search);
    url.searchParams.set('limit', '20');
    const res = await fetch(url, { cache: 'no-store' });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    const items = Array.isArray(data.items) ? data.items : [];
    if (!items.length) {
      bodyEl.innerHTML = `<tr><td colspan="7" class="text-secondary">Không có dữ liệu.</td></tr>`;
    } else {
      bodyEl.innerHTML = items.map(rowHtml).join('');
    }
    statusEl.textContent = 'OK';
  } catch (e) {
    statusEl.textContent = 'Lỗi tải dữ liệu (tự thử lại)…';
  }
}
function startLive() {
  if (!document.getElementById('live-body')) return;
  pollLiveOnce();
  window.__liveTimer && clearInterval(window.__liveTimer);
  window.__liveTimer = setInterval(pollLiveOnce, 5000);
}

// ----------- Defect Gallery -----------
function galleryCardHtml(item) {
  const ts = fmtTs(item.ts || item.ts_ms);
  const dec = String(item.aql_final_decision || 'FAIL').toUpperCase();
  const badge = dec === 'PASS' ? 'text-bg-success' : (dec === 'FAIL' ? 'text-bg-danger' : 'text-bg-secondary');
  const thumb = item.overlay_url || item.image_overlay_url || ''; // may be empty (need presign)
  const viewLink = `/inspections/${item.event_id}`;
  const product = item.product_code || '';
  const station = item.station_id || '';
  const defCnt = item.defect_count ?? 0;

  // placeholder image if missing; will be swapped after presign
  const imgTag = thumb
    ? `<img src="${thumb}" alt="overlay">`
    : `<div class="text-secondary">No image</div>`;

  return `
  <div class="gallery-card" data-event="${item.event_id}" data-key="${item.overlay_key || ''}">
    <div class="thumb-wrap">${imgTag}</div>
    <div class="meta d-flex justify-content-between align-items-center">
      <div>
        <div class="small">${ts}</div>
        <div class="small">${product} • ${station}</div>
        <div class="small">defects: ${defCnt}</div>
      </div>
      <div>
        <span class="badge ${badge}">${dec}</span>
        <a href="${viewLink}" class="btn btn-sm btn-outline-light ms-2">View</a>
      </div>
    </div>
  </div>`;
}

async function fetchDefectsAndRender() {
  const grid = document.getElementById('gallery-grid');
  const statusEl = document.getElementById('gallery-status');
  if (!grid || !statusEl) return;

  const product = qsGet('product') || '';
  const station = qsGet('station') || '';
  const params = new URLSearchParams({
    decision: 'FAIL',
    product: product || '',
    station: station || '',
    limit: '30',
    page: '1'
  });

  try {
    statusEl.textContent = 'Loading…';
    const res = await fetch('/api/defects?' + params.toString(), { cache: 'no-store' });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    const items = Array.isArray(data.items) ? data.items : [];
    if (!items.length) {
      grid.innerHTML = `<div class="text-secondary">Không có lỗi nào trong khoảng thời gian đã chọn.</div>`;
      statusEl.textContent = 'OK';
      return;
    }
    grid.innerHTML = items.map(galleryCardHtml).join('');
    statusEl.textContent = `OK (${items.length})`;

    // Attempt presign for cards that lack an image URL but have a key
    const cards = Array.from(grid.querySelectorAll('.gallery-card'));
    for (const card of cards) {
      const img = card.querySelector('img');
      if (img && img.getAttribute('src')) continue;
      const key = card.getAttribute('data-key');
      if (!key) continue;
      try {
        const ps = await fetch('/api/presign?key=' + encodeURIComponent(key));
        if (ps.ok) {
          const data = await ps.json();
          const url = data.url || data.overlay_url || '';
          if (url) {
            const wrap = card.querySelector('.thumb-wrap');
            wrap.innerHTML = `<img src="${url}" alt="overlay">`;
          }
        }
      } catch {}
    }
  } catch (e) {
    statusEl.textContent = 'Lỗi tải dữ liệu';
    grid.innerHTML = `<div class="text-danger small">${String(e)}</div>`;
  }
}

// ----------- Boot -----------
document.addEventListener('DOMContentLoaded', () => {
  startLive();
  if (document.getElementById('gallery-grid')) {
    fetchDefectsAndRender();
  }
});
