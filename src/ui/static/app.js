
function fmtTs(ts) {
  try {
    const d = new Date(ts.includes('T') ? ts : ts.replace(' ', 'T') + 'Z');
    return d.toLocaleString();
  } catch { return ts; }
}
function rowHtml(r) {
  const dec = String(r.aql_final_decision || '').toUpperCase();
  const badge = dec === 'PASS' ? 'text-bg-success' : (dec === 'FAIL' ? 'text-bg-danger' : 'text-bg-secondary');
  const overlay = r.overlay_url ? `<a href="${r.overlay_url}" target="_blank">Open</a>` : '-';
  return `
    <tr>
      <td>${fmtTs(r.ts)}</td>
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
document.addEventListener('DOMContentLoaded', startLive);
