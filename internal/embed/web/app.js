const api = {
  health: '/api/v1/health',
  episodes: '/api/v1/anime/21/episodes',
  airing: '/api/v1/anilist',
};

const airingQuery = `query {
  Page(page: 1, perPage: 20) {
    media(type: ANIME, status: RELEASING, sort: POPULARITY_DESC) {
      id
      title { romaji english userPreferred }
      coverImage { large medium }
      format
      airingSchedule(notYetAired: true) {
        nodes { episode airingAt timeUntilAiring }
      }
    }
  }
}`;

const endpointFilter = document.querySelector('#endpoint-filter');
const endpointCards = [...document.querySelectorAll('.endpoint-card')];
const emptyState = document.querySelector('#filter-empty');
const scheduleList = document.querySelector('#schedule-list');
const dateStrip = document.querySelector('#date-strip');
const healthStatus = document.querySelector('#service-health');
const trendingGrid = document.querySelector('#trending-grid');
const trendingCount = document.querySelector('#trending-count');

const titleOf = (item) => item?.title?.english || item?.title?.romaji || item?.title?.userPreferred || 'Untitled record';
const coverOf = (item) => item?.coverImage?.large || item?.coverImage?.medium || item?.coverImage?.extraLarge || '';
const timeFormat = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit' });
const dateLabel = new Intl.DateTimeFormat(undefined, { weekday: 'short', month: 'short', day: 'numeric' });
const localDateKey = (value) => {
  const date = value instanceof Date ? value : new Date(value * 1000);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
};

function escapeHtml(value) {
  return String(value).replace(/[&<>'"]/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[char]));
}

function flattenAiring(payload) {
  const media = Array.isArray(payload?.data?.Page?.media) ? payload.data.Page.media : [];
  const now = Math.floor(Date.now() / 1000);
  const seen = new Set();
  return media.flatMap((record) => (record?.airingSchedule?.nodes || []).map((node) => ({
    id: record.id,
    title: record.title,
    coverImage: record.coverImage,
    format: record.format,
    episode: node.episode,
    airingAt: node.airingAt,
    timeUntilAiring: node.timeUntilAiring,
  }))).filter((item) => item.airingAt >= now - 60 && !seen.has(`${item.id}:${item.episode}`) && seen.add(`${item.id}:${item.episode}`)).sort((left, right) => left.airingAt - right.airingAt).slice(0, 48);
}

function formatRelative(timestamp) {
  const seconds = Math.max(0, timestamp - Math.floor(Date.now() / 1000));
  if (seconds < 3600) return `in ${Math.max(1, Math.round(seconds / 60))}m`;
  if (seconds < 86400) return `in ${Math.round(seconds / 3600)}h`;
  return `in ${Math.round(seconds / 86400)}d`;
}

function renderDateStrip(dates, selectedDate) {
  dateStrip.innerHTML = dates.map((key) => {
    const [year, month, day] = key.split('-').map(Number);
    const date = new Date(year, month - 1, day);
    const isActive = key === selectedDate;
    return `<button type="button" role="tab" aria-selected="${isActive}" data-date="${key}">${escapeHtml(dateLabel.format(date))}</button>`;
  }).join('');
  dateStrip.querySelectorAll('button').forEach((button) => button.addEventListener('click', () => renderSchedule(window.__anirakuAiring || [], button.dataset.date)));
}

function renderSchedule(items, requestedDate) {
  const dates = [...new Set(items.map((item) => localDateKey(item.airingAt)))].sort();
  const selectedDate = dates.includes(requestedDate) ? requestedDate : dates[0];
  renderDateStrip(dates, selectedDate);
  const matching = items.filter((item) => localDateKey(item.airingAt) === selectedDate).slice(0, 8);
  scheduleList.innerHTML = matching.map((item) => {
    const airing = new Date(item.airingAt * 1000);
    const cover = coverOf(item);
    return `<a class="schedule-item" href="/api/v1/anime/${encodeURIComponent(item.id)}" target="_blank" rel="noreferrer"><time datetime="${airing.toISOString()}">${escapeHtml(timeFormat.format(airing))}</time>${cover ? `<img src="${escapeHtml(cover)}" alt="" loading="lazy" />` : '<span></span>'}<div><strong>${escapeHtml(titleOf(item))}</strong><small>${escapeHtml(item.format || 'ANIME')} · Episode ${escapeHtml(item.episode || '—')}</small></div><em>${escapeHtml(formatRelative(item.airingAt))}</em></a>`;
  }).join('');
}

function renderTrending(items) {
  if (!items.length) {
    trendingGrid.innerHTML = '<div class="trend-fallback">Episode data is temporarily unavailable. The endpoint directory remains usable.</div>';
    trendingCount.textContent = 'Unavailable';
    return;
  }
  trendingCount.textContent = `${items.length} episodes`;
  trendingGrid.innerHTML = items.slice(0, 8).map((item) => {
    const thumb = item.thumbnail || coverOf(item) || '';
    const title = item.title || `Episode ${item.number}`;
    return `<a class="trend-card" href="/api/v1/anime/21/episodes" target="_blank" rel="noreferrer">${thumb ? `<img src="${escapeHtml(thumb)}" alt="" loading="lazy" />` : '<div class="trend-fallback">No thumbnail</div>'}<div><strong>${escapeHtml(title)}</strong><small>Episode ${escapeHtml(item.number)}${item.airdate ? ' · ' + escapeHtml(item.airdate) : ''}</small></div></a>`;
  }).join('');
}

async function loadPublicData() {
  try {
    const response = await fetch(api.health, { headers: { Accept: 'application/json' } });
    const data = await response.json();
    if (!response.ok || data?.status !== 'ok') throw new Error('Health check was not ready');
    healthStatus.classList.add('is-online');
    healthStatus.querySelector('span:last-child').textContent = 'Service online';
  } catch {
    healthStatus.classList.add('is-error');
    healthStatus.querySelector('span:last-child').textContent = 'Health check unavailable';
  }

  try {
    const response = await fetch(api.episodes, { headers: { Accept: 'application/json' } });
    if (!response.ok) throw new Error('Episodes request failed');
    const data = await response.json();
    const items = Array.isArray(data?.episodes) ? data.episodes : [];
    renderTrending(items);
  } catch {
    renderTrending([]);
  }

  try {
    const response = await fetch(api.airing, { method: 'POST', headers: { 'content-type': 'application/json', accept: 'application/json' }, body: JSON.stringify({ query: airingQuery }) });
    if (!response.ok) throw new Error('Airing request failed');
    window.__anirakuAiring = flattenAiring(await response.json());
    if (!window.__anirakuAiring.length) throw new Error('Airing response contained no upcoming entries');
    renderSchedule(window.__anirakuAiring);
  } catch {
    dateStrip.innerHTML = '';
    scheduleList.innerHTML = '<p class="error-message">The public airing feed is unavailable right now. Please try again shortly; no schedule data is shown until the backend can provide real entries.</p>';
  }
}

endpointFilter.addEventListener('input', () => {
  const term = endpointFilter.value.trim().toLowerCase();
  let shown = 0;
  endpointCards.forEach((card) => {
    const visible = !term || card.dataset.search.includes(term) || card.textContent.toLowerCase().includes(term);
    card.hidden = !visible;
    if (visible) shown += 1;
  });
  emptyState.hidden = shown !== 0;
});

document.querySelector('#copy-request').addEventListener('click', async (event) => {
  const button = event.currentTarget;
  try {
    await navigator.clipboard.writeText('GET https://api.aniraku.tech/api/v1/anime/21/episodes');
    button.textContent = 'COPIED';
  } catch {
    button.textContent = 'SELECT';
  }
  window.setTimeout(() => { button.textContent = 'COPY'; }, 1400);
});

loadPublicData();
