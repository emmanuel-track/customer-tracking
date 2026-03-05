const CACHE = 'fuel-tracker-v3';
const ASSETS = [
  '/emmanuel123/',
  '/emmanuel123/index.html',
  '/emmanuel123/styles.css',
  '/emmanuel123/app.js',
];

// Install — cache core assets
self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE)
      .then(c => c.addAll(ASSETS))
      .then(() => self.skipWaiting())
  );
});

// Activate — clean old caches
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(
        keys.filter(k => k !== CACHE).map(k => caches.delete(k))
      ))
      .then(() => self.clients.claim())
  );
});

// Fetch — network first, cache fallback
self.addEventListener('fetch', e => {
  if (e.request.method !== 'GET') return;
  if (!e.request.url.startsWith(self.location.origin)) return;

  e.respondWith(
    fetch(e.request)
      .then(res => {
        // Cache successful responses for core assets
        const url = e.request.url;
        const isCoreAsset = ASSETS.some(a => url.endsWith(a)) ||
          e.request.destination === 'document';
        if (res.ok && isCoreAsset) {
          const clone = res.clone();
          caches.open(CACHE).then(c => c.put(e.request, clone));
        }
        return res;
      })
      .catch(() =>
        caches.match(e.request)
          .then(r => r || caches.match('/emmanuel123/'))
      )
  );
});
