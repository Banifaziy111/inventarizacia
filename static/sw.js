const CACHE = "inventory-v1";

self.addEventListener("install", (e) => {
  e.waitUntil(caches.open(CACHE).then(() => self.skipWaiting()));
});

self.addEventListener("activate", (e) => {
  e.waitUntil(
    caches.keys().then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
  );
  self.clients.claim();
});

self.addEventListener("fetch", (e) => {
  if (e.request.method !== "GET" || e.request.url.includes("/api/")) return;
  e.respondWith(
    fetch(e.request)
      .then((r) => {
        const clone = r.clone();
        if (r.ok && (r.type === "basic" || r.type === "")) {
          caches.open(CACHE).then((cache) => cache.put(e.request, clone));
        }
        return r;
      })
      .catch(() => caches.match(e.request).then((c) => c || fetch(e.request)))
  );
});
