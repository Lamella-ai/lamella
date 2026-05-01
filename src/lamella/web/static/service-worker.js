// Copyright 2026 Lamella LLC
// SPDX-License-Identifier: Apache-2.0
//
// Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
// https://lamella.ai

/* Lamella offline shell.
 *
 * Scope: only /notes and /mileage are precached. The dashboard, review, and
 * everything else are explicitly *not* served from cache — they show
 * live state and would lie if cached. We pass network-first for any URL
 * outside the precache list, including HTML for /notes and /mileage when
 * online (so a deploy is picked up on the next online load).
 */

// Bump the cache name when changing the precache list or fetch
// behavior — `activate` deletes any cache whose key doesn't match,
// so the old SW's offline shell is purged on first run.
const CACHE = "lamella-shell-v4";
const PRECACHE = [
  "/notes",
  "/mileage",
  "/static/app.css",
  "/static/mobile.css",
  "/static/htmx.min.js",
  "/static/manifest.webmanifest",
  "/static/img/lamella-icon.svg",
  "/static/img/favicon.svg",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE).then((cache) =>
      Promise.all(
        PRECACHE.map((url) =>
          fetch(url, { credentials: "same-origin" })
            .then((response) => {
              if (response && response.ok) {
                return cache.put(url, response.clone());
              }
            })
            .catch(() => undefined),
        ),
      ),
    ),
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))),
    ),
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  const req = event.request;
  if (req.method !== "GET") return;
  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;
  // Never serve dashboard / review / API / webhook URLs from cache.
  const cacheable = PRECACHE.some((p) => url.pathname === p || url.pathname.startsWith(p + "/"));
  if (!cacheable) return;
  event.respondWith(
    fetch(req)
      .then((response) => {
        if (response && response.ok) {
          const copy = response.clone();
          caches.open(CACHE).then((cache) => cache.put(req, copy));
        }
        return response;
      })
      .catch(() => caches.match(req).then((cached) => cached || Response.error())),
  );
});
