const ANIMATE_SELECTORS = [
    ".card",
    ".work-block",
    ".admin-kpi-card",
    ".admin-widget",
    ".progress-card",
    ".hero-card",
    ".place-card",
    ".photo-card",
    ".notification-panel",
    ".admin-activity-item",
];
let animationObserver = null;

const PLACE_CACHE_KEY = "inventory-mx-cache";
const PLACE_CACHE_TTL_MS = 30 * 24 * 60 * 60 * 1000; // 30 дней
const PLACE_CACHE_MAX = 6000;
const PLACE_CACHE_FLUSH_DEBOUNCE_MS = 1500;
const PLACE_IDB_NAME = "inventory-offline-db";
const PLACE_IDB_STORE = "places";
const PLACE_IDB_VERSION = 1;
const SYNC_CURSOR_KEY = "inventory-sync-cursor";
const SYNC_LAST_AT_KEY = "inventory-sync-last-at";
const SYNC_LAST_BLOCKS_KEY = "inventory-sync-last-blocks";
const SYNC_LAST_FLOORS_KEY = "inventory-sync-last-floors";
const SYNC_META_CACHE_TTL_MS = 5 * 60 * 1000;
const OFFLINE_QUEUE_KEY = "inventory-offline-queue";
const OFFLINE_RETRY_BASE_MS = 5000;
const OFFLINE_RETRY_MAX_MS = 120000;
const OFFLINE_AUTO_SYNC_INTERVAL_MS = 12000;
const API_REQUEST_TIMEOUT_MS = 9000;
const API_SCAN_COMPLETE_TIMEOUT_MS = 2500;

// Ограничение размера фото для отправки (Vercel лимит тела запроса 4.5 MB)
const MAX_PHOTO_DIMENSION = 1280;
const PHOTO_JPEG_QUALITY = 0.82;
const MAX_PHOTO_BASE64_BYTES = 3 * 1024 * 1024; // ~3 MB на фото, чтобы несколько фото + JSON укладывались в 4.5 MB

let _placeCacheStore = null;
let _placeCacheDirty = false;
let _placeCacheFlushTimer = null;
let _placeIdbPromise = null;
let _placeIdbWriteTail = Promise.resolve();

function normalizePlaceCacheStore(store) {
    const src = store && typeof store === "object" ? store : {};
    const items = src.items && typeof src.items === "object" ? src.items : {};
    const order = Array.isArray(src.order) ? src.order : Object.keys(items);
    return { items, order };
}

function backfillPlaceCacheAliases(store) {
    const safeStore = normalizePlaceCacheStore(store);
    const orderSet = new Set(safeStore.order || []);
    let changed = false;
    Object.entries(safeStore.items || {}).forEach(([existingKey, entry]) => {
        const data = entry?.data;
        if (!data) return;
        const nowTs = Number(entry?.ts) || Date.now();
        const keyNorm = String(existingKey || "").trim().toUpperCase();
        const numId = data.place_cod != null ? String(data.place_cod).trim().toUpperCase() : "";
        const strCode = data.place_name ? String(data.place_name).trim().toUpperCase() : "";
        [numId, strCode].forEach((alias) => {
            if (!alias || alias === keyNorm || safeStore.items[alias]) return;
            safeStore.items[alias] = { data, ts: nowTs };
            if (!orderSet.has(alias)) {
                safeStore.order.push(alias);
                orderSet.add(alias);
            }
            changed = true;
        });
    });
    while (safeStore.order.length > PLACE_CACHE_MAX) {
        const old = safeStore.order.shift();
        if (old) {
            delete safeStore.items[old];
            changed = true;
        }
    }
    return { store: safeStore, changed };
}

function loadPlaceCacheStore() {
    if (_placeCacheStore) return _placeCacheStore;
    try {
        const raw = localStorage.getItem(PLACE_CACHE_KEY);
        const normalized = normalizePlaceCacheStore(raw ? JSON.parse(raw) : null);
        const backfilled = backfillPlaceCacheAliases(normalized);
        _placeCacheStore = backfilled.store;
        if (backfilled.changed) {
            try {
                localStorage.setItem(PLACE_CACHE_KEY, JSON.stringify(_placeCacheStore));
            } catch (_) {}
        }
    } catch {
        _placeCacheStore = { items: {}, order: [] };
    }
    return _placeCacheStore;
}

function persistPlaceCacheStore() {
    if (!_placeCacheStore) return;
    try {
        localStorage.setItem(PLACE_CACHE_KEY, JSON.stringify(_placeCacheStore));
        _placeCacheDirty = false;
    } catch (e) {
        console.warn("PlaceCache persist error", e);
    }
}

function schedulePlaceCacheFlush() {
    _placeCacheDirty = true;
    if (_placeCacheFlushTimer != null) return;
    _placeCacheFlushTimer = window.setTimeout(() => {
        _placeCacheFlushTimer = null;
        if (_placeCacheDirty) persistPlaceCacheStore();
    }, PLACE_CACHE_FLUSH_DEBOUNCE_MS);
}

function buildPlaceLookupVariants(rawKey) {
    const variants = new Set();
    const pushVariant = (v) => {
        const s = String(v || "").trim().toUpperCase();
        if (s) variants.add(s);
    };
    const base = String(rawKey || "").trim().toUpperCase();
    pushVariant(base);
    const plceMatch = base.match(/^PLCE\s*(.+)$/i);
    if (plceMatch) pushVariant(plceMatch[1]);
    pushVariant(base.replace(/\s+/g, ""));
    if (/^\d+$/.test(base)) pushVariant(base.replace(/^0+/, "") || "0");
    return Array.from(variants);
}

function openPlaceIdb() {
    if (_placeIdbPromise) return _placeIdbPromise;
    _placeIdbPromise = new Promise((resolve) => {
        if (typeof indexedDB === "undefined") {
            resolve(null);
            return;
        }
        const req = indexedDB.open(PLACE_IDB_NAME, PLACE_IDB_VERSION);
        req.onupgradeneeded = () => {
            const db = req.result;
            if (!db.objectStoreNames.contains(PLACE_IDB_STORE)) {
                db.createObjectStore(PLACE_IDB_STORE, { keyPath: "key" });
            }
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => {
            console.warn("IndexedDB open error", req.error);
            resolve(null);
        };
    });
    return _placeIdbPromise;
}

function idbRecordsFromPlaceRow(row) {
    if (!row || typeof row !== "object") return [];
    const records = [];
    const now = Date.now();
    const pushRecord = (key) => {
        const normalized = String(key || "").trim().toUpperCase();
        if (!normalized) return;
        records.push({ key: normalized, data: row, ts: now });
    };
    pushRecord(row.place_cod);
    pushRecord(row.place_name);
    return records;
}

function queuePlaceRowsForIdb(rows) {
    if (!Array.isArray(rows) || rows.length === 0) return;
    _placeIdbWriteTail = _placeIdbWriteTail
        .then(async () => {
            const db = await openPlaceIdb();
            if (!db) return;
            const records = rows.flatMap(idbRecordsFromPlaceRow);
            if (!records.length) return;
            await new Promise((resolve) => {
                const tx = db.transaction(PLACE_IDB_STORE, "readwrite");
                const store = tx.objectStore(PLACE_IDB_STORE);
                records.forEach((rec) => store.put(rec));
                tx.oncomplete = () => resolve();
                tx.onerror = () => resolve();
                tx.onabort = () => resolve();
            });
        })
        .catch(() => {});
}

async function lookupPlaceInIdb(rawKey) {
    const db = await openPlaceIdb();
    if (!db) return null;
    const variants = buildPlaceLookupVariants(rawKey);
    for (const key of variants) {
        // eslint-disable-next-line no-await-in-loop
        const result = await new Promise((resolve) => {
            const tx = db.transaction(PLACE_IDB_STORE, "readonly");
            const req = tx.objectStore(PLACE_IDB_STORE).get(key);
            req.onsuccess = () => resolve(req.result?.data || null);
            req.onerror = () => resolve(null);
        });
        if (result) return result;
    }
    return null;
}

window.addEventListener("beforeunload", () => {
    if (_placeCacheDirty) persistPlaceCacheStore();
});

const PlaceCache = {
    get(key) {
        try {
            const store = loadPlaceCacheStore();
            const k = String(key).trim().toUpperCase();
            const entry = store.items?.[k];
            if (!entry || !entry.data) return null;
            if (Date.now() - (entry.ts || 0) > PLACE_CACHE_TTL_MS) {
                delete store.items[k];
                const idx = store.order.indexOf(k);
                if (idx >= 0) store.order.splice(idx, 1);
                schedulePlaceCacheFlush();
                return null;
            }
            return entry.data;
        } catch {
            return null;
        }
    },
    set(key, data) {
        try {
            const store = loadPlaceCacheStore();
            const k = String(key).trim().toUpperCase();
            const now = Date.now();
            const orderSet = new Set(store.order);
            store.items[k] = { data, ts: now };
            if (!orderSet.has(k)) {
                store.order.push(k);
                orderSet.add(k);
            }
            const numId = data.place_cod != null ? String(data.place_cod) : null;
            const strCode = data.place_name ? String(data.place_name).trim().toUpperCase() : null;
            if (numId && numId !== k) {
                store.items[numId] = { data, ts: now };
                if (!orderSet.has(numId)) {
                    store.order.push(numId);
                    orderSet.add(numId);
                }
            }
            if (strCode && strCode !== k && strCode !== numId) {
                store.items[strCode] = { data, ts: now };
                if (!orderSet.has(strCode)) {
                    store.order.push(strCode);
                    orderSet.add(strCode);
                }
            }
            while (store.order.length > PLACE_CACHE_MAX) {
                const old = store.order.shift();
                if (old) delete store.items[old];
            }
            schedulePlaceCacheFlush();
            queuePlaceRowsForIdb([data]);
        } catch (e) {
            console.warn("PlaceCache set error", e);
        }
    },
    setMany(rows) {
        try {
            if (!Array.isArray(rows) || rows.length === 0) return;
            const store = loadPlaceCacheStore();
            const now = Date.now();
            const orderSet = new Set(store.order);
            const pushKey = (k, data) => {
                if (!k) return;
                const key = String(k).trim().toUpperCase();
                if (!key) return;
                store.items[key] = { data, ts: now };
                if (!orderSet.has(key)) {
                    store.order.push(key);
                    orderSet.add(key);
                }
            };
            rows.forEach((row) => {
                if (!row) return;
                const numId = row.place_cod != null ? String(row.place_cod) : "";
                const strCode = row.place_name ? String(row.place_name).trim().toUpperCase() : "";
                // Для офлайн-работы поддерживаем оба варианта ввода:
                // numeric place_cod (сканеры/QR) и string place_name (ручной ввод).
                if (strCode) pushKey(strCode, row);
                if (numId) pushKey(numId, row);
            });
            while (store.order.length > PLACE_CACHE_MAX) {
                const old = store.order.shift();
                if (old) delete store.items[old];
            }
            schedulePlaceCacheFlush();
            queuePlaceRowsForIdb(rows);
        } catch (e) {
            console.warn("PlaceCache setMany error", e);
        }
    },
    flush() {
        if (_placeCacheFlushTimer != null) {
            clearTimeout(_placeCacheFlushTimer);
            _placeCacheFlushTimer = null;
        }
        if (_placeCacheDirty) persistPlaceCacheStore();
    },
    size() {
        try {
            const store = loadPlaceCacheStore();
            return Array.isArray(store?.order) ? store.order.length : 0;
        } catch {
            return 0;
        }
    },
    lookup(rawKey) {
        try {
            const variants = new Set(buildPlaceLookupVariants(rawKey));

            for (const key of variants) {
                const item = this.get(key);
                if (item) return item;
            }

            const store = loadPlaceCacheStore();
            const byValues = Object.values(store.items || {});
            for (const entry of byValues) {
                const data = entry?.data;
                if (!data) continue;
                const code = String(data.place_cod ?? "").trim().toUpperCase();
                const name = String(data.place_name ?? "").trim().toUpperCase();
                const codeNoSpaces = code.replace(/\s+/g, "");
                const nameNoSpaces = name.replace(/\s+/g, "");
                const codeNoZeros = /^\d+$/.test(code) ? (code.replace(/^0+/, "") || "0") : "";
                if (variants.has(code) || variants.has(name) || variants.has(codeNoSpaces) || variants.has(nameNoSpaces) || (codeNoZeros && variants.has(codeNoZeros))) {
                    return data;
                }
            }
            return null;
        } catch {
            return null;
        }
    },
    async lookupAsync(rawKey) {
        const local = this.lookup(rawKey);
        if (local) return local;
        return await lookupPlaceInIdb(rawKey);
    },
};

const SoundFeedback = {
    playSuccess() {
        try {
            const ctx = new (window.AudioContext || window.webkitAudioContext)();
            const osc = ctx.createOscillator();
            const gain = ctx.createGain();
            osc.connect(gain);
            gain.connect(ctx.destination);
            osc.frequency.value = 800;
            osc.type = "sine";
            gain.gain.setValueAtTime(0.15, ctx.currentTime);
            gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.15);
            osc.start(ctx.currentTime);
            osc.stop(ctx.currentTime + 0.15);
        } catch (_) {}
    },
    playError() {
        try {
            const ctx = new (window.AudioContext || window.webkitAudioContext)();
            const osc = ctx.createOscillator();
            const gain = ctx.createGain();
            osc.connect(gain);
            gain.connect(ctx.destination);
            osc.frequency.value = 300;
            osc.type = "sawtooth";
            gain.gain.setValueAtTime(0.1, ctx.currentTime);
            gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.2);
            osc.start(ctx.currentTime);
            osc.stop(ctx.currentTime + 0.2);
        } catch (_) {}
    },
};

function makeOfflineQueueId() {
    return `q_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

function computeRetryDelayMs(attempts) {
    const safeAttempts = Math.max(1, Number(attempts) || 1);
    const exp = Math.min(OFFLINE_RETRY_MAX_MS, OFFLINE_RETRY_BASE_MS * Math.pow(2, safeAttempts - 1));
    const jitter = Math.round(exp * (0.25 * Math.random()));
    return Math.min(OFFLINE_RETRY_MAX_MS, exp + jitter);
}

function normalizeQueueItem(item) {
    const src = item || {};
    const body = src.body && typeof src.body === "object" ? src.body : {};
    const attempts = Number.isFinite(Number(src.attempts)) ? Math.max(0, Number(src.attempts)) : 0;
    const status = src.status === "conflict" ? "conflict" : "pending";
    return {
        qid: src.qid || makeOfflineQueueId(),
        path: src.path || "/api/scan/complete",
        body,
        ts: Number.isFinite(Number(src.ts)) ? Number(src.ts) : Date.now(),
        attempts,
        lastError: src.lastError ? String(src.lastError) : "",
        nextRetryAt: Number.isFinite(Number(src.nextRetryAt)) ? Number(src.nextRetryAt) : 0,
        status,
    };
}

function isRetryableScanError(status, data = null) {
    if (status >= 500) return true;
    if (status === 408 || status === 425 || status === 429) return true;
    if (status === 0) return true;
    if (data?.code === "duplicate_in_shift" || data?.confirm_required) return false;
    return false;
}

function parseQueueResponseError(data, fallback) {
    const msg = data?.error || data?.message || fallback;
    return String(msg || "Ошибка отправки");
}

const OfflineQueue = {
    push(payload) {
        try {
            const raw = localStorage.getItem(OFFLINE_QUEUE_KEY);
            const queue = raw ? JSON.parse(raw) : [];
            const item = normalizeQueueItem(payload);
            queue.push(item);
            localStorage.setItem(OFFLINE_QUEUE_KEY, JSON.stringify(queue));
            return { ok: true, length: queue.length, item };
        } catch (e) {
            return { ok: false, length: 0, error: e };
        }
    },
    getAll() {
        try {
            const raw = localStorage.getItem(OFFLINE_QUEUE_KEY);
            const parsed = raw ? JSON.parse(raw) : [];
            const normalized = Array.isArray(parsed) ? parsed.map(normalizeQueueItem) : [];
            if (JSON.stringify(parsed) !== JSON.stringify(normalized)) {
                localStorage.setItem(OFFLINE_QUEUE_KEY, JSON.stringify(normalized));
            }
            return normalized;
        } catch {
            return [];
        }
    },
    clear() {
        localStorage.removeItem(OFFLINE_QUEUE_KEY);
    },
    set(items) {
        try {
            const normalized = Array.isArray(items) ? items.map(normalizeQueueItem) : [];
            localStorage.setItem(OFFLINE_QUEUE_KEY, JSON.stringify(normalized));
            return true;
        } catch {
            return false;
        }
    },
};

function enqueueScanForSync({ path = "/api/scan/complete", body = {}, reason = "Нет связи" } = {}) {
    const pushResult = OfflineQueue.push({
        path,
        body,
        attempts: 0,
        lastError: String(reason || ""),
        nextRetryAt: Date.now(),
        status: "pending",
    });
    if (!pushResult.ok) {
        return {
            ok: false,
            message: "Не удалось сохранить в офлайн-очередь. Проверьте свободное место в браузере.",
            error: pushResult.error,
            length: 0,
        };
    }
    return {
        ok: true,
        message: "Сохранено в очередь. Синхронизация выполнится при появлении сети.",
        length: pushResult.length,
    };
}

let _offlineSyncInFlight = null;

async function syncOfflineQueue() {
    if (_offlineSyncInFlight) return _offlineSyncInFlight;
    _offlineSyncInFlight = (async () => {
        const queue = OfflineQueue.getAll();
        if (!queue.length || !navigator.onLine) return { sent: 0, left: queue.length, conflicts: queue.filter((x) => x.status === "conflict").length };
        const nowTs = Date.now();
        const remaining = [];
        let sent = 0;
        for (const item of queue) {
            if (item.status !== "conflict" && item.nextRetryAt && item.nextRetryAt > nowTs) {
                remaining.push(item);
                continue;
            }
            try {
                const res = await fetch("/api/scan/complete", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    credentials: "include",
                    body: JSON.stringify(item.body || (item.place_cod ? item : item.body) || {}),
                });
                const data = await res.json().catch(() => ({}));
                if (res.ok && !data.error) {
                    sent += 1;
                    continue;
                }
                const status = Number(res.status) || 0;
                const retryable = isRetryableScanError(status, data);
                const attempts = (item.attempts || 0) + 1;
                if (retryable) {
                    remaining.push({
                        ...item,
                        attempts,
                        status: "pending",
                        lastError: parseQueueResponseError(data, `HTTP ${status}`),
                        nextRetryAt: Date.now() + computeRetryDelayMs(attempts),
                    });
                } else {
                    remaining.push({
                        ...item,
                        attempts,
                        status: "conflict",
                        lastError: parseQueueResponseError(data, `HTTP ${status}`),
                        nextRetryAt: 0,
                    });
                }
            } catch (_) {
                const attempts = (item.attempts || 0) + 1;
                remaining.push({
                    ...item,
                    attempts,
                    status: "pending",
                    lastError: "Ошибка сети при отправке",
                    nextRetryAt: Date.now() + computeRetryDelayMs(attempts),
                });
            }
        }

        // merge-safe: учитываем новые элементы, добавленные пока шёл sync
        const latest = OfflineQueue.getAll();
        const sentSet = new Set(queue.map((x) => x.qid));
        const newItems = latest.filter((x) => !sentSet.has(x.qid));
        const merged = [...remaining, ...newItems];

        if (merged.length === 0) {
            OfflineQueue.clear();
        } else {
            OfflineQueue.set(merged);
        }
        return { sent, left: merged.length, conflicts: merged.filter((x) => x.status === "conflict").length };
    })();
    try {
        return await _offlineSyncInFlight;
    } finally {
        _offlineSyncInFlight = null;
    }
}

const API = {
    async request(path, options = {}) {
        const config = {
            headers: {
                "Content-Type": "application/json",
                ...(options.headers || {}),
            },
            credentials: "include",
            ...options,
        };

        if (config.body && typeof config.body !== "string") {
            config.body = JSON.stringify(config.body);
        }
        const timeoutMs = path.includes("/api/scan/complete")
            ? API_SCAN_COMPLETE_TIMEOUT_MS
            : API_REQUEST_TIMEOUT_MS;
        const ownController = !config.signal && typeof AbortController !== "undefined";
        const controller = ownController ? new AbortController() : null;
        let timeoutId = null;
        if (controller) {
            config.signal = controller.signal;
            timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);
        }

        try {
            const response = await fetch(path, config);
            const data = await response.json().catch(() => ({}));
            const isScanCompletePost = path.includes("/api/scan/complete") && (options.method === "POST" || config.method === "POST");
            if (!response.ok && isScanCompletePost && isRetryableScanError(response.status, data)) {
                try {
                    const body = typeof config.body === "string" ? JSON.parse(config.body) : {};
                    const queued = enqueueScanForSync({
                        path,
                        body,
                        reason: parseQueueResponseError(data, `HTTP ${response.status}`),
                    });
                    if (queued.ok) {
                        return {
                            ok: false,
                            status: response.status,
                            data: {
                                ...(data || {}),
                                queued: true,
                                error: queued.message,
                                original_error: parseQueueResponseError(data, `HTTP ${response.status}`),
                            },
                        };
                    }
                    return {
                        ok: false,
                        status: response.status,
                        data: {
                            ...(data || {}),
                            queued: false,
                            queue_write_failed: true,
                            error: queued.message,
                            original_error: parseQueueResponseError(data, `HTTP ${response.status}`),
                        },
                    };
                } catch (_) {}
            }

            return {
                ok: response.ok,
                status: response.status,
                data,
            };
        } catch (error) {
            if (path.includes("/api/scan/complete") && (options.method === "POST" || config.method === "POST")) {
                try {
                    const body = typeof config.body === "string" ? JSON.parse(config.body) : {};
                    const queued = enqueueScanForSync({ path, body, reason: String(error?.message || "Ошибка сети") });
                    return {
                        ok: false,
                        status: 0,
                        data: queued.ok
                            ? { queued: true, error: queued.message }
                            : { queued: false, queue_write_failed: true, error: queued.message },
                    };
                } catch (_) {}
            }
            console.error("API error", error);
            return {
                ok: false,
                status: 0,
                data: { error: "Не удалось связаться с сервером" },
            };
        } finally {
            if (timeoutId != null) window.clearTimeout(timeoutId);
        }
    },

    get(path) {
        return this.request(path, { method: "GET" });
    },

    post(path, body) {
        return this.request(path, { method: "POST", body });
    },

    delete(path) {
        return this.request(path, { method: "DELETE" });
    },
};

async function fetchJsonWithTimeout(path, timeoutMs = 4500) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
        const res = await fetch(path, { method: "GET", credentials: "include", signal: controller.signal });
        const data = await res.json().catch(() => ({}));
        return { ok: res.ok, status: res.status, data };
    } catch (error) {
        return { ok: false, status: 0, data: {}, error };
    } finally {
        clearTimeout(timer);
    }
}

async function getConnectivitySnapshot() {
    if (!navigator.onLine) {
        return { offline: true, serverReachable: false, dbWritable: false };
    }
    const ping = await fetchJsonWithTimeout("/api/ping", 3500);
    if (!ping.ok || !ping.data?.ok) {
        return { offline: false, serverReachable: false, dbWritable: false };
    }
    const health = await fetchJsonWithTimeout("/api/health", 4500);
    return {
        offline: false,
        serverReachable: true,
        dbWritable: !!(health.ok && health.data?.ok),
    };
}

function showAlert(element, message, type = "danger") {
    if (!element) {
        return;
    }
    element.textContent = message;
    element.classList.remove("d-none", "alert-danger", "alert-success", "alert-info", "alert-warning", "alert-animate");
    element.classList.add(`alert-${type}`, "alert-animate");
}

function hideAlert(element) {
    if (!element) return;
    element.classList.add("d-none");
    element.classList.remove("alert-animate");
}

const TOAST_MAX_VISIBLE = 3;
const TOAST_DELAY_MS = { success: 4500, info: 5000, warning: 5500, danger: 6000 };

function showToastMessage(message, type = "success") {
    const container = document.getElementById("toastContainer");
    if (!container || typeof bootstrap === "undefined") return;
    while (container.children.length >= TOAST_MAX_VISIBLE && container.firstChild) {
        container.removeChild(container.firstChild);
    }
    const icons = {
        success: "bi-check-circle-fill",
        danger: "bi-x-circle-fill",
        warning: "bi-exclamation-triangle-fill",
        info: "bi-info-circle-fill",
    };
    const toast = document.createElement("div");
    toast.className = `toast toast-visual align-items-center text-bg-${type} border-0`;
    toast.role = "alert";
    toast.setAttribute("aria-live", type === "danger" || type === "warning" ? "assertive" : "polite");
    toast.innerHTML = `
        <div class="d-flex align-items-center w-100">
            <div class="toast-body flex-grow-1">
                <i class="bi ${icons[type] || icons.info} me-2"></i>
                <span>${message}</span>
            </div>
            <button type="button" class="btn-close btn-close-white flex-shrink-0" data-bs-dismiss="toast" aria-label="Закрыть"></button>
        </div>`;
    container.appendChild(toast);
    const delay = TOAST_DELAY_MS[type] || TOAST_DELAY_MS.success;
    const bsToast = new bootstrap.Toast(toast, { delay });
    toast.addEventListener("hidden.bs.toast", () => toast.remove());
    bsToast.show();
}

function applyTheme(theme) {
    const body = document.body;
    if (!body) return;
    body.dataset.theme = theme;
    body.setAttribute("data-bs-theme", theme);
}

function initMenuBadgeAndLogout() {
    const menuBadge = document.getElementById("menuBadge");
    const menuLogoutBtn = document.getElementById("menuLogoutBtn");
    if (!menuBadge) return;
    const pathname = window.location.pathname;
    const badgeFromBody = document.body.dataset.badge || "";
    const isAdminDashboard = pathname === "/admin/dashboard" || pathname.indexOf("/admin/dashboard") === 0;
    const displayBadge = badgeFromBody || (isAdminDashboard ? "Админ" : "") || "—";
    menuBadge.textContent = displayBadge;
    if (menuLogoutBtn) {
        menuLogoutBtn.style.display = badgeFromBody || isAdminDashboard ? "" : "none";
        menuLogoutBtn.addEventListener("click", (e) => {
            e.preventDefault();
            if (isAdminDashboard) {
                fetch("/api/admin/logout", { method: "POST", credentials: "include" })
                    .then(() => { window.location.href = "/admin"; })
                    .catch(() => { window.location.href = "/admin"; });
            } else if (badgeFromBody) {
                window.location.href = "/logout";
            }
        });
    }
}

function initThemeToggle() {
    const stored = localStorage.getItem("inventory-theme");
    applyTheme(stored || "light");
    const toggle = document.getElementById("themeToggle");
    toggle?.addEventListener("click", (event) => {
        const current = document.body.dataset.theme === "dark" ? "dark" : "light";
        const next = current === "dark" ? "light" : "dark";

        // Точка анимации (куда кликнули)
        const x = event?.clientX ?? window.innerWidth / 2;
        const y = event?.clientY ?? 80;

        // splash overlay — красивое наложение при смене переменных
        const splashId = "themeSplash";
        let splash = document.getElementById(splashId);
        if (!splash) {
            splash = document.createElement("div");
            splash.id = splashId;
            splash.className = "theme-splash";
            document.body.appendChild(splash);
        }
        splash.style.setProperty("--splash-x", `${x}px`);
        splash.style.setProperty("--splash-y", `${y}px`);
        splash.classList.remove("theme-splash--run");
        // force reflow so the animation restarts
        // eslint-disable-next-line no-unused-expressions
        splash.offsetHeight;
        splash.classList.add("theme-splash--run");

        toggle.classList.add("theme-toggle--clicked", next === "dark" ? "theme-sunset" : "theme-sunrise");
        applyTheme(next);
        localStorage.setItem("inventory-theme", next);

        setTimeout(() => {
            toggle.classList.remove("theme-toggle--clicked", "theme-sunset", "theme-sunrise");
        }, 650);
    });
}

function initCommandPalette() {
    const palette = document.getElementById("commandPalette");
    if (!palette) return;
    const backdrop = document.getElementById("commandPaletteBackdrop");
    const cmdNewShift = document.getElementById("cmdNewShift");
    const cmdSyncQueue = document.getElementById("cmdSyncQueue");
    const cmdOpenReports = document.getElementById("cmdOpenReports");

    const close = () => palette.classList.add("d-none");
    const open = () => palette.classList.remove("d-none");

    document.addEventListener("keydown", (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "k") {
            e.preventDefault();
            if (palette.classList.contains("d-none")) open();
            else close();
        }
        if (e.key === "Escape") close();
    });
    backdrop?.addEventListener("click", close);
    palette.querySelectorAll("a.command-item").forEach((a) => a.addEventListener("click", close));
    cmdNewShift?.addEventListener("click", () => {
        document.getElementById("newShiftBtn")?.click();
        document.getElementById("newShiftBtnScanOnly")?.click();
        close();
    });
    cmdSyncQueue?.addEventListener("click", () => {
        document.getElementById("offlineQueueSyncBtn")?.click();
        close();
    });
    cmdOpenReports?.addEventListener("click", () => {
        document.getElementById("exportBtn")?.click();
        close();
    });
}

function initTopbarClock() {
    const el = document.getElementById("topbarClock");
    if (!el) return;
    const render = () => {
        const now = new Date();
        el.textContent = now.toLocaleTimeString("ru-RU", {
            hour: "2-digit",
            minute: "2-digit",
        });
    };
    render();
    setInterval(render, 1000);
}

function markAnimateTargets() {
    ANIMATE_SELECTORS.forEach((selector) => {
        document.querySelectorAll(selector).forEach((element, idx) => {
            element.classList.add("animate-on-scroll");
            if (!element.dataset.staggerIndex) {
                element.style.setProperty("--stagger-index", String(idx % 12));
                element.dataset.staggerIndex = "1";
            }
        });
    });
}

function refreshAnimations() {
    if (!animationObserver) return;
    markAnimateTargets();
    document.querySelectorAll(".animate-on-scroll").forEach((element) => {
        if (element.dataset.animateBound) return;
        animationObserver.observe(element);
        element.dataset.animateBound = "true";
    });
}

function initAnimations() {
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
    animationObserver = new IntersectionObserver(
        (entries) => {
            entries.forEach((entry) => {
                if (entry.isIntersecting) {
                    entry.target.classList.add("animate-visible");
                    animationObserver.unobserve(entry.target);
                }
            });
        },
        { threshold: 0.15 }
    );
    markAnimateTargets();
    refreshAnimations();
}

function initMagneticButtons() {
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
    const ids = ["saveScanBtn", "exportBtn", "offlineQueueSyncBtn"];
    ids.forEach((id) => {
        const btn = document.getElementById(id);
        if (!btn) return;
        btn.classList.add("magnetic-btn");
        const intensity = 10;
        btn.addEventListener("mousemove", (e) => {
            const rect = btn.getBoundingClientRect();
            const x = e.clientX - rect.left;
            const y = e.clientY - rect.top;
            const mx = ((x / rect.width) - 0.5) * intensity;
            const my = ((y / rect.height) - 0.5) * intensity;
            btn.style.transform = `translate(${mx.toFixed(1)}px, ${my.toFixed(1)}px)`;
        });
        btn.addEventListener("mouseleave", () => {
            btn.style.transform = "";
        });
    });
}

function formatDate(value) {
    if (!value) return "—";
    const date = typeof value === "string" ? new Date(value) : value;
    return date.toLocaleString("ru-RU");
}

function formatDuration(seconds) {
    if (!seconds) return "—";
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    return `${hours}ч ${minutes}м`;
}

function initLoginPage() {
    const userForm      = document.getElementById("loginForm");
    const adminForm     = document.getElementById("loginFormAdmin");
    if (!userForm && !adminForm) return;

    // ── элементы ──
    const flipCard       = document.getElementById("loginFlipCard");
    const badgeInput     = document.getElementById("badgeInput");
    const badgeAdminIn   = document.getElementById("badgeInputAdmin");
    const passwordInput  = document.getElementById("passwordInput");
    const rememberCheck  = document.getElementById("remember");
    const alertBox       = document.getElementById("loginAlert");
    const alertAdmin     = document.getElementById("loginAlertAdmin");
    const connBadge      = document.getElementById("connectionStatusBadge");
    const deviceInfo     = document.getElementById("deviceStatusInfo");
    const togglePwdBtn   = document.getElementById("togglePassword");
    const adminToggle    = document.getElementById("adminToggleLink");
    const backLink       = document.getElementById("adminBackLink");

    // подставить сохранённый бэйдж
    const storedBadge = localStorage.getItem("badge");
    if (storedBadge && badgeInput) badgeInput.value = storedBadge;

    // ── стартовая анимация появления (на сцене, не на flipCard, чтобы не перебить transform flip) ──
    const flipScene = flipCard?.closest(".login-flip-scene") || flipCard?.parentElement;
    if (flipScene) {
        flipScene.style.opacity = "0";
        flipScene.style.transform = "translateY(24px)";
        flipScene.style.transition = "opacity .5s ease, transform .5s ease";
        requestAnimationFrame(() => {
            flipScene.style.opacity = "1";
            flipScene.style.transform = "translateY(0)";
        });
    }

    // ── утилита: flip ──
    let isAdminMode = false;
    let flipLocked = false;

    function doFlip(toAdmin) {
        if (flipLocked || isAdminMode === toAdmin) return;
        flipLocked = true;
        isAdminMode = toAdmin;

        flipCard?.classList.add("flipping");

        if (toAdmin) {
            flipCard?.classList.add("is-flipped");
            if (alertBox) { alertBox.classList.add("d-none"); alertBox.textContent = ""; }
            // фокус после окончания анимации
            setTimeout(() => { if (badgeAdminIn) { badgeAdminIn.value = ""; badgeAdminIn.focus(); } }, 680);
        } else {
            flipCard?.classList.remove("is-flipped");
            const stored = localStorage.getItem("badge");
            if (badgeInput) { badgeInput.readOnly = false; badgeInput.value = stored || ""; }
            if (alertAdmin) { alertAdmin.classList.add("d-none"); alertAdmin.textContent = ""; }
            if (passwordInput) { passwordInput.value = ""; }
            setTimeout(() => badgeInput?.focus(), 680);
        }

        setTimeout(() => {
            flipCard?.classList.remove("flipping");
            flipLocked = false;
        }, 750);
    }

    adminToggle?.addEventListener("click", e => { e.preventDefault(); doFlip(true); });
    backLink?.addEventListener("click",    e => { e.preventDefault(); doFlip(false); });

    // ── показ/скрытие пароля ──
    togglePwdBtn?.addEventListener("click", () => {
        if (!passwordInput) return;
        passwordInput.type = passwordInput.type === "password" ? "text" : "password";
        const icon = togglePwdBtn.querySelector("i");
        if (icon) icon.className = passwordInput.type === "password" ? "bi bi-eye" : "bi bi-eye-slash";
    });

    // ── проверка соединения ──
    async function updateConnectionStatus() {
        if (!connBadge) return;
        const status = await getConnectivitySnapshot();
        if (status.offline) {
            connBadge.textContent = "Оффлайн: нет сети";
            connBadge.className = "badge rounded-pill text-bg-danger";
            return;
        }
        connBadge.textContent = "Проверяем соединение…";
        connBadge.className = "badge rounded-pill text-bg-secondary connection-status";
        connBadge.classList.remove("connection-online", "connection-offline");
        if (status.serverReachable && status.dbWritable) {
            connBadge.textContent = "Онлайн: связь с сервером";
            connBadge.className = "badge rounded-pill text-bg-success connection-status connection-online";
            connBadge.classList.add("badge-pulse-once");
            setTimeout(() => connBadge.classList.remove("badge-pulse-once"), 1300);
        } else if (status.serverReachable) {
            connBadge.textContent = "Сервер доступен, БД недоступна";
            connBadge.className = "badge rounded-pill text-bg-warning connection-status connection-offline";
        } else {
            connBadge.textContent = "Проблемы соединения";
            connBadge.className = "badge rounded-pill text-bg-warning connection-status connection-offline";
        }
    }
    updateConnectionStatus();
    window.addEventListener("online",  updateConnectionStatus);
    window.addEventListener("offline", updateConnectionStatus);
    connBadge?.addEventListener("click", updateConnectionStatus);

    // информация об устройстве
    if (deviceInfo) {
        const cameraOk = !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia);
        deviceInfo.textContent = cameraOk
            ? "Камера устройства поддерживается. Можно использовать сканер."
            : "Браузер не даёт доступ к камере. Используйте сканер штрихкодов или ручной ввод.";
    }

    // ── общая функция отправки авторизации ──
    async function submitAuth({ badge, password, alertEl, formEl, remember }) {
        hideAlert(alertEl);
        const { ok, data } = await API.post("/api/auth", { badge, password });
        if (!ok || data.error) {
            showAlert(alertEl, data.error || "Ошибка авторизации");
            return;
        }
        if (data.require_password) {
            showAlert(alertEl, data.message || "Введите пароль", "info");
            return;
        }
        if (remember) {
            localStorage.setItem("badge", data.badge);
        } else {
            localStorage.removeItem("badge");
        }
        sessionStorage.setItem("badge", data.badge);
        sessionStorage.setItem("isAdmin", data.is_admin ? "1" : "0");
        if (!data.is_admin && (data.redirect || "/work") === "/work") {
            sessionStorage.setItem("inventory_new_shift", "1");
        }
        // анимация исчезновения перед редиректом (на сцене, не на flipCard)
        const scene = flipCard?.closest(".login-flip-scene") || flipCard?.parentElement;
        if (scene) {
            scene.style.transition = "opacity .35s ease, transform .35s ease";
            scene.style.opacity = "0";
            scene.style.transform = "scale(.96) translateY(-8px)";
        }
        setTimeout(() => {
            window.location.href = data.redirect || (data.is_admin ? "/admin/dashboard" : "/work");
        }, 350);
    }

    // ── форма обычного входа ──
    userForm?.addEventListener("submit", async e => {
        e.preventDefault();
        userForm.classList.add("was-validated");
        if (!userForm.checkValidity()) return;
        await submitAuth({
            badge:    badgeInput?.value.trim() || "",
            password: "",
            alertEl:  alertBox,
            formEl:   userForm,
            remember: rememberCheck?.checked,
        });
    });

    // ── форма Admin-входа ──
    adminForm?.addEventListener("submit", async e => {
        e.preventDefault();
        adminForm.classList.add("was-validated");
        if (!adminForm.checkValidity()) return;
        await submitAuth({
            badge:    badgeAdminIn?.value.trim() || "ADMIN",
            password: passwordInput?.value.trim() || "",
            alertEl:  alertAdmin,
            formEl:   adminForm,
            remember: false,
        });
    });
}

const SHIFT_START_KEY = "inventory_shift_start";
const NEW_SHIFT_KEY = "inventory_new_shift";

function initWorkPage() {
    const pageBadge = document.body.dataset.badge;
    const badge = pageBadge || sessionStorage.getItem("badge") || localStorage.getItem("badge");
    if (!badge) {
        window.location.href = "/";
        return;
    }
    // При первом заходе после логина — начать смену с текущего момента (статистика с нуля)
    try {
        if (sessionStorage.getItem(NEW_SHIFT_KEY) === "1") {
            sessionStorage.setItem(SHIFT_START_KEY, String(Date.now()));
            sessionStorage.removeItem(NEW_SHIFT_KEY);
        }
    } catch (e) {}

    const badgeLabel = document.getElementById("currentBadge");
    const lastSyncLabel = document.getElementById("lastSyncLabel");
    const catalogSyncStatus = document.getElementById("catalogSyncStatus");
    const newShiftBtn = document.getElementById("newShiftBtn");
    const syncCatalogBtn = document.getElementById("syncCatalogBtn");
    const syncCatalogModalEl = document.getElementById("syncCatalogModal");
    const syncCatalogBlocksList = document.getElementById("syncCatalogBlocksList");
    const syncCatalogFloorsList = document.getElementById("syncCatalogFloorsList");
    const syncCatalogApplyBtn = document.getElementById("syncCatalogApplyBtn");
    const syncCatalogFullBtn = document.getElementById("syncCatalogFullBtn");
    const newShiftBtnScanOnly = document.getElementById("newShiftBtnScanOnly");
    const placeForm = document.getElementById("scanForm");
    const placeInput = document.getElementById("placeInput");
    const placeAlert = document.getElementById("placeAlert");
    const placeTitle = document.getElementById("placeTitle");
    const placeUpdatedLabel = document.getElementById("placeUpdatedLabel");
    const clearPlaceBtn = document.getElementById("clearPlaceBtn");
    const refreshPlaceBtn = document.getElementById("refreshPlaceBtn");
    const statusButtonsContainer = document.getElementById("statusButtons");
    const statusLabel = document.getElementById("statusLabel");
    const commentInput = document.getElementById("commentInput");
    const duplicateDetailsBlock = document.getElementById("duplicateDetailsBlock");
    const duplicateFloorInput = document.getElementById("duplicateFloorInput");
    const duplicateRowInput = document.getElementById("duplicateRowInput");
    const duplicateShelfInput = document.getElementById("duplicateShelfInput");
    const discrepancyReasonBlock = document.getElementById("discrepancyReasonBlock");
    const discrepancyReasonSelect = document.getElementById("discrepancyReasonSelect");
    const reasonDetailRow = document.getElementById("reasonDetailRow");
    const reasonDetailLabel = document.getElementById("reasonDetailLabel");
    const reasonDetailSelect = document.getElementById("reasonDetailSelect");
    const reasonDetailInput = document.getElementById("reasonDetailInput");
    const otherReasonBlock = document.getElementById("otherReasonBlock");
    const otherReasonInput = document.getElementById("otherReasonInput");
    const photoDropzone = document.getElementById("photoDropzone");
    const photoInput = document.getElementById("photoInput");
    if (photoInput && typeof navigator !== "undefined" && /Android/i.test(navigator.userAgent || "")) {
        photoInput.setAttribute("capture", "environment");
    }
    const photoSelectBtn = document.getElementById("photoSelectBtn");
    const photoPreview = document.getElementById("photoPreview");
    const photoHint = document.getElementById("photoHint");
    const saveScanBtn = document.getElementById("saveScanBtn");
    const saveSpinner = document.getElementById("saveSpinner");
    const qrModalEl = document.getElementById("qrModal");
    const qrReaderEl = document.getElementById("qrReader");
    const qrStatusBadge = document.getElementById("qrStatus");
    const qrTorchBtn = document.getElementById("qrTorchBtn");
    const qrTorchIcon = document.getElementById("qrTorchIcon");
    const qrTorchLabel = document.getElementById("qrTorchLabel");
    const openQrScannerBtn = document.getElementById("openQrScannerBtn");
    const notificationList = document.getElementById("notificationList");
    const clearLogBtn = document.getElementById("clearLogBtn");
    const fabScanBtn = document.getElementById("fabScanBtn");
    const routeSuggestionsEl = document.getElementById("routeSuggestions");
    const routeMapEl = document.getElementById("routeMap");
    const refreshRouteBtn = document.getElementById("refreshRouteBtn");
    const routeFilterSelect = document.getElementById("routeFilterSelect");
    const quickScanModeCheck = document.getElementById("quickScanMode");
    const historyFromInput = document.getElementById("historyFrom");
    const historyToInput = document.getElementById("historyTo");
    const historyReloadBtn = document.getElementById("historyReloadBtn");
    const historyTableBody = document.getElementById("historyTableBody");
    const historyDownloadBadBtn = document.getElementById("historyDownloadBadBtn");
    const onlineBadge = document.getElementById("onlineBadge");
    const scanOnlyToggle = document.getElementById("scanOnlyToggle");
    const scanOnlyOkBtn = document.getElementById("scanOnlyOkBtn");
    const scanOnlyErrorBtn = document.getElementById("scanOnlyErrorBtn");
    const placeCardSwipeHint = document.getElementById("placeCardSwipeHint");
    const repeatMxChip = document.getElementById("repeatMxChip");
    const repeatMxChipLabel = document.getElementById("repeatMxChipLabel");
    const todaySavedCountEl = document.getElementById("todaySavedCount");
    const offlineQueueBar = document.getElementById("offlineQueueBar");
    const offlineQueueCount = document.getElementById("offlineQueueCount");
    const offlineQueueSyncBtn = document.getElementById("offlineQueueSyncBtn");
    const offlineQueueHint = document.getElementById("offlineQueueHint");
    const contextModeChip = document.getElementById("contextModeChip");
    const contextBlockChip = document.getElementById("contextBlockChip");
    const contextFreshChip = document.getElementById("contextFreshChip");
    const lastScanAgoChip = document.getElementById("lastScanAgoChip");
    const quickReasonChips = document.getElementById("quickReasonChips");
    const quickErrorGrid = document.getElementById("quickErrorGrid");
    const historyShowTodayBtn = document.getElementById("historyShowTodayBtn");
    const emptyStateScanNowBtn = document.getElementById("emptyStateScanNowBtn");
    const miniRecentScansList = document.getElementById("miniRecentScansList");
    const miniRecentScansHint = document.getElementById("miniRecentScansHint");

    badgeLabel.textContent = badge;
    const syncCatalogModal =
        syncCatalogModalEl && typeof bootstrap !== "undefined"
            ? new bootstrap.Modal(syncCatalogModalEl)
            : null;
    let syncAvailableBlocks = [];
    let syncBlockGroups = [];
    let syncAvailableFloors = [];
    let syncSelectedGroupCodes = new Set();
    let syncSelectedFloors = new Set();
    let syncBlockToGroupMap = new Map();
    let syncBlocksCache = { at: 0, blocks: [] };
    const syncFloorsCache = new Map();
    let syncFloorsRequestSeq = 0;
    let syncBlocksLoadingPromise = null;

    function blockDisplayName(blockCode) {
        const code = String(blockCode || "").toUpperCase();
        const m = code.match(/^Э(\d+)/);
        if (m) return `Электросталь ${parseInt(m[1], 10)}`;
        return code;
    }

    function resolveGroupCodeByBlock(blockCode) {
        const code = String(blockCode || "").toUpperCase().trim();
        if (!code) return "";
        return syncBlockToGroupMap.get(code) || code;
    }

    function buildSyncBlockGroupingMap(blockCodes) {
        const normalized = blockCodes
            .map((x) => String(x || "").toUpperCase().trim())
            .filter(Boolean);
        const map = new Map();
        // После перехода на warehouse_name группируем "как есть":
        // каждое название склада является отдельным логическим блоком.
        normalized.forEach((code) => {
            map.set(code, code);
        });
        return map;
    }

    function buildSyncBlockGroups(blockCodes) {
        syncBlockToGroupMap = buildSyncBlockGroupingMap(blockCodes);
        const groupMap = new Map();
        blockCodes.forEach((rawCode) => {
            const sourceCode = String(rawCode || "").toUpperCase().trim();
            if (!sourceCode) return;
            const groupCode = resolveGroupCodeByBlock(sourceCode);
            if (!groupMap.has(groupCode)) {
                groupMap.set(groupCode, {
                    groupCode,
                    displayName: blockDisplayName(groupCode),
                    sourceCodes: [],
                });
            }
            const group = groupMap.get(groupCode);
            if (!group.sourceCodes.includes(sourceCode)) group.sourceCodes.push(sourceCode);
        });
        const groups = Array.from(groupMap.values());
        groups.forEach((g) => g.sourceCodes.sort((a, b) => a.localeCompare(b, "ru")));
        groups.sort((a, b) => {
            const am = a.groupCode.match(/^Э(\d+)/);
            const bm = b.groupCode.match(/^Э(\d+)/);
            if (am && bm) return Number(am[1]) - Number(bm[1]);
            return a.groupCode.localeCompare(b.groupCode, "ru");
        });
        return groups;
    }

    function renderSyncBlocksSelector() {
        if (!syncCatalogBlocksList) return;
        if (!syncBlockGroups.length) {
            syncCatalogBlocksList.innerHTML = '<div class="text-muted small">Блоки не найдены</div>';
            return;
        }
        syncCatalogBlocksList.innerHTML = syncBlockGroups
            .map((group) => {
                const active = syncSelectedGroupCodes.has(group.groupCode) ? "active" : "";
                const codesText = group.sourceCodes.join(", ");
                return `
                    <button type="button" class="sync-block-chip ${active}" data-sync-group="${group.groupCode}">
                        <span class="sync-block-code">${group.displayName}</span>
                        <span class="sync-block-name">${codesText}</span>
                    </button>
                `;
            })
            .join("");
    }

    function getSelectedSourceBlocks() {
        const pickedGroups = Array.from(syncSelectedGroupCodes);
        return syncBlockGroups
            .filter((g) => pickedGroups.includes(g.groupCode))
            .flatMap((g) => g.sourceCodes);
    }

    function renderSyncFloorsSelector() {
        if (!syncCatalogFloorsList) return;
        if (!syncAvailableFloors.length) {
            syncCatalogFloorsList.innerHTML = '<div class="text-muted small">Этажи не найдены</div>';
            return;
        }
        syncCatalogFloorsList.innerHTML = syncAvailableFloors
            .map((floor) => {
                const active = syncSelectedFloors.has(floor) ? "active" : "";
                return `<button type="button" class="sync-floor-chip ${active}" data-sync-floor="${floor}">${floor}</button>`;
            })
            .join("");
    }

    async function loadSyncFloorsForSelectedBlocks() {
        if (!syncCatalogFloorsList) return;
        const reqSeq = ++syncFloorsRequestSeq;
        const pickedBlocks = getSelectedSourceBlocks();
        if (!pickedBlocks.length) {
            syncAvailableFloors = [];
            syncSelectedFloors = new Set();
            syncCatalogFloorsList.innerHTML = '<div class="text-muted small">Сначала выберите блок</div>';
            return;
        }
        syncCatalogFloorsList.innerHTML = '<div class="text-muted small">Загрузка этажей…</div>';
        const cacheKey = pickedBlocks.slice().sort().join("|");
        const cached = syncFloorsCache.get(cacheKey);
        const now = Date.now();
        let floorsList = null;
        if (cached && now - cached.at < SYNC_META_CACHE_TTL_MS) {
            floorsList = cached.floors;
        } else {
            const params = new URLSearchParams();
            params.set("blocks", pickedBlocks.join(","));
            const floorsRes = await API.get(`/api/sync/floors?${params.toString()}`);
            if (reqSeq !== syncFloorsRequestSeq) return;
            if (!floorsRes.ok || !floorsRes.data?.success || !Array.isArray(floorsRes.data.floors)) {
                syncAvailableFloors = [];
                syncSelectedFloors = new Set();
                syncCatalogFloorsList.innerHTML = '<div class="text-danger small">Не удалось загрузить этажи</div>';
                showToastMessage(floorsRes.data?.error || "Не удалось загрузить этажи", "danger");
                return;
            }
            floorsList = floorsRes.data.floors;
            syncFloorsCache.set(cacheKey, { at: now, floors: floorsList });
        }
        if (reqSeq !== syncFloorsRequestSeq) return;
        syncAvailableFloors = floorsList
            .map((x) => String(x || "").trim().toUpperCase())
            .filter(Boolean);
        const lastFloors = (localStorage.getItem(SYNC_LAST_FLOORS_KEY) || "")
            .split(",")
            .map((x) => x.trim().toUpperCase())
            .filter(Boolean);
        const initialFloors = lastFloors.filter((x) => syncAvailableFloors.includes(x));
        syncSelectedFloors = new Set(initialFloors.length ? initialFloors.slice(0, 1) : syncAvailableFloors.slice(0, 1));
        renderSyncFloorsSelector();
    }

    function applySyncBlocksList(blocksList) {
        syncAvailableBlocks = (blocksList || []).map((b) => String(b || "").toUpperCase()).filter(Boolean);
        syncBlockGroups = buildSyncBlockGroups(syncAvailableBlocks);
        const lastBlocks = (localStorage.getItem(SYNC_LAST_BLOCKS_KEY) || "")
            .split(",")
            .map((x) => x.trim().toUpperCase())
            .filter(Boolean);
        const lastGroupCodes = lastBlocks.map((x) => resolveGroupCodeByBlock(x));
        const initialGroups = lastGroupCodes.length
            ? lastGroupCodes
            : syncBlockGroups.slice(0, 2).map((g) => g.groupCode);
        syncSelectedGroupCodes = new Set(initialGroups);
        renderSyncBlocksSelector();
        void loadSyncFloorsForSelectedBlocks();
    }

    async function fetchSyncBlocks(forceRefresh = false) {
        const now = Date.now();
        if (!forceRefresh && syncBlocksCache.blocks.length && now - syncBlocksCache.at < SYNC_META_CACHE_TTL_MS) {
            return syncBlocksCache.blocks;
        }
        if (syncBlocksLoadingPromise) return syncBlocksLoadingPromise;
        syncBlocksLoadingPromise = (async () => {
            const blocksRes = await API.get("/api/sync/blocks");
            if (!blocksRes.ok || !blocksRes.data?.success || !Array.isArray(blocksRes.data.blocks)) {
                throw new Error(blocksRes.data?.error || "Не удалось загрузить блоки");
            }
            const fresh = blocksRes.data.blocks;
            syncBlocksCache = { at: Date.now(), blocks: fresh };
            return fresh;
        })();
        try {
            return await syncBlocksLoadingPromise;
        } finally {
            syncBlocksLoadingPromise = null;
        }
    }

    async function openSyncCatalogModal() {
        if (!syncCatalogBlocksList) return;
        if (syncCatalogModal) syncCatalogModal.show();
        if (syncCatalogFloorsList) {
            syncCatalogFloorsList.innerHTML = '<div class="text-muted small">Сначала выберите блок</div>';
        }
        if (syncBlocksCache.blocks.length) {
            applySyncBlocksList(syncBlocksCache.blocks);
        } else {
            syncCatalogBlocksList.innerHTML = '<div class="text-muted small">Загрузка блоков…</div>';
        }
        try {
            const blocks = await fetchSyncBlocks(false);
            applySyncBlocksList(blocks);
        } catch (e) {
            if (!syncBlocksCache.blocks.length) {
                syncCatalogBlocksList.innerHTML = '<div class="text-danger small">Не удалось загрузить блоки</div>';
            }
            showToastMessage(e?.message || "Не удалось загрузить блоки", "danger");
            return;
        }
        // Stale-while-revalidate: если кэш старый — обновляем в фоне, не блокируя UI.
        if (Date.now() - syncBlocksCache.at > SYNC_META_CACHE_TTL_MS && navigator.onLine) {
            fetchSyncBlocks(true)
                .then((freshBlocks) => applySyncBlocksList(freshBlocks))
                .catch(() => {});
        }
    }

    function preloadSyncCatalogMeta() {
        if (!navigator.onLine) return;
        fetchSyncBlocks(false)
            .then((blocks) => {
                if (!syncBlocksCache.blocks.length) applySyncBlocksList(blocks);
            })
            .catch(() => {});
    }

    function setCatalogSyncStatus(text, type = "success") {
        if (!catalogSyncStatus) return;
        catalogSyncStatus.textContent = text;
        catalogSyncStatus.classList.remove("d-none", "text-bg-success", "text-bg-warning", "text-bg-danger", "text-bg-secondary");
        catalogSyncStatus.classList.add(`text-bg-${type}`);
    }
    function updateOfflineReadinessStatus() {
        const cacheSize = PlaceCache.size();
        const hasCache = cacheSize > 0;
        if (lastSyncLabel) {
            const lastAt = localStorage.getItem(SYNC_LAST_AT_KEY);
            if (lastAt) lastSyncLabel.textContent = `Синхронизация: ${formatDate(lastAt)} (${cacheSize})`;
            else lastSyncLabel.textContent = hasCache ? `Локальный кэш: ${cacheSize} МХ` : "Локальный кэш: пуст";
        }
        if (!catalogSyncStatus) return;
        if (hasCache) {
            setCatalogSyncStatus("Офлайн-режим готов (кэш загружен)", "success");
        } else {
            setCatalogSyncStatus("Офлайн ограничен: сначала синхронизируйте справочник", "warning");
        }
    }
    try {
        const lastAt = localStorage.getItem(SYNC_LAST_AT_KEY);
        if (lastAt && lastSyncLabel) {
            lastSyncLabel.textContent = `Синхронизация: ${formatDate(lastAt)}`;
            setCatalogSyncStatus("Справочник синхронизирован", "success");
        }
    } catch (_) {}
    updateOfflineReadinessStatus();

    let placeSyncInProgress = false;

    async function syncPlacesInChunks({ forceFull = false, silent = false, blocks = [], floors = [] } = {}) {
        if (placeSyncInProgress) return;
        if (!navigator.onLine) {
            if (!silent) showToastMessage("Нет сети для синхронизации", "warning");
            setCatalogSyncStatus("Нет сети для синхронизации", "warning");
            return;
        }
        const normalizedBlocks = Array.isArray(blocks)
            ? Array.from(new Set(blocks.map((b) => String(b || "").trim().toUpperCase()).filter(Boolean)))
            : [];
        const normalizedFloors = Array.isArray(floors)
            ? Array.from(new Set(floors.map((f) => String(f || "").trim().toUpperCase()).filter(Boolean)))
            : [];
        const blocksKeyPart = normalizedBlocks.length ? normalizedBlocks.join("|") : "ALL";
        const floorsKeyPart = normalizedFloors.length ? normalizedFloors.join("|") : "ALL";
        const cursorKey = `${SYNC_CURSOR_KEY}:${blocksKeyPart}:${floorsKeyPart}`;
        placeSyncInProgress = true;
        const globalProgressBar = document.getElementById("globalProgressBar");
        if (globalProgressBar) globalProgressBar.classList.remove("d-none");
        let totalLoaded = 0;
        let hadSyncError = false;
        try {
            let since = null;
            if (!forceFull) {
                const raw = localStorage.getItem(cursorKey);
                if (raw && /^\d+$/.test(raw)) since = Number(raw);
            }
            let limit = 5000;
            let rateLimitRetries = 0;
            if (normalizedBlocks.length) {
                const floorSuffix = normalizedFloors.length ? `, этаж ${normalizedFloors.join(", ")}` : "";
                setCatalogSyncStatus(`Синхронизация: ${normalizedBlocks.join(", ")}${floorSuffix}`, "secondary");
            }
            while (true) {
                const t0 = performance.now ? performance.now() : Date.now();
                const params = new URLSearchParams();
                params.set("limit", String(limit));
                if (since != null) params.set("since_id", String(since));
                if (normalizedBlocks.length) params.set("blocks", normalizedBlocks.join(","));
                if (normalizedFloors.length) params.set("floors", normalizedFloors.join(","));
                const { ok, status, data } = await API.get(`/api/sync?${params.toString()}`);
                const elapsed = (performance.now ? performance.now() : Date.now()) - t0;
                if (status === 429) {
                    // Сервер попросил притормозить — делаем паузу и пробуем снова.
                    rateLimitRetries += 1;
                    if (rateLimitRetries > 8) {
                        hadSyncError = true;
                        setCatalogSyncStatus("Слишком много запросов к серверу", "danger");
                        if (!silent) showToastMessage("Синхронизация остановлена: лимит запросов", "danger");
                        break;
                    }
                    const text = String(data?.error || "");
                    const m = text.match(/(\d+)\s*сек/i);
                    const waitSec = m ? Math.max(1, Number(m[1])) : 2;
                    setCatalogSyncStatus(`Пауза ${waitSec}с (лимит API)`, "warning");
                    await new Promise((resolve) => setTimeout(resolve, waitSec * 1000));
                    continue;
                }
                if (!ok || data?.error) {
                    if (!silent) showToastMessage(data?.error || "Ошибка синхронизации", "danger");
                    hadSyncError = true;
                    setCatalogSyncStatus("Ошибка синхронизации", "danger");
                    break;
                }
                rateLimitRetries = 0;
                const rows = data?.data || [];
                if (!rows.length) break;
                // Пакетная запись вместо тысяч setItem подряд (сильно меньше лагов UI).
                PlaceCache.setMany(rows);
                totalLoaded += rows.length;
                since = data?.next_since_id ?? since;
                if (lastSyncLabel) {
                    lastSyncLabel.textContent = `Синхронизация: ${totalLoaded} (пачка ${limit}, ${Math.round(elapsed)}мс)`;
                }
                // Адаптируем размер чанка под сеть/сервер:
                // быстро -> увеличиваем, медленно -> уменьшаем.
                if (elapsed < 700 && limit < 5000) limit = Math.min(5000, limit + 300);
                else if (elapsed > 1400 && limit > 800) limit = Math.max(800, limit - 300);
                if (!data?.has_more) break;
            }
            if (since != null) localStorage.setItem(cursorKey, String(since));
            localStorage.setItem(SYNC_LAST_AT_KEY, new Date().toISOString());
            localStorage.setItem(SYNC_LAST_BLOCKS_KEY, normalizedBlocks.join(","));
            localStorage.setItem(SYNC_LAST_FLOORS_KEY, normalizedFloors.join(","));
            PlaceCache.flush();
            if (lastSyncLabel) {
                const scopeParts = [];
                if (normalizedBlocks.length) scopeParts.push(normalizedBlocks.join(", "));
                if (normalizedFloors.length) scopeParts.push(`этаж ${normalizedFloors.join(", ")}`);
                const scope = scopeParts.length ? ` (${scopeParts.join("; ")})` : "";
                lastSyncLabel.textContent = `Синхронизация: ${formatDate(new Date())}${scope}`;
            }
            if (!hadSyncError) {
                setCatalogSyncStatus("Справочник синхронизирован", "success");
                updateOfflineReadinessStatus();
                if (!silent) {
                    if (totalLoaded > 0) {
                        const suffixParts = [];
                        if (normalizedBlocks.length) suffixParts.push(normalizedBlocks.join(", "));
                        if (normalizedFloors.length) suffixParts.push(`этаж ${normalizedFloors.join(", ")}`);
                        const suffix = suffixParts.length ? ` (${suffixParts.join("; ")})` : "";
                        showToastMessage(`Синхронизировано ${totalLoaded} МХ${suffix}`, "success");
                    }
                    else showToastMessage("Синхронизация актуальна", "info");
                }
            }
            return { ok: !hadSyncError, totalLoaded };
        } finally {
            if (globalProgressBar) globalProgressBar.classList.add("d-none");
            placeSyncInProgress = false;
        }
    }

    function updateOfflineQueueUI() {
        const queue = OfflineQueue.getAll();
        const n = queue.length;
        const conflicts = queue.filter((item) => item.status === "conflict").length;
        if (offlineQueueBar && offlineQueueCount) {
            if (n === 0) {
                offlineQueueBar.classList.add("d-none");
            } else {
                offlineQueueBar.classList.remove("d-none");
                offlineQueueCount.textContent = n;
            }
            const online = navigator.onLine;
            if (offlineQueueSyncBtn) {
                offlineQueueSyncBtn.disabled = !online;
                offlineQueueSyncBtn.title = online ? "Отправить неотправленные сканы на сервер" : "Нет сети — подключитесь к интернету";
            }
            if (offlineQueueHint) {
                if (conflicts > 0) {
                    offlineQueueHint.textContent = `Есть конфликтные сканы: ${conflicts}. Требуется ручная проверка данных.`;
                } else {
                    offlineQueueHint.textContent = online ? "Нажмите «Синхронизировать», чтобы отправить." : "При появлении сети отправка начнётся автоматически.";
                }
            }
        }
        const headerIndicator = document.getElementById("headerOfflineQueueIndicator");
        const headerCount = document.getElementById("headerOfflineQueueCount");
        if (headerIndicator && headerCount) {
            if (n === 0) {
                headerIndicator.classList.add("d-none");
            } else {
                headerIndicator.classList.remove("d-none");
                headerCount.textContent = n;
                const titleBase = n === 1 ? "1 скан в очереди на отправку" : `${n} сканов в очереди на отправку`;
                headerIndicator.title = conflicts > 0 ? `${titleBase}; конфликтов: ${conflicts}` : titleBase;
            }
        }
    }

    function updateTodaySavedCount() {
        if (!todaySavedCountEl) return;
        todaySavedCountEl.textContent = state.savedCount;
        todaySavedCountEl.setAttribute("data-count", String(state.savedCount));
    }

    function applyScanOnlyMode() {
        const on = state.scanOnlyMode;
        try { localStorage.setItem(SCAN_ONLY_STORAGE_KEY, on ? "1" : "0"); } catch (e) {}
        document.body.classList.toggle("scan-only-mode", on);
        if (scanOnlyToggle) scanOnlyToggle.checked = on;
        const headerIndicator = document.getElementById("headerScanOnlyIndicator");
        if (headerIndicator) headerIndicator.classList.toggle("d-none", !on);
        if (contextModeChip) {
            contextModeChip.innerHTML = on
                ? '<i class="bi bi-lightning-charge"></i> Режим: focus scan'
                : '<i class="bi bi-bullseye"></i> Режим: стандарт';
        }
    }

    function extractBlockLabel(value) {
        const s = (value || "").toString().trim().toUpperCase();
        const m = s.match(/^(Э\d+)/);
        return m ? m[1] : "—";
    }

    function updateLastScanAgoChip() {
        if (!lastScanAgoChip) return;
        if (!state.lastSavedAt) {
            lastScanAgoChip.innerHTML = '<i class="bi bi-clock-history"></i> Последний скан: —';
            return;
        }
        const diffMin = Math.max(0, Math.round((Date.now() - state.lastSavedAt) / 60000));
        const text = diffMin === 0 ? "только что" : `${diffMin} мин назад`;
        lastScanAgoChip.innerHTML = `<i class="bi bi-clock-history"></i> Последний скан: ${text}`;
    }

    function focusPlaceInput(selectText = false) {
        if (!placeInput) return;
        placeInput.focus();
        if (selectText && typeof placeInput.select === "function") {
            placeInput.select();
        }
    }

    function setTodayDates() {
        const today = new Date().toISOString().slice(0, 10);
        if (historyFromInput && !historyFromInput.value) historyFromInput.value = today;
        if (historyToInput && !historyToInput.value) historyToInput.value = today;
    }

    async function updateOnlineStatus() {
        updateOfflineQueueUI();
        if (!onlineBadge) return;
        const status = await getConnectivitySnapshot();
        if (status.offline) {
            onlineBadge.textContent = "Офлайн";
            onlineBadge.className = "badge bg-danger rounded-pill mt-2";
            return;
        }
        if (status.serverReachable && status.dbWritable) {
            onlineBadge.textContent = "Онлайн";
            onlineBadge.className = "badge bg-success rounded-pill mt-2";
        } else if (status.serverReachable) {
            onlineBadge.textContent = "Сервер без БД";
            onlineBadge.className = "badge bg-warning rounded-pill mt-2";
        } else {
            onlineBadge.textContent = "Нет связи";
            onlineBadge.className = "badge bg-warning rounded-pill mt-2";
        }
    }

    function normalizePlaceCode(raw) {
        if (!raw || typeof raw !== "string") return "";
        let s = raw.trim();
        const plceMatch = s.match(/^PLCE\s*(.+)$/i);
        if (plceMatch) s = plceMatch[1].trim();
        return s.toUpperCase();
    }

    function extractPlaceCodeFromQr(decodedText) {
        if (!decodedText || typeof decodedText !== "string") return "";
        const safeDecode = (value) => {
            try {
                return decodeURIComponent(value);
            } catch (e) {
                return value;
            }
        };
        const addCandidate = (set, value) => {
            if (!value) return;
            const raw = safeDecode(String(value)).trim();
            if (!raw) return;
            const normalized = normalizePlaceCode(raw).replace(/^["'`]+|["'`]+$/g, "");
            if (!normalized) return;
            set.add(normalized);
        };

        const rawText = decodedText.trim();
        const candidates = new Set();
        addCandidate(candidates, rawText);
        addCandidate(candidates, rawText.replace(/^PLCE\s*/i, ""));

        // Частый кейс: QR содержит URL, где код лежит в query/path.
        try {
            const url = new URL(rawText);
            ["mx_id", "mx", "place_cod", "place", "code"].forEach((key) => {
                addCandidate(candidates, url.searchParams.get(key));
            });
            url.pathname
                .split("/")
                .map((part) => part.trim())
                .filter(Boolean)
                .forEach((part) => addCandidate(candidates, part));
        } catch (e) {}

        safeDecode(rawText)
            .split(/[\s,;|/?&#=:]+/)
            .map((part) => part.trim())
            .filter(Boolean)
            .forEach((part) => addCandidate(candidates, part));

        const valid = Array.from(candidates)
            .map((candidate) => candidate.replace(/^PLCE\s*/i, "").trim())
            .filter((candidate) => /^[А-ЯЁA-Z0-9.\-]+$/i.test(candidate));

        if (!valid.length) return "";

        const score = (candidate) => {
            let value = 0;
            if (/^\d{6,}$/.test(candidate)) value += 30; // MX id
            if (/[А-ЯЁA-Z]/i.test(candidate)) value += 10;
            if (/[.\-]/.test(candidate)) value += 6;
            if (/\d/.test(candidate)) value += 8;
            value += Math.min(candidate.length, 20);
            return value;
        };

        valid.sort((a, b) => score(b) - score(a));
        return valid[0] || "";
    }

    function normalizePlaceKey(value) {
        if (value == null) return "";
        return String(value).trim().toUpperCase();
    }

    function collectPlaceKeys(placeCod, placeName) {
        const keys = new Set();
        const codeKey = normalizePlaceKey(placeCod);
        const nameKey = normalizePlaceKey(placeName);
        if (codeKey) keys.add(codeKey);
        if (nameKey) keys.add(nameKey);
        return Array.from(keys);
    }

    function rememberScannedPlace(placeCod, placeName) {
        collectPlaceKeys(placeCod, placeName).forEach((k) => state.scannedPlaceCodes.add(k));
    }

    function syncDuplicateDetailsVisibility() {
        if (!duplicateDetailsBlock) return;
        const on = !!state.allowDuplicateForCurrentPlace;
        duplicateDetailsBlock.classList.toggle("d-none", !on);
        if (on) {
            // Для задвойки автоматически фиксируем статус "Ошибка",
            // но не требуем отдельный выбор причины.
            setStatus("error");
            if (discrepancyReasonBlock) discrepancyReasonBlock.classList.add("d-none");
            if (discrepancyReasonSelect) discrepancyReasonSelect.value = "";
            hideReasonDetail();
            if (otherReasonBlock) otherReasonBlock.classList.add("d-none");
            if (otherReasonInput) otherReasonInput.value = "";
            if (quickReasonChips) quickReasonChips.innerHTML = "";
        }
    }

    function parseDuplicateField(value) {
        if (value == null || value === "") return null;
        const n = Number(value);
        if (!Number.isFinite(n) || n < 0) return null;
        return Math.trunc(n);
    }

    function resolveDuplicateOnLoad(placeCod, placeData) {
        state.allowDuplicateForCurrentPlace = false;
        syncDuplicateDetailsVisibility();
        const keys = collectPlaceKeys(placeData?.place_cod ?? placeCod, placeData?.place_name);
        const isDuplicate = keys.some((k) => state.scannedPlaceCodes.has(k));
        if (!isDuplicate) return "ok";

        const confirmDuplicate = window.confirm(
            "Такая ячейка уже сканировалась в текущей смене.\nЭто задвойка?"
        );
        if (!confirmDuplicate) {
            showAlert(placeAlert, "Скан отменен: задвойка не подтверждена", "info");
            logEvent("Повторный скан отменен пользователем", "info");
            focusPlaceInput(true);
            return "cancel";
        }

        state.allowDuplicateForCurrentPlace = true;
        syncDuplicateDetailsVisibility();
        setTimeout(() => duplicateFloorInput?.focus(), 0);
        return "confirmed";
    }

    const SCAN_ONLY_STORAGE_KEY = "inventory_scan_only_mode";
    const state = {
        badge,
        lastMxCode: null,
        lastPlaces: [],
        currentStatus: null,
        photoData: null,
        photos: [],
        photoLoading: false,
        currentPlace: null,
        photoUploads: 0,
        savedCount: 0,
        notifications: [],
        suggestions: [],
        scannedPlaceCodes: new Set(),
        quickScanMode: false,
        routeFilter: "all",
        scanOnlyMode: (() => {
            try { return localStorage.getItem(SCAN_ONLY_STORAGE_KEY) === "1"; } catch (e) { return false; }
        })(),
        lastSavedAt: null,
        recentScans: [],
        allowDuplicateForCurrentPlace: false,
    };
    const PLACE_LOOKUP_RETRY_COOLDOWN_MS = 7000;
    const DB_UNAVAILABLE_COOLDOWN_MS = 15000;
    const placeLookupInFlight = new Map();
    const placeLookupCooldownUntil = new Map();
    let dbUnavailableUntilTs = 0;
    let saveInFlight = false;
    let qrScanner = null;
    let qrModalInstance = null;
    let qrVideoTrack = null;
    let qrTorchOn = false;
    let qrOverlayRoot = null;
    let qrOverlayStatusEl = null;
    let qrOverlayTorchBtn = null;

    const STATUS_META = {
        ok: { label: "Совпадает", badge: "success" },
        error: { label: "Ошибка", badge: "danger" },
        missing: { label: "Отсутствует", badge: "warning" },
        shelf_error: { label: "Поломалось", badge: "warning" },
        default: { label: "Неизвестно", badge: "secondary" },
    };

    function updateStatusLabel() {
        if (!statusLabel) return;
        if (!state.currentStatus) {
            statusLabel.textContent = "Статус не выбран";
            statusLabel.className = "badge text-bg-secondary";
            return;
        }
        const meta = STATUS_META[state.currentStatus];
        statusLabel.textContent = meta?.label || state.currentStatus;
        statusLabel.className = `badge text-bg-${meta?.badge || "secondary"}`;
    }

    /** Причины по статусам: Ошибка, Отсутствует, Поломалось (sub — подпункты выпадающего списка, detailPlaceholder — подсказка для текстового поля) */
    const REASONS_BY_STATUS = {
        error: [
            { value: 'no_rack', text: 'Нет стеллажа', sub: [{ value: 'passage', text: 'проход' }, { value: 'obstacle', text: 'препятствие' }] },
            { value: 'wrong_numbering', text: 'Нарушена нумерация', detailPlaceholder: 'правильный номер' },
            { value: 'other', text: 'Другое' }
        ],
        missing: [
            { value: 'no_shelf', text: 'Нет полки', sub: [{ value: 'n_shelves', text: 'N полок' }, { value: 'shelf_size', text: 'размер полки' }] },
            { value: 'no_divider', text: 'Нет делителя', sub: [{ value: 'n_dividers', text: 'N делителей' }, { value: 'divider_size', text: 'размер делителя' }] },
            { value: 'no_box', text: 'Нет короба', sub: [{ value: 'n_boxes', text: 'N коробов' }, { value: 'box_size', text: 'размер короба' }] },
            { value: 'no_container', text: 'Нет короба', detailPlaceholder: 'ID короба' },
            { value: 'other', text: 'Другое' }
        ],
        shelf_error: [
            { value: 'shelf_broken', text: 'Сломана полка', detailPlaceholder: 'размер полки' },
            { value: 'divider_broken', text: 'Сломан делитель' },
            { value: 'box_broken', text: 'Сломан короб' },
            { value: 'box_wrong_size', text: 'Неверный размер короба' },
            { value: 'other', text: 'Другое' }
        ]
    };

    function hideReasonDetail() {
        if (reasonDetailRow) reasonDetailRow.classList.add('d-none');
        if (reasonDetailSelect) {
            reasonDetailSelect.classList.add('d-none');
            reasonDetailSelect.innerHTML = '';
            reasonDetailSelect.value = '';
        }
        if (reasonDetailInput) {
            reasonDetailInput.classList.add('d-none');
            reasonDetailInput.value = '';
            reasonDetailInput.placeholder = '';
        }
        if (reasonDetailLabel) reasonDetailLabel.textContent = '';
    }

    function updateDiscrepancyReasons(status) {
        if (!discrepancyReasonSelect) return;
        const list = (status && REASONS_BY_STATUS[status]) ? REASONS_BY_STATUS[status] : [];
        discrepancyReasonSelect.innerHTML = '<option value="">Выберите причину…</option>';
        list.forEach(opt => {
            const option = document.createElement('option');
            option.value = opt.value;
            option.textContent = opt.text;
            discrepancyReasonSelect.appendChild(option);
        });
        discrepancyReasonSelect.value = '';
        hideReasonDetail();
        if (otherReasonBlock) otherReasonBlock.classList.add('d-none');
        if (otherReasonInput) otherReasonInput.value = '';
        if (quickReasonChips) {
            quickReasonChips.innerHTML = list
                .filter((x) => x.value !== "other")
                .slice(0, 4)
                .map((x) => `<button type="button" class="quick-reason-chip" data-reason-chip="${x.value}">${x.text}</button>`)
                .join("");
        }
    }

    function showReasonDetail(reasonOption) {
        if (!reasonDetailRow || !reasonOption) return;
        reasonDetailRow.classList.remove('d-none');
        if (reasonOption.sub && reasonOption.sub.length) {
            if (reasonDetailLabel) reasonDetailLabel.textContent = 'Уточнение';
            if (reasonDetailSelect) {
                reasonDetailSelect.classList.remove('d-none');
                reasonDetailSelect.innerHTML = '<option value="">—</option>';
                reasonOption.sub.forEach(s => {
                    const opt = document.createElement('option');
                    opt.value = s.value;
                    opt.textContent = s.text;
                    reasonDetailSelect.appendChild(opt);
                });
                reasonDetailSelect.value = '';
            }
            if (reasonDetailInput) reasonDetailInput.classList.add('d-none');
        } else if (reasonOption.detailPlaceholder) {
            if (reasonDetailLabel) reasonDetailLabel.textContent = 'Уточнение';
            if (reasonDetailInput) {
                reasonDetailInput.classList.remove('d-none');
                reasonDetailInput.placeholder = reasonOption.detailPlaceholder;
                reasonDetailInput.value = '';
            }
            if (reasonDetailSelect) reasonDetailSelect.classList.add('d-none');
        }
    }

    function setStatus(value) {
        state.currentStatus = value;
        statusButtonsContainer
            ?.querySelectorAll("[data-status]")
            .forEach((btn) => {
                btn.classList.toggle("active", btn.dataset.status === value);
            });
        updateStatusLabel();
        if (discrepancyReasonBlock) {
            if (value && value !== 'ok') {
                discrepancyReasonBlock.classList.remove('d-none');
                if (discrepancyReasonSelect) discrepancyReasonSelect.value = '';
                hideReasonDetail();
                if (otherReasonBlock) otherReasonBlock.classList.add('d-none');
                if (otherReasonInput) otherReasonInput.value = '';
                updateDiscrepancyReasons(value);
            } else {
                discrepancyReasonBlock.classList.add('d-none');
                if (discrepancyReasonSelect) discrepancyReasonSelect.value = '';
                hideReasonDetail();
                if (otherReasonBlock) otherReasonBlock.classList.add('d-none');
                if (otherReasonInput) otherReasonInput.value = '';
                if (quickReasonChips) quickReasonChips.innerHTML = "";
            }
        }
    }

    function renderLastPlaces() {
        const lastPlacesGrid = document.getElementById("lastPlacesGrid");
        const lastPlacesCount = document.getElementById("lastPlacesCount");
        if (!lastPlacesGrid) return;
        if (!state.lastPlaces.length) {
            lastPlacesGrid.innerHTML = '<div class="empty-state">Нет сканирований</div>';
            if (lastPlacesCount) lastPlacesCount.textContent = "0 записей";
            return;
        }
        if (lastPlacesCount) lastPlacesCount.textContent = `${state.lastPlaces.length} записей`;
        lastPlacesGrid.innerHTML = state.lastPlaces
            .map((item) => {
                const statusClass = (item.status || "default").toLowerCase();
                const placeKey = item.place_name || item.place_cod || "";
                return `
                <div class="place-card ${statusClass}" data-place-cod="${item.place_cod || ""}" data-place-name="${(item.place_name || "").replace(/"/g, "&quot;")}" role="button" tabindex="0">
                    <div class="d-flex justify-content-between align-items-center mb-2">
                        <div>
                            <div class="fw-semibold">${item.place_name || "—"}</div>
                        </div>
                        ${renderStatusBadge(item.status)}
                    </div>
                    <div class="d-flex justify-content-between align-items-center">
                        <span class="text-muted small">${formatDate(item.created_at || item.updated_at)}</span>
                        ${item.has_photo ? '<span class="text-muted small"><i class="bi bi-camera-fill me-1"></i>Фото</span>' : ""}
                    </div>
                </div>`;
            })
            .join("");
        lastPlacesGrid.querySelectorAll(".place-card[data-place-cod]").forEach((card) => {
            card.addEventListener("click", () => {
                const cod = card.dataset.placeCod;
                const name = card.dataset.placeName;
                const key = name || cod;
                if (key && placeInput) {
                    placeInput.value = key;
                    placeInput.classList.add("is-valid");
                    loadPlace(key);
                }
            });
        });
        refreshAnimations();
    }

    function renderStatusBadge(status) {
        if (!status) return '<span class="badge text-bg-secondary"><i class="bi bi-question-circle me-1"></i>—</span>';
        const key = typeof status === "string" ? status.toLowerCase() : status;
        const meta = STATUS_META[key] || STATUS_META.default;
        const icons = {
            ok: "bi-check-circle",
            error: "bi-exclamation-octagon",
            default: "bi-info-circle",
        };
        const icon = icons[key] || icons.default;
        return `<span class="badge text-bg-${meta.badge}"><i class="bi ${icon} me-1"></i>${meta.label}</span>`;
    }

    async function loadRouteSuggestions() {
        if (!routeMapEl && !routeSuggestionsEl) return;
        const nearParam = state.currentPlace?.place_name
            ? `&near=${encodeURIComponent(state.currentPlace.place_name)}`
            : "";
        const { ok, data } = await API.get(
            `/api/tasks/suggestions?badge=${encodeURIComponent(badge || "")}${nearParam}`
        );
        if (!ok || data.error) {
            if (routeSuggestionsEl) {
                routeSuggestionsEl.innerHTML = `<span class="text-danger small">${data?.error || "Не удалось загрузить ближайшие МХ"}</span>`;
            }
            if (routeMapEl) routeMapEl.innerHTML = "";
            return;
        }
        state.suggestions = data.suggestions || [];
        renderRouteSuggestions();
        renderRouteMap();
    }

    function renderRouteSuggestions() {
        if (!routeSuggestionsEl) return;
        if (!state.suggestions.length) {
            routeSuggestionsEl.innerHTML = '<span class="text-muted small">Ближайшие МХ не найдены</span>';
            return;
        }
        routeSuggestionsEl.innerHTML = state.suggestions
            .slice(0, 6)
            .map(
                (item) => `
        <span class="badge ${item.highlight ? "text-bg-danger" : "text-bg-secondary"}">
            ${item.mx_code || item.zone || "—"} ${item.highlight ? "•" : ""}
        </span>`
            )
            .join("");
    }

    function renderRouteMap() {
        if (!routeMapEl) return;
        let items = state.suggestions.length
            ? state.suggestions.slice(0, 12)
            : Array.from({ length: 12 }, (_, idx) => ({ mx_code: `Z-${idx + 1}`, highlight: false }));
        if (state.routeFilter === "priority") {
            items = items.filter((i) => i.highlight);
        } else if (state.routeFilter === "free") {
            items = items.filter((i) => !state.scannedPlaceCodes.has((i.mx_code || "").toString().trim().toUpperCase()));
        }
        if (!items.length) items = state.suggestions.slice(0, 12);
        routeMapEl.innerHTML = items
            .map(
                (item) => {
                    const mx = item.mx_code || item.zone || "—";
                    const mxKey = String(mx).trim().toUpperCase();
                    const isScanned = state.scannedPlaceCodes.has(mxKey);
                    const cls = item.highlight ? "highlight" : isScanned ? "scanned" : "";
                    return `
        <div class="route-node ${cls}" data-mx-code="${String(mx).replace(/"/g, "&quot;")}">
            ${mx}
        </div>`;
                }
            )
            .join("");

        routeMapEl.querySelectorAll(".route-node").forEach((node) => {
            node.addEventListener("click", () => {
                const mxCode = node.dataset.mxCode || node.dataset.zone;
                if (mxCode && placeInput) {
                    placeInput.value = mxCode;
                    placeInput.classList.add("is-valid");
                    logEvent(`Выбрано МХ ${mxCode}`, "info");
                    loadPlace(mxCode);
                    placeInput.focus();
                }
            });
        });
        refreshAnimations();
    }

    function historyEmptyRow(iconClass, title, text, isError = false) {
        const cta = !isError ? '<button type="button" class="btn btn-sm btn-outline-primary mt-2" data-history-today>Показать за сегодня</button>' : "";
        return `<tr class="history-empty-row">
            <td colspan="5" class="p-0 border-0">
                <div class="history-empty-state ${isError ? "history-empty-state-error" : ""}">
                    <i class="bi ${iconClass} history-empty-icon"></i>
                    <p class="history-empty-title">${title}</p>
                    <p class="history-empty-text">${text}</p>
                    ${cta}
                </div>
            </td>
        </tr>`;
    }

    async function loadHistory() {
        if (!historyTableBody) return;
        const params = new URLSearchParams();
        params.set("badge", badge);
        const from = historyFromInput?.value;
        const to = historyToInput?.value;
        if (from) params.set("from", from);
        if (to) params.set("to", to);

        historyTableBody.innerHTML = historyEmptyRow(
            "bi-hourglass-split",
            "Загружаем…",
            "Подождите, загружаем историю сканов"
        );

        const { ok, data } = await API.get(`/api/user/history?${params.toString()}`);
        if (!ok || data.error) {
            historyTableBody.innerHTML = historyEmptyRow(
                "bi-exclamation-triangle",
                "Ошибка",
                data?.error || "Не удалось загрузить историю",
                true
            );
            return;
        }

        const history = data.history || [];
        history.forEach((item) => rememberScannedPlace(item?.place_cod, item?.place_name));
        state.recentScans = history.slice(0, 3);
        renderMiniRecentScans();
        if (!history.length) {
            historyTableBody.innerHTML = historyEmptyRow(
                "bi-inbox",
                "Записей нет",
                "За выбранный период сканов не найдено"
            );
            return;
        }

        historyTableBody.innerHTML = history
            .map(
                (item) => `
            <tr class="history-row">
                <td>${formatDate(item.created_at)}</td>
                <td class="fw-semibold">${item.place_name || "—"}</td>
                <td>${item.status || "—"}</td>
                <td>${item.has_discrepancy ? "Да" : "Нет"}</td>
                <td>${item.has_photo ? '<i class="bi bi-camera-fill text-muted"></i>' : "—"}</td>
            </tr>`
            )
            .join("");
    }

    function renderMiniRecentScans() {
        if (!miniRecentScansList) return;
        const items = Array.isArray(state.recentScans) ? state.recentScans.slice(0, 3) : [];
        if (!items.length) {
            miniRecentScansList.innerHTML = '<div class="text-muted small">Пока нет сканов</div>';
            if (miniRecentScansHint) miniRecentScansHint.textContent = "—";
            return;
        }
        if (miniRecentScansHint) miniRecentScansHint.textContent = `${items.length} шт`;
        miniRecentScansList.innerHTML = items.map((item) => {
            const statusRaw = String(item?.status || "—");
            const status = statusRaw.toLowerCase();
            const statusClass = status === "ok" ? "success" : status === "error" ? "danger" : "secondary";
            const place = item?.place_name || item?.place_cod || "—";
            const placeEscaped = String(place).replace(/"/g, "&quot;");
            return `<button type="button" class="btn btn-sm btn-outline-secondary w-100 text-start mb-1" data-mini-place="${placeEscaped}">
                <span class="fw-semibold me-2">${place}</span>
                <span class="badge text-bg-${statusClass}">${statusRaw}</span>
                <span class="small text-muted ms-2">${formatDate(item?.created_at)}</span>
            </button>`;
        }).join("");
    }

    function applyPlaceData(data, placeCod, fromCache = false) {
        placeTitle.textContent = data.place_name ?? placeCod;
        const placeAdminStatusBadge = document.getElementById("placeAdminStatusBadge");
        if (placeAdminStatusBadge) {
            const s = data.admin_status;
            if (s === "in_work") {
                placeAdminStatusBadge.textContent = "В работе";
                placeAdminStatusBadge.className = "badge bg-warning text-dark";
                placeAdminStatusBadge.classList.remove("d-none");
            } else if (s === "repaired") {
                placeAdminStatusBadge.textContent = "Исправлено";
                placeAdminStatusBadge.className = "badge bg-success";
                placeAdminStatusBadge.classList.remove("d-none");
            } else {
                placeAdminStatusBadge.classList.add("d-none");
                placeAdminStatusBadge.textContent = "";
            }
        }
        const mxFloor = document.getElementById("mxFloor");
        const mxRow = document.getElementById("mxRow");
        const mxSection = document.getElementById("mxSection");
        const mxStatus = document.getElementById("mxStatus");
        const setPlaceValue = (el, value) => {
            if (!el) return;
            el.textContent = value ?? "—";
            el.classList.toggle("place-value-empty", value == null || value === "—");
        };
        setPlaceValue(mxFloor, data.floor != null ? String(data.floor) : null);
        setPlaceValue(mxRow, data.row_num != null ? String(data.row_num) : null);
        setPlaceValue(mxSection, data.section != null ? String(data.section) : null);
        setPlaceValue(mxStatus, data.mx_status != null && data.mx_status !== "" ? String(data.mx_status) : null);
        placeUpdatedLabel.textContent = data.updated_at ? `Обновлено ${formatDate(data.updated_at)}` : "—";
        if (contextBlockChip) {
            const block = extractBlockLabel(data.place_name || data.place_cod || placeCod);
            contextBlockChip.innerHTML = `<i class="bi bi-grid-1x2"></i> Блок: ${block}`;
        }
        if (contextFreshChip) {
            const updated = data.updated_at ? new Date(data.updated_at).getTime() : 0;
            const days = updated ? Math.floor((Date.now() - updated) / 86400000) : null;
            if (days == null || Number.isNaN(days)) {
                contextFreshChip.innerHTML = '<i class="bi bi-database-check"></i> Данные: нет даты';
            } else if (days <= 7) {
                contextFreshChip.innerHTML = `<i class="bi bi-database-check"></i> Данные: свежие (${days} дн)`;
            } else {
                contextFreshChip.innerHTML = `<i class="bi bi-exclamation-circle"></i> Данные: устарели (${days} дн)`;
            }
        }
        state.lastMxCode = data.place_cod;
        state.currentPlace = { place_cod: data.place_cod, place_name: data.place_name, qty_db: data.qty_shk, mx_type: data.mx_type };
        if (repeatMxChip && repeatMxChipLabel) {
            repeatMxChipLabel.textContent = "Повторить: " + (data.place_name || data.place_cod || "").toString().slice(0, 20);
            repeatMxChip.classList.remove("d-none");
        }
        showAlert(placeAlert, fromCache ? "Карточка МХ (из кэша)" : "Карточка МХ загружена", "success");
        placeInput.classList.add("is-valid");
        const placeCard = document.getElementById("placeCard");
        if (placeCard) placeCard.classList.add("is-loaded");
        if (state.scanOnlyMode && placeCardSwipeHint && !fromCache) {
            placeCardSwipeHint.classList.remove("d-none");
            setTimeout(() => placeCardSwipeHint?.classList.add("d-none"), 3500);
        }
        logEvent(`Загружено место ${data.place_name ?? data.place_cod}${fromCache ? " [кэш]" : ""}`, "info");
        const placeCardEl = document.getElementById("placeCard");
        if (placeCardEl) placeCardEl.scrollIntoView({ behavior: "smooth", block: "nearest" });
        loadRouteSuggestions();
    }

    async function loadPlace(placeCod, options = {}) {
        hideAlert(placeAlert);
        if (!placeCod) {
            showAlert(placeAlert, "Введите код МХ");
            return;
        }
        const normalizedInput = normalizePlaceCode(String(placeCod));
        const lookupKey = normalizePlaceKey(normalizedInput || placeCod);
        const nowTs = Date.now();
        if (lookupKey) {
            const inFlight = placeLookupInFlight.get(lookupKey);
            if (inFlight) return inFlight;
            const blockedUntil = placeLookupCooldownUntil.get(lookupKey) || 0;
            if (blockedUntil > nowTs) {
                const cached = await PlaceCache.lookupAsync(placeCod);
                if (cached) {
                    const duplicateState = resolveDuplicateOnLoad(placeCod, cached);
                    if (duplicateState === "cancel") return;
                    applyPlaceData(cached, placeCod, true);
                    showAlert(placeAlert, "Сервер недоступен. Загружено из локального кэша.", "warning");
                    return;
                }
                const waitSec = Math.max(1, Math.ceil((blockedUntil - nowTs) / 1000));
                const msg = `Сервер БД недоступен. Повторите через ${waitSec} с или используйте МХ из локального кэша.`;
                showAlert(placeAlert, msg, "warning");
                logEvent(msg, "warning");
                placeInput.classList.add("is-invalid");
                return;
            }
        }
        if (Date.now() < dbUnavailableUntilTs) {
            const cached = await PlaceCache.lookupAsync(placeCod);
            if (cached) {
                const duplicateState = resolveDuplicateOnLoad(placeCod, cached);
                if (duplicateState === "cancel") return;
                applyPlaceData(cached, placeCod, true);
                showAlert(placeAlert, "БД временно недоступна. Загружено из локального кэша.", "warning");
                return;
            }
            const waitSec = Math.max(1, Math.ceil((dbUnavailableUntilTs - Date.now()) / 1000));
            const msg = `БД недоступна. Повторите через ${waitSec} с или используйте предварительно синхронизированный МХ.`;
            showAlert(placeAlert, msg, "warning");
            logEvent(msg, "warning");
            placeInput.classList.add("is-invalid");
            return;
        }

        placeForm?.classList.add("was-validated");
        placeInput.classList.remove("is-valid", "is-invalid");

        if (!options.skipCache) {
            const cached = await PlaceCache.lookupAsync(placeCod);
            if (cached) {
                const duplicateState = resolveDuplicateOnLoad(placeCod, cached);
                if (duplicateState === "cancel") return;
                applyPlaceData(cached, placeCod, true);
                if (state.allowDuplicateForCurrentPlace) {
                    showAlert(placeAlert, "Повторный скан подтвержден (задвойка)", "warning");
                }
                return;
            }
        }
        if (!navigator.onLine) {
            const cacheSize = PlaceCache.size();
            const msg = cacheSize > 0
                ? "Офлайн: МХ не найден в локальном кэше. Проверьте код или обновите справочник при появлении сети."
                : "Офлайн: кэш пуст. Перед работой без сети выполните синхронизацию справочника.";
            showAlert(placeAlert, msg, "warning");
            logEvent(msg, "warning");
            placeInput.classList.add("is-invalid");
            return;
        }

        const runLookup = async () => {
            const placeCard = document.getElementById("placeCard");
            if (placeCard) placeCard.classList.add("is-loading");
            const { ok, status, data } = await API.get(`/api/place/${encodeURIComponent(placeCod)}`);
            if (placeCard) placeCard.classList.remove("is-loading");
            if (!ok || data.error) {
                const canUseCacheFallback = status === 503 || status === 0 || String(data?.error || "").toLowerCase().includes("нет связи с бд");
                if (canUseCacheFallback) {
                    dbUnavailableUntilTs = Date.now() + DB_UNAVAILABLE_COOLDOWN_MS;
                    if (lookupKey) placeLookupCooldownUntil.set(lookupKey, Date.now() + PLACE_LOOKUP_RETRY_COOLDOWN_MS);
                    const cached = await PlaceCache.lookupAsync(placeCod);
                    if (cached) {
                        const duplicateState = resolveDuplicateOnLoad(placeCod, cached);
                        if (duplicateState === "cancel") return;
                        applyPlaceData(cached, placeCod, true);
                        showAlert(placeAlert, "БД недоступна. Загружено из локального кэша.", "warning");
                        logEvent("Справочник БД недоступен, использован локальный кэш", "warning");
                        return;
                    }
                } else if (lookupKey) {
                    placeLookupCooldownUntil.delete(lookupKey);
                }
                const errorMessage = data.error || "Место не найдено";
                showAlert(placeAlert, errorMessage);
                logEvent(errorMessage, "danger");
                placeInput.classList.add("is-invalid");
                SoundFeedback.playError();
                return;
            }
            dbUnavailableUntilTs = 0;
            if (lookupKey) placeLookupCooldownUntil.delete(lookupKey);
            const duplicateState = resolveDuplicateOnLoad(placeCod, data);
            if (duplicateState === "cancel") return;
            PlaceCache.set(placeCod, data);
            applyPlaceData(data, placeCod, false);
            if (state.allowDuplicateForCurrentPlace) {
                showAlert(placeAlert, "Повторный скан подтвержден (задвойка)", "warning");
            }
        };

        if (!lookupKey) {
            await runLookup();
            return;
        }
        const task = runLookup().finally(() => placeLookupInFlight.delete(lookupKey));
        placeLookupInFlight.set(lookupKey, task);
        return task;
    }

    function clearPlaceCard() {
        placeTitle.textContent = "—";
        const placeAdminStatusBadge = document.getElementById("placeAdminStatusBadge");
        if (placeAdminStatusBadge) {
            placeAdminStatusBadge.classList.add("d-none");
            placeAdminStatusBadge.textContent = "";
        }
        const mxFloor = document.getElementById("mxFloor");
        const mxRow = document.getElementById("mxRow");
        const mxSection = document.getElementById("mxSection");
        const mxStatus = document.getElementById("mxStatus");
        [mxFloor, mxRow, mxSection, mxStatus].forEach((el) => {
            if (el) { el.textContent = "—"; el.classList.add("place-value-empty"); }
        });
        placeUpdatedLabel.textContent = "—";
        if (contextBlockChip) contextBlockChip.innerHTML = '<i class="bi bi-grid-1x2"></i> Блок: —';
        if (contextFreshChip) contextFreshChip.innerHTML = '<i class="bi bi-database-check"></i> Данные: —';
        hideAlert(placeAlert);
        
        // Очищаем тип МХ в state
        if (state.currentPlace) {
            state.currentPlace.mx_type = null;
        }
        
        // Сбрасываем причины расхождения
        updateDiscrepancyReasons(state.currentStatus);
        hideReasonDetail();
        if (discrepancyReasonBlock) discrepancyReasonBlock.classList.add('d-none');
        if (discrepancyReasonSelect) discrepancyReasonSelect.value = '';
        if (otherReasonBlock) otherReasonBlock.classList.add('d-none');
        if (otherReasonInput) otherReasonInput.value = '';
        
        placeInput.value = "";
        placeInput.classList.remove("is-valid", "is-invalid");
        const placeCard = document.getElementById("placeCard");
        if (placeCard) placeCard.classList.remove("is-loaded");
        state.lastMxCode = null;
        state.allowDuplicateForCurrentPlace = false;
        if (duplicateFloorInput) duplicateFloorInput.value = "";
        if (duplicateRowInput) duplicateRowInput.value = "";
        if (duplicateShelfInput) duplicateShelfInput.value = "";
        syncDuplicateDetailsVisibility();
        if (commentInput) commentInput.value = "";
        if (discrepancyReasonSelect) discrepancyReasonSelect.value = "";
        if (otherReasonInput) otherReasonInput.value = "";
        if (discrepancyReasonBlock) discrepancyReasonBlock.classList.add("d-none");
        if (otherReasonBlock) otherReasonBlock.classList.add("d-none");
        setStatus(null);
        clearPhoto();
        state.currentPlace = null;
    }

    function updatePhotoPreview() {
        if (!photoPreview || !photoHint) return;
        if (!state.photos.length) {
            photoPreview.src = "";
            photoPreview.classList.add("d-none");
            photoDropzone?.classList.remove("has-file");
            photoHint.textContent =
                "Нажмите на превью, чтобы удалить фото. Сейчас можно приложить несколько фото подряд.";
            return;
        }
        // Показываем первое фото как основное
        photoPreview.src = state.photos[0];
        photoPreview.classList.remove("d-none");
        photoDropzone?.classList.add("has-file");
        if (state.photos.length === 1) {
            photoHint.textContent = "Нажмите на превью, чтобы удалить фото.";
        } else {
            photoHint.textContent = `Нажмите на превью, чтобы удалить все фото. Прикреплено фото: ${state.photos.length} шт.`;
        }
    }

    function clearPhoto() {
        state.photoData = null;
        state.photos = [];
        state.photoLoading = false;
        if (photoInput) {
            photoInput.value = "";
        }
        updatePhotoPreview();
    }

    /**
     * Сжимает изображение (data URL) до разумного размера для отправки на сервер.
     * На Vercel лимит тела запроса 4.5 MB — без сжатия фото часто не сохраняются.
     */
    function compressImageToDataUrl(dataUrl) {
        return new Promise((resolve, reject) => {
            const img = new Image();
            img.crossOrigin = "anonymous";
            img.onload = () => {
                try {
                    let w = img.naturalWidth || img.width;
                    let h = img.naturalHeight || img.height;
                    if (w <= 0 || h <= 0) {
                        resolve(dataUrl);
                        return;
                    }
                    let scale = 1;
                    if (w > MAX_PHOTO_DIMENSION || h > MAX_PHOTO_DIMENSION) {
                        scale = Math.min(MAX_PHOTO_DIMENSION / w, MAX_PHOTO_DIMENSION / h);
                        w = Math.round(w * scale);
                        h = Math.round(h * scale);
                    }
                    const canvas = document.createElement("canvas");
                    canvas.width = w;
                    canvas.height = h;
                    const ctx = canvas.getContext("2d");
                    if (!ctx) {
                        resolve(dataUrl);
                        return;
                    }
                    ctx.drawImage(img, 0, 0, w, h);
                    let out = canvas.toDataURL("image/jpeg", PHOTO_JPEG_QUALITY);
                    // Если всё ещё слишком большое — уменьшаем качество
                    let bytes = out.length * 0.75;
                    if (bytes > MAX_PHOTO_BASE64_BYTES) {
                        for (let q = PHOTO_JPEG_QUALITY; q >= 0.5 && bytes > MAX_PHOTO_BASE64_BYTES; q -= 0.1) {
                            out = canvas.toDataURL("image/jpeg", Math.max(0.5, q));
                            bytes = out.length * 0.75;
                        }
                    }
                    resolve(out);
                } catch (e) {
                    reject(e);
                }
            };
            img.onerror = () => reject(new Error("Ошибка загрузки изображения"));
            img.src = dataUrl;
        });
    }

    function handlePhotoFiles(files) {
        const arr = Array.from(files || []).filter((f) => f && f.type && f.type.startsWith("image/"));
        if (!arr.length) {
            showAlert(placeAlert, "Выберите изображения", "warning");
            return;
        }
        state.photoLoading = true;
        saveScanBtn?.setAttribute("disabled", "disabled");
        let done = 0;
        const total = arr.length;
        arr.forEach((file) => {
            const reader = new FileReader();
            reader.onload = async () => {
                const dataUrl = reader.result;
                if (!dataUrl) {
                    done++;
                    if (done >= total) {
                        state.photoLoading = false;
                        saveScanBtn?.removeAttribute("disabled");
                        updatePhotoPreview();
                    }
                    return;
                }
                try {
                    const compressed = await compressImageToDataUrl(dataUrl);
                    if (compressed) state.photos.push(compressed);
                } catch (e) {
                    console.warn("Сжатие фото не удалось, отправляем как есть", e);
                    state.photos.push(dataUrl);
                }
                done++;
                if (done >= total) {
                    state.photoLoading = false;
                    saveScanBtn?.removeAttribute("disabled");
                    updatePhotoPreview();
                    logEvent(`Загружено фото: ${total} шт.`, "info");
                }
            };
            reader.onerror = () => {
                done++;
                if (done >= total) {
                    state.photoLoading = false;
                    saveScanBtn?.removeAttribute("disabled");
                    showAlert(placeAlert, "Ошибка чтения файла", "warning");
                }
            };
            reader.readAsDataURL(file);
        });
    }

    function handlePhotoFile(file) {
        if (!file) {
            showAlert(placeAlert, "Файл фото не выбран или браузер ограничил доступ", "warning");
            return;
        }
        handlePhotoFiles([file]);
    }

    async function saveScan() {
        if (saveInFlight) return;
        if (!state.lastMxCode) {
            showAlert(placeAlert, "Сначала отсканируйте МХ");
            logEvent("Попытка сохранения без выбранного МХ", "warning");
            return;
        }
        if (!state.currentStatus) {
            showAlert(placeAlert, "Выберите статус результата");
            logEvent("Выберите статус перед сохранением", "warning");
            return;
        }

        // Если пользователь выбрал файл, но FileReader ещё не закончил чтение – ждём
        if (photoInput?.files?.length && state.photoLoading) {
            showAlert(placeAlert, "Фото ещё загружается, подождите пару секунд");
            return;
        }

        // В режиме «Только скан» для Совпадает/Ошибка не требуем причину
        const skipReasonValidation =
            state.allowDuplicateForCurrentPlace
            || (state.scanOnlyMode && (state.currentStatus === 'ok' || state.currentStatus === 'error'));
        if (!skipReasonValidation && state.currentStatus && state.currentStatus !== 'ok') {
            if (!discrepancyReasonSelect?.value) {
                showAlert(placeAlert, "Выберите причину расхождения");
                logEvent("Выберите причину расхождения", "warning");
                return;
            }
        }

        let duplicateFloor = null;
        let duplicateRow = null;
        let duplicateShelf = null;
        if (state.allowDuplicateForCurrentPlace) {
            duplicateFloor = parseDuplicateField(duplicateFloorInput?.value);
            duplicateRow = parseDuplicateField(duplicateRowInput?.value);
            duplicateShelf = parseDuplicateField(duplicateShelfInput?.value);
            if (duplicateFloor == null || duplicateRow == null || duplicateShelf == null) {
                showAlert(placeAlert, "Для задвойки укажите этаж, ряд и номер стеллажа", "warning");
                logEvent("Для задвойки не заполнены этаж/ряд/стеллаж", "warning");
                duplicateFloorInput?.focus();
                return;
            }
        }

        // Формируем причину: основной текст + подпункт (выпадающий или поле ввода) или «Другое»
        let discrepancyReason = '';
        if (state.allowDuplicateForCurrentPlace) discrepancyReason = 'Задвойка';
        else if (skipReasonValidation && state.currentStatus === 'error') discrepancyReason = null;
        else if (discrepancyReasonSelect?.value) {
            if (discrepancyReasonSelect.value === 'other') {
                discrepancyReason = otherReasonInput?.value?.trim() || 'Другое';
            } else {
                const mainOpt = discrepancyReasonSelect.options[discrepancyReasonSelect.selectedIndex];
                discrepancyReason = mainOpt?.text || discrepancyReasonSelect.value;
                const reasonOpt = getCurrentReasonOption();
                if (reasonOpt?.sub?.length && reasonDetailSelect?.value) {
                    const subOpt = reasonDetailSelect.options[reasonDetailSelect.selectedIndex];
                    if (subOpt?.text) discrepancyReason += ' — ' + subOpt.text;
                } else if (reasonOpt?.detailPlaceholder && reasonDetailInput?.value?.trim()) {
                    discrepancyReason += ' — ' + reasonDetailInput.value.trim();
                }
            }
        }
        
        const payload = {
            badge,
            place_cod: state.lastMxCode,
            fact_qty: null,
            status: state.currentStatus,
            discrepancy_reason: discrepancyReason || null,
            comment: commentInput?.value?.trim() || null,
            force_duplicate: !!state.allowDuplicateForCurrentPlace,
            duplicate_floor: duplicateFloor,
            duplicate_row: duplicateRow,
            duplicate_shelf: duplicateShelf,
            // Для обратной совместимости отправляем первое фото как photo,
            // а также полный массив photos для новой логики на сервере
            photo: state.photos[0] || state.photoData,
            photos: state.photos,
        };
        saveInFlight = true;
        const scanOnlyBtns = document.querySelectorAll(".scan-only-btn");
        saveSpinner?.classList.remove("d-none");
        saveScanBtn?.setAttribute("disabled", "disabled");
        scanOnlyBtns.forEach((b) => b?.setAttribute("disabled", "disabled"));
        let ok = false;
        let data = {};
        try {
            const firstTry = await API.post("/api/scan/complete", payload);
            ok = firstTry.ok;
            data = firstTry.data || {};
        } finally {
            saveSpinner?.classList.add("d-none");
            saveScanBtn?.removeAttribute("disabled");
            scanOnlyBtns.forEach((b) => b?.removeAttribute("disabled"));
            saveInFlight = false;
        }

        if (!ok || data.error || !data.success) {
            const errorMessage = data.error || "Не удалось сохранить результат";
            const isDuplicateInShift = data.code === "duplicate_in_shift" || (data.error && data.error.includes("уже отсканирован в текущей смене"));
            const isQueued = !!data.queued || (data.error && data.error.includes("очередь"));
            if (isDuplicateInShift) {
                const confirmDuplicate = window.confirm(
                    "Такая ячейка уже сканировалась в текущей смене.\nЭто задвойка?"
                );
                if (confirmDuplicate) {
                    payload.force_duplicate = true;
                    state.allowDuplicateForCurrentPlace = true;
                    syncDuplicateDetailsVisibility();
                    payload.duplicate_floor = parseDuplicateField(duplicateFloorInput?.value);
                    payload.duplicate_row = parseDuplicateField(duplicateRowInput?.value);
                    payload.duplicate_shelf = parseDuplicateField(duplicateShelfInput?.value);
                    if (payload.duplicate_floor == null || payload.duplicate_row == null || payload.duplicate_shelf == null) {
                        showAlert(placeAlert, "Подтвердите задвойку и укажите этаж/ряд/стеллаж", "warning");
                        duplicateFloorInput?.focus();
                        return;
                    }
                    const secondTry = await API.post("/api/scan/complete", payload);
                    if (!secondTry.ok || secondTry.data?.error || !secondTry.data?.success) {
                        const secondError = secondTry.data?.error || "Не удалось сохранить задвойку";
                        showAlert(placeAlert, secondError, "danger");
                        logEvent(secondError, "danger");
                        SoundFeedback.playError();
                        return;
                    }
                    data = secondTry.data;
                    ok = secondTry.ok;
                } else {
                    showAlert(placeAlert, "Скан не сохранен: задвойка отменена", "info");
                    logEvent("Задвойка отменена пользователем", "info");
                    focusPlaceInput(true);
                    return;
                }
            }
        }

        if (!ok || data.error || !data.success) {
            const errorMessage = data.error || "Не удалось сохранить результат";
            const isQueued = !!data.queued || (data.error && data.error.includes("очередь"));
            const isDuplicateInShift = data.code === "duplicate_in_shift" || (data.error && data.error.includes("уже отсканирован в текущей смене"));
            showAlert(placeAlert, errorMessage, isDuplicateInShift || isQueued ? "info" : "danger");
            logEvent(errorMessage, isQueued ? "info" : "danger");
            if (!isQueued) SoundFeedback.playError();
            updateOfflineQueueUI();
            if (isQueued) {
                const nextMx = state.quickScanMode && state.suggestions.length
                    ? state.suggestions.find((s) => !state.scannedPlaceCodes.has((s.mx_code || "").toString().trim().toUpperCase()))?.mx_code
                    : null;
                clearPlaceCard();
                loadRouteSuggestions();
                if (nextMx && placeInput) {
                    placeInput.value = nextMx;
                    placeInput.classList.add("is-valid");
                    loadPlace(nextMx);
                }
                focusPlaceInput(true);
            }
            return;
        }

        SoundFeedback.playSuccess();
        hideAlert(placeAlert);
        const result = data.result;
        if (result) {
            rememberScannedPlace(result.place_cod, result.place_name);
            state.recentScans = [result, ...state.recentScans.filter((x) => {
                const a = (x?.place_cod ?? x?.place_name ?? "").toString();
                const b = (result?.place_cod ?? result?.place_name ?? "").toString();
                return a !== b;
            })].slice(0, 3);
            renderMiniRecentScans();
        }
        state.savedCount += 1;
        updateTodaySavedCount();
        if (state.photoData) {
            state.photoUploads += 1;
        }
        showToastMessage("Результат сохранен");
        state.lastSavedAt = Date.now();
        updateLastScanAgoChip();
        if (saveScanBtn) {
            saveScanBtn.classList.add("save-success");
            setTimeout(() => saveScanBtn.classList.remove("save-success"), 600);
        }
        const placeCard = document.getElementById("placeCard");
        if (placeCard) {
            placeCard.classList.add("scan-success");
            setTimeout(() => placeCard.classList.remove("scan-success"), 800);
        }
        logEvent(`Место ${state.lastMxCode} сохранено со статусом ${state.currentStatus}`, "success");
        const nextMx = state.quickScanMode && state.suggestions.length
            ? state.suggestions.find((s) => !state.scannedPlaceCodes.has((s.mx_code || "").toString().trim().toUpperCase()))?.mx_code
            : null;
        setTimeout(() => {
            clearPlaceCard();
            loadRouteSuggestions();
            if (nextMx && placeInput) {
                placeInput.value = nextMx;
                placeInput.classList.add("is-valid");
                loadPlace(nextMx);
            }
            focusPlaceInput(true);
        }, 850);
    }

    function logEvent(text, type = "info") {
        if (!notificationList) return;
        const time = new Date().toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" });
        state.notifications = [{ text, type, time }, ...state.notifications].slice(0, 20);
        notificationList.innerHTML = state.notifications
            .map(
                (item) => `
            <div class="d-flex justify-content-between align-items-start mb-2">
                <span class="text-${item.type === "danger" ? "danger" : item.type === "warning" ? "warning" : "muted"}">
                    ${item.text}
                </span>
                <small class="text-muted">${item.time}</small>
            </div>`
            )
            .join("");
    }

    function updateQrStatus(text, type = "secondary") {
        const el = qrOverlayStatusEl || qrStatusBadge;
        if (!el) return;
        el.textContent = text;
        el.className = `badge text-bg-${type}`;
    }

    function isCameraApiAvailable() {
        return !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia);
    }

    /** Снижаем разрешение потока на телефонах — иначе Android часто даёт 4K и декод QR лагает. */
    function isMobileCameraProfile() {
        if (typeof navigator === "undefined") return false;
        const ua = navigator.userAgent || "";
        return /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(ua);
    }

    function getQrVideoTrack() {
        const root = document.getElementById("qrReaderLive") || qrReaderEl;
        if (!root) return null;
        const video = root.querySelector("video");
        if (!video?.srcObject) return null;
        const tracks = video.srcObject.getVideoTracks();
        return tracks.length ? tracks[0] : null;
    }

    async function setTorch(on) {
        if (!qrVideoTrack) return;
        qrTorchOn = on;
        const track = qrVideoTrack;
        try {
            await track.applyConstraints({ torch: on });
        } catch {
            try {
                await track.applyConstraints({ advanced: [{ torch: on }] });
            } catch {
                /* устройство/браузер не поддерживает torch */
            }
        }
        if (qrTorchIcon) qrTorchIcon.className = on ? "bi bi-flashlight-fill" : "bi bi-flashlight";
        if (qrTorchLabel) qrTorchLabel.textContent = on ? "Выкл. фонарик" : "Фонарик";
        if (qrTorchBtn) qrTorchBtn.title = on ? "Выключить фонарик" : "Включить фонарик";
        if (qrOverlayTorchBtn) {
            const icon = qrOverlayTorchBtn.querySelector(".bi");
            const label = qrOverlayTorchBtn.querySelector("[data-torch-label]");
            if (icon) icon.className = on ? "bi bi-flashlight-fill" : "bi bi-flashlight";
            if (label) label.textContent = on ? "Выкл. фонарик" : "Фонарик";
        }
    }

    async function stopQrScanner() {
        if (qrVideoTrack && qrTorchOn) {
            try {
                await setTorch(false);
            } catch (e) {
                /* noop */
            }
            qrVideoTrack = null;
            qrTorchOn = false;
        }
        if (qrTorchBtn) qrTorchBtn.classList.add("d-none");
        if (qrOverlayTorchBtn) qrOverlayTorchBtn.classList.add("d-none");
        if (qrScanner) {
            try {
                await qrScanner.stop();
            } catch (e) {
                console.warn("QR stop error", e);
            }
            try {
                qrScanner.clear();
            } catch {
                /* noop */
            }
            qrScanner = null;
        }
        qrVideoTrack = null;
        qrTorchOn = false;
        updateQrStatus("Сканер остановлен", "secondary");
    }

    function closeQrOverlay() {
        if (qrScanner) {
            stopQrScanner().catch(() => {});
        } else {
            updateQrStatus("Сканер остановлен", "secondary");
        }
        if (qrOverlayRoot && qrOverlayRoot.parentNode) {
            qrOverlayRoot.parentNode.removeChild(qrOverlayRoot);
        }
        qrOverlayRoot = null;
        qrOverlayStatusEl = null;
        qrOverlayTorchBtn = null;
    }

    async function startQrScanner() {
        if (!openQrScannerBtn && !fabScanBtn) {
            showAlert(placeAlert, "Сканер QR недоступен на этой странице");
            return;
        }

        // Проверяем поддержку браузером доступа к камере
        if (!isCameraApiAvailable()) {
            updateQrStatus("Камера недоступна", "danger");
            showAlert(
                placeAlert,
                "Этот браузер или политика безопасности не разрешают доступ к камере. " +
                    "Используйте сканер штрихкодов или ручной ввод.",
                "warning"
            );
            return;
        }

        // На Android Chrome камера работает только по HTTPS (secure context)
        const isSecureContext = window.isSecureContext || (location.protocol === "https:") || (location.hostname === "localhost" && location.port !== "80");
        if (!isSecureContext) {
            updateQrStatus("Камера недоступна по HTTP", "danger");
            showAlert(
                placeAlert,
                "Камера доступна только по HTTPS. Откройте сайт по адресу https://… или используйте сканер штрихкодов/ручной ввод.",
                "danger"
            );
            return;
        }

        // Проверяем, что модуль Html5Qrcode загружен
        if (typeof Html5Qrcode === "undefined") {
            updateQrStatus("Модуль сканера не загружен", "danger");
            showAlert(
                placeAlert,
                "Модуль сканера камеры не загружен. Сообщите администратору или попробуйте обновить страницу (Ctrl+F5).",
                "danger"
            );
            return;
        }

        closeQrOverlay();

        const overlay = document.createElement("div");
        overlay.className = "qr-custom-overlay";
        overlay.setAttribute("role", "dialog");
        overlay.setAttribute("aria-label", "Сканер QR-кода");
        overlay.innerHTML = [
            '<div class="qr-custom-overlay__panel">',
            '<button type="button" class="btn btn-sm btn-outline-secondary qr-custom-overlay__close" aria-label="Закрыть">Закрыть</button>',
            '<h5 class="qr-custom-overlay__title">QR-код</h5>',
            '<p class="small text-muted mb-2">Наведите камеру на QR-код</p>',
            '<div class="qr-custom-overlay__reader"><div id="qrReaderLive" class="border rounded"></div></div>',
            '<span class="badge text-bg-secondary qr-custom-overlay__status" id="qrOverlayStatus">Инициализация</span>',
            '<div class="qr-custom-overlay__footer">',
            '<button type="button" class="btn btn-outline-secondary btn-sm d-none" id="qrOverlayTorchBtn" title="Фонарик"><i class="bi bi-flashlight"></i> <span data-torch-label>Фонарик</span></button>',
            '<button type="button" class="btn btn-secondary btn-sm qr-custom-overlay__close">Закрыть</button>',
            "</div></div>",
        ].join("");
        document.body.appendChild(overlay);
        qrOverlayRoot = overlay;
        qrOverlayStatusEl = document.getElementById("qrOverlayStatus");
        qrOverlayTorchBtn = document.getElementById("qrOverlayTorchBtn");

        const closeBtn = () => closeQrOverlay();
        overlay.querySelectorAll(".qr-custom-overlay__close").forEach((btn) => btn.addEventListener("click", closeBtn));
        overlay.addEventListener("click", (e) => { if (e.target === overlay) closeBtn(); });
        if (qrOverlayTorchBtn) {
            qrOverlayTorchBtn.addEventListener("click", () => {
                if (!qrVideoTrack) return;
                void setTorch(!qrTorchOn);
            });
        }

        updateQrStatus("Инициализация камеры...", "warning");

        if (qrScanner) {
            await stopQrScanner();
        }

        qrScanner = new Html5Qrcode("qrReaderLive");
        try {
            const cameras = await Html5Qrcode.getCameras();
            if (!cameras || !cameras.length) {
                throw new Error("Камера не найдена. Разрешите доступ к камере в настройках браузера/устройства.");
            }

            // Пытаемся выбрать тыльную камеру:
            // 1) по facingMode=environment (если браузер поддерживает),
            // 2) по label (back/rear/тыл/задняя и т.п.),
            // 3) иначе — первая доступная.
            const lower = (s) => (s || "").toLowerCase();
            const backCamera =
                cameras.find((cam) => {
                    const label = lower(cam.label);
                    return (
                        label.includes("back") ||
                        label.includes("rear") ||
                        label.includes("зад") ||
                        label.includes("тыл") ||
                        label.includes("environment")
                    );
                }) || cameras[0];

            const readerLive = document.getElementById("qrReaderLive");
            await new Promise((resolve) => {
                requestAnimationFrame(() => requestAnimationFrame(resolve));
            });
            const readerWidth =
                readerLive?.getBoundingClientRect?.().width || readerLive?.offsetWidth || 280;
            const qrbox = Math.min(Math.max(Math.round(readerWidth * 0.9), 220), 360);

            const mobileCam = isMobileCameraProfile();
            const buildScanConfig = (withMobileVideoLimits) => {
                const fps = mobileCam ? 12 : 10;
                const cfg = { fps, qrbox };
                if (mobileCam && withMobileVideoLimits) {
                    cfg.videoConstraints = {
                        width: { max: 1280 },
                        height: { max: 720 },
                        frameRate: { ideal: 24, max: 30 },
                    };
                }
                return cfg;
            };

            const onScan = (decodedText) => {
                if (!decodedText) return;
                handleQrResult(decodedText);
            };
            const onError = () => {};

            const tryStart = async (cameraConfig) => {
                const scanAttempts = mobileCam
                    ? [buildScanConfig(true), buildScanConfig(false)]
                    : [buildScanConfig(false)];
                let lastErr = null;
                for (const scanCfg of scanAttempts) {
                    try {
                        await qrScanner.start(cameraConfig, scanCfg, onScan, onError);
                        return;
                    } catch (e) {
                        lastErr = e;
                        try {
                            await qrScanner.stop();
                        } catch {
                            /* noop */
                        }
                        try {
                            qrScanner.clear();
                        } catch {
                            /* noop */
                        }
                    }
                }
                throw lastErr;
            };

            // Сначала пробуем тыльную камеру (на Android часто нужна именно она для QR)
            let lastError = null;
            const configsToTry = [];
            if ("mediaDevices" in navigator && "getUserMedia" in navigator.mediaDevices) {
                configsToTry.push({ facingMode: "environment" });
            }
            if (backCamera.id) {
                configsToTry.push({ deviceId: { exact: backCamera.id } });
            }
            if (cameras[0] && cameras[0].id) {
                configsToTry.push(cameras[0].id);
            }
            for (const config of configsToTry) {
                try {
                    await tryStart(config);
                    lastError = null;
                    break;
                } catch (e) {
                    lastError = e;
                }
            }
            if (lastError) {
                const msg = lastError?.message || String(lastError);
                const isDenied = /denied|not allowed|permission|NotAllowedError/i.test(msg);
                throw new Error(
                    isDenied
                        ? "Доступ к камере запрещён. Разрешите камеру в настройках сайта (иконка замка/инфо в адресной строке) и обновите страницу."
                        : msg
                );
            }
            updateQrStatus("Наведите камеру на QR-код", "success");
            // Поддержка фонарика: ищем видео-трек после запуска сканера
            setTimeout(() => {
                const track = getQrVideoTrack();
                if (track && typeof track.getCapabilities === "function") {
                    const caps = track.getCapabilities();
                    if (caps && caps.torch) {
                        qrVideoTrack = track;
                        qrTorchOn = false;
                        if (qrOverlayTorchBtn) {
                            qrOverlayTorchBtn.classList.remove("d-none");
                            const icon = qrOverlayTorchBtn.querySelector(".bi");
                            const label = qrOverlayTorchBtn.querySelector("[data-torch-label]");
                            if (icon) icon.className = "bi bi-flashlight";
                            if (label) label.textContent = "Фонарик";
                        } else if (qrTorchBtn) {
                            qrTorchBtn.classList.remove("d-none");
                            qrTorchIcon.className = "bi bi-flashlight";
                            qrTorchLabel.textContent = "Фонарик";
                        }
                    }
                }
            }, 300);
        } catch (error) {
            console.error("QR scanner error", error);
            updateQrStatus(error?.message || "Ошибка камеры", "danger");
            const hint = /HTTPS|secure|https/i.test(error?.message || "")
                ? ""
                : " Убедитесь, что сайт открыт по HTTPS и доступ к камере разрешён.";
            showAlert(
                placeAlert,
                "Не удалось запустить камеру." + hint + " Попробуйте другой браузер (Chrome) или используйте сканер штрихкодов/ручной ввод.",
                "danger"
            );
        }
    }

    function handleQrResult(decodedText) {
        if (!decodedText) {
            updateQrStatus("QR не содержит данных", "danger");
            return;
        }

        // Извлекаем код МХ из разных форматов (PLCE, URL, чистый код).
        const placeCod = extractPlaceCodeFromQr(decodedText);
        if (!placeCod) {
            updateQrStatus("QR не содержит корректный код МХ", "danger");
            return;
        }
        placeInput.value = placeCod;
        showToastMessage("QR-код считан");
        closeQrOverlay();
        loadPlace(placeCod);
    }

    async function startNewShift() {
        try {
            sessionStorage.setItem(SHIFT_START_KEY, String(Date.now()));
        } catch (e) {}
        try {
            const { ok } = await API.post("/api/user/shift/start", { badge });
            if (!ok) logEvent("Смена сброшена локально; сервер не обновил границу смены", "warning");
        } catch (err) {
            logEvent("Смена сброшена локально; ошибка связи с сервером", "warning");
        }
        state.scannedPlaceCodes.clear();
        clearPlaceCard();
        showAlert(placeAlert, "Новая смена начата. Отчёты считаются с этого момента.", "info");
        logEvent("Начата новая смена", "info");
    }
    const newShiftConfirmModalEl = document.getElementById("newShiftConfirmModal");
    const newShiftConfirmBtn = document.getElementById("newShiftConfirmBtn");
    const newShiftConfirmModal = newShiftConfirmModalEl && typeof bootstrap !== "undefined" ? new bootstrap.Modal(newShiftConfirmModalEl) : null;
    function showNewShiftConfirm() {
        if (newShiftConfirmModal) newShiftConfirmModal.show();
        else startNewShift();
    }
    newShiftConfirmBtn?.addEventListener("click", () => {
        if (newShiftConfirmModal) newShiftConfirmModal.hide();
        startNewShift();
    });
    newShiftBtn?.addEventListener("click", showNewShiftConfirm);
    newShiftBtnScanOnly?.addEventListener("click", showNewShiftConfirm);

    refreshRouteBtn?.addEventListener("click", () => {
        loadRouteSuggestions();
        logEvent("Маршрут обновлён", "info");
    });
    routeFilterSelect?.addEventListener("change", () => {
        state.routeFilter = routeFilterSelect.value || "all";
        renderRouteMap();
    });
    quickScanModeCheck?.addEventListener("change", () => {
        state.quickScanMode = !!quickScanModeCheck?.checked;
    });
    syncCatalogBtn?.addEventListener("click", () => {
        openSyncCatalogModal();
    });
    syncCatalogBlocksList?.addEventListener("click", async (event) => {
        const btn = event.target.closest("[data-sync-group]");
        if (!btn) return;
        const groupCode = String(btn.dataset.syncGroup || "").toUpperCase();
        if (!groupCode) return;
        if (syncSelectedGroupCodes.has(groupCode)) syncSelectedGroupCodes.delete(groupCode);
        else syncSelectedGroupCodes.add(groupCode);
        renderSyncBlocksSelector();
        void loadSyncFloorsForSelectedBlocks();
    });
    syncCatalogFloorsList?.addEventListener("click", (event) => {
        const btn = event.target.closest("[data-sync-floor]");
        if (!btn) return;
        const floor = String(btn.dataset.syncFloor || "").toUpperCase();
        if (!floor) return;
        // Выбор одного этажа за раз, чтобы не перегружать кэш.
        syncSelectedFloors = new Set([floor]);
        renderSyncFloorsSelector();
    });
    syncCatalogApplyBtn?.addEventListener("click", () => {
        const picked = getSelectedSourceBlocks();
        const pickedFloors = Array.from(syncSelectedFloors);
        if (!picked.length) {
            showToastMessage("Выберите хотя бы один блок", "warning");
            return;
        }
        if (!pickedFloors.length) {
            showToastMessage("Выберите этаж", "warning");
            return;
        }
        if (syncCatalogModal) syncCatalogModal.hide();
        syncPlacesInChunks({ forceFull: true, silent: false, blocks: picked, floors: pickedFloors });
    });
    syncCatalogFullBtn?.addEventListener("click", () => {
        if (syncCatalogModal) syncCatalogModal.hide();
        syncPlacesInChunks({ forceFull: true, silent: false, blocks: [] });
    });
    refreshPlaceBtn?.addEventListener("click", (event) => {
        if (state.lastMxCode) {
            loadPlace(state.lastMxCode, { skipCache: true });
        } else {
            // Если МХ ещё не открыт — используем кнопку как ручной запуск справочника (chunk sync).
            syncPlacesInChunks({ forceFull: !!event?.shiftKey, silent: false });
        }
    });
    clearPlaceBtn?.addEventListener("click", clearPlaceCard);
    repeatMxChip?.addEventListener("click", () => {
        if (state.lastMxCode && placeInput) {
            placeInput.value = state.lastMxCode;
            placeInput.classList.add("is-valid");
            loadPlace(state.lastMxCode);
        }
    });
    historyReloadBtn?.addEventListener("click", () => {
        loadHistory();
        logEvent("История сканов обновлена", "info");
    });
    historyShowTodayBtn?.addEventListener("click", () => {
        const today = new Date().toISOString().slice(0, 10);
        if (historyFromInput) historyFromInput.value = today;
        if (historyToInput) historyToInput.value = today;
        loadHistory();
    });
    historyTableBody?.addEventListener("click", (event) => {
        const btn = event.target.closest("[data-history-today]");
        if (!btn) return;
        const today = new Date().toISOString().slice(0, 10);
        if (historyFromInput) historyFromInput.value = today;
        if (historyToInput) historyToInput.value = today;
        loadHistory();
    });
    miniRecentScansList?.addEventListener("click", (event) => {
        const btn = event.target.closest("[data-mini-place]");
        if (!btn || !placeInput) return;
        const place = btn.dataset.miniPlace || "";
        if (!place) return;
        placeInput.value = place;
        placeInput.classList.add("is-valid");
        loadPlace(place);
        focusPlaceInput(true);
    });
    historyDownloadBadBtn?.addEventListener("click", () => {
        const params = new URLSearchParams();
        params.set("badge", badge);
        const from = historyFromInput?.value;
        const to = historyToInput?.value;
        if (from) params.set("from", from);
        if (to) params.set("to", to);
        const url = `/api/user/history/export?${params.toString()}`;
        window.location.href = url;
        logEvent("Скачивание проблемных сканов", "info");
    });
    clearLogBtn?.addEventListener("click", () => {
        state.notifications = [];
        if (notificationList) notificationList.innerHTML = "Пока событий нет";
    });

    statusButtonsContainer?.addEventListener("click", (event) => {
        const target = event.target.closest("[data-status]");
        if (!target) return;
        setStatus(target.dataset.status);
    });
    quickReasonChips?.addEventListener("click", (event) => {
        const chip = event.target.closest("[data-reason-chip]");
        if (!chip || !discrepancyReasonSelect) return;
        discrepancyReasonSelect.value = chip.dataset.reasonChip || "";
        discrepancyReasonSelect.dispatchEvent(new Event("change", { bubbles: true }));
    });
    quickErrorGrid?.addEventListener("click", (event) => {
        const btn = event.target.closest("[data-quick-error]");
        if (!btn || !discrepancyReasonSelect) return;
        const reason = btn.dataset.quickError || "";
        // Один клик: переводим в статус Ошибка и сразу выбираем типовую причину.
        setStatus("error");
        discrepancyReasonSelect.value = reason;
        discrepancyReasonSelect.dispatchEvent(new Event("change", { bubbles: true }));
        const label = btn.textContent?.trim() || "ошибка";
        showToastMessage(`Выбрано: ${label}`, "info");
    });
    emptyStateScanNowBtn?.addEventListener("click", () => startQrScanner());
    
    // При выборе причины показываем подпункт (выпадающий список или поле ввода) или «Другое»
    function getCurrentReasonOption() {
        const status = state.currentStatus;
        const list = status && REASONS_BY_STATUS[status] ? REASONS_BY_STATUS[status] : [];
        const val = discrepancyReasonSelect?.value;
        return list.find(r => r.value === val) || null;
    }

    discrepancyReasonSelect?.addEventListener("change", (event) => {
        const value = event.target.value;
        hideReasonDetail();
        if (value === 'other') {
            if (otherReasonBlock) otherReasonBlock.classList.remove('d-none');
            otherReasonInput?.focus();
        } else {
            if (otherReasonBlock) otherReasonBlock.classList.add('d-none');
            if (otherReasonInput) otherReasonInput.value = '';
            const reasonOpt = getCurrentReasonOption();
            if (reasonOpt && (reasonOpt.sub?.length || reasonOpt.detailPlaceholder))
                showReasonDetail(reasonOpt);
        }
    });

    // Открываем выбор файла ТОЛЬКО по кнопке "Выбрать",
    // чтобы не было двойного срабатывания (клик по кнопке + по дропзоне)
    photoSelectBtn?.addEventListener("click", () => photoInput?.click());
    photoDropzone?.addEventListener("dragover", (event) => {
        event.preventDefault();
        photoDropzone.classList.add("dragover");
    });
    photoDropzone?.addEventListener("dragleave", () => photoDropzone.classList.remove("dragover"));
    photoDropzone?.addEventListener("drop", (event) => {
        event.preventDefault();
        photoDropzone.classList.remove("dragover");
        const files = event.dataTransfer?.files;
        if (files?.length) handlePhotoFiles(files);
    });
    photoInput?.addEventListener("change", (event) => {
        const files = event.target?.files || photoInput?.files;
        if (files?.length) handlePhotoFiles(files);
    });
    photoPreview?.addEventListener("click", (event) => {
        event.stopPropagation();
        clearPhoto();
    });
    saveScanBtn?.addEventListener("click", saveScan);
    openQrScannerBtn?.addEventListener("click", startQrScanner);
    fabScanBtn?.addEventListener("click", startQrScanner);
    document.addEventListener("keydown", (e) => {
        const key = (e.key || "").toLowerCase();
        const target = e.target;
        const isEditable = !!(
            target &&
            (target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.tagName === "SELECT" || target.isContentEditable)
        );

        if (key === "escape") {
            if (qrOverlayRoot && document.body.contains(qrOverlayRoot)) {
                closeQrOverlay();
                return;
            }
            clearPlaceCard();
            focusPlaceInput();
            return;
        }

        if ((e.ctrlKey || e.metaKey) && key === "enter") {
            e.preventDefault();
            if (!saveScanBtn?.disabled) saveScan();
            return;
        }

        if (isEditable || e.ctrlKey || e.metaKey || e.altKey) return;

        if (key === "1") {
            e.preventDefault();
            setStatus("ok");
        } else if (key === "2") {
            e.preventDefault();
            setStatus("error");
        } else if (key === "3") {
            e.preventDefault();
            setStatus("missing");
        }
    });
    qrTorchBtn?.addEventListener("click", () => {
        if (!qrVideoTrack) return;
        void setTorch(!qrTorchOn);
    });

    placeInput?.addEventListener("paste", (event) => {
        setTimeout(() => {
            const v = placeInput.value;
            if (v && /^PLCE\s*/i.test(v)) {
                placeInput.value = normalizePlaceCode(v) || v.replace(/^PLCE\s*/i, "").trim();
            }
        }, 0);
    });

    placeForm?.addEventListener("submit", (event) => {
        event.preventDefault();
        if (!placeForm.checkValidity()) {
            placeForm.classList.add("was-validated");
            return;
        }
        const placeCod = normalizePlaceCode(placeInput.value) || placeInput.value.trim().toUpperCase();
        if (!placeCod || !/^[А-ЯЁA-Z0-9.\-]+$/i.test(placeCod)) {
            showAlert(placeAlert, "Код МХ: только буквы, цифры, точки и дефис");
            return;
        }
        placeInput.value = placeCod;
        loadPlace(placeCod);
    });

    placeInput?.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
            event.preventDefault();
            const placeCod = normalizePlaceCode(placeInput.value) || placeInput.value.trim().toUpperCase();
            if (placeCod && /^[А-ЯЁA-Z0-9.\-]+$/i.test(placeCod)) {
                placeInput.value = placeCod;
                loadPlace(placeCod);
            } else {
                placeForm.classList.add("was-validated");
            }
        }
    });

    (() => {
        const placeCard = document.getElementById("placeCard");
        if (!placeCard) return;
        const SWIPE_THRESHOLD = 60;
        let startX = 0;
        let pointerId = null;

        function getClientX(e) {
            if (e.touches?.length) return e.touches[0].clientX;
            if (e.changedTouches?.length) return e.changedTouches[0].clientX;
            return e.clientX ?? 0;
        }

        function onPointerStart(e) {
            if (!state.lastMxCode) return;
            startX = getClientX(e);
            pointerId = e.pointerId ?? e.touches?.[0]?.identifier;
            placeCard.classList.remove("swipe-left", "swipe-right");
        }

        function onPointerMove(e) {
            if (pointerId == null || startX === 0) return;
            const id = e.pointerId ?? e.touches?.[0]?.identifier;
            if (id !== pointerId) return;
            const x = getClientX(e);
            const diff = x - startX;
            placeCard.classList.remove("swipe-left", "swipe-right");
            if (diff > 20) placeCard.classList.add("swipe-right");
            else if (diff < -20) placeCard.classList.add("swipe-left");
        }

        function onPointerEnd(e) {
            if (pointerId == null || startX === 0) return;
            const id = e.pointerId ?? e.changedTouches?.[0]?.identifier;
            if (id !== pointerId) return;
            const endX = getClientX(e);
            const diff = endX - startX;
            placeCard.classList.remove("swipe-left", "swipe-right");
            if (Math.abs(diff) > SWIPE_THRESHOLD) {
                if (diff > 0) {
                    setStatus("ok");
                    if (state.scanOnlyMode) saveScan();
                    else logEvent("Свайп: Совпадает", "info");
                } else {
                    setStatus("error");
                    if (state.scanOnlyMode) saveScan();
                    else logEvent("Свайп: Ошибка", "info");
                }
            }
            pointerId = null;
            startX = 0;
        }

        function start(e) {
            if (e.type.startsWith("touch")) onPointerStart(e);
            else if (e.pointerType !== "mouse" || e.buttons === 1) onPointerStart(e);
        }
        function move(e) {
            if (e.type.startsWith("touch")) onPointerMove(e);
            else if (e.pointerId === pointerId) onPointerMove(e);
        }
        function end(e) {
            if (e.type.startsWith("touch")) onPointerEnd(e);
            else if (e.pointerId === pointerId) onPointerEnd(e);
        }
        placeCard.addEventListener("touchstart", start, { passive: true });
        placeCard.addEventListener("touchmove", move, { passive: true });
        placeCard.addEventListener("touchend", end, { passive: true });
        placeCard.addEventListener("pointerdown", (e) => { if (e.pointerType === "mouse") placeCard.setPointerCapture?.(e.pointerId); start(e); });
        placeCard.addEventListener("pointermove", move);
        placeCard.addEventListener("pointerup", end);
        placeCard.addEventListener("pointercancel", () => { placeCard.classList.remove("swipe-left", "swipe-right"); pointerId = null; startX = 0; });
    })();

    scanOnlyToggle?.addEventListener("change", () => {
        state.scanOnlyMode = scanOnlyToggle.checked;
        applyScanOnlyMode();
    });

    scanOnlyOkBtn?.addEventListener("click", () => {
        if (!state.lastMxCode) { showAlert(placeAlert, "Сначала отсканируйте МХ"); return; }
        setStatus("ok");
        saveScan();
    });
    scanOnlyErrorBtn?.addEventListener("click", () => {
        if (!state.lastMxCode) { showAlert(placeAlert, "Сначала отсканируйте МХ"); return; }
        setStatus("error");
        saveScan();
    });

    applyScanOnlyMode();
    setTodayDates();
    updateTodaySavedCount();
    loadRouteSuggestions();
    renderLastPlaces();
    updateStatusLabel();
    updateOnlineStatus();
    updateOfflineQueueUI();
    loadHistory();
    refreshAnimations();
    updateLastScanAgoChip();
    renderMiniRecentScans();
    setInterval(updateLastScanAgoChip, 30000);
    focusPlaceInput();
    setTimeout(preloadSyncCatalogMeta, 700);

    offlineQueueSyncBtn?.addEventListener("click", async () => {
        if (!navigator.onLine) {
            showToastMessage("Нет сети. Подключитесь к интернету.", "warning");
            return;
        }
        offlineQueueSyncBtn.disabled = true;
        try {
            const result = await syncOfflineQueue();
            updateOfflineQueueUI();
            const left = result?.left ?? OfflineQueue.getAll().length;
            const conflicts = result?.conflicts ?? OfflineQueue.getAll().filter((x) => x.status === "conflict").length;
            if (left === 0) showToastMessage("Очередь синхронизирована", "success");
            else if (conflicts > 0) showToastMessage(`В очереди осталось: ${left}. Конфликтов: ${conflicts}.`, "warning");
            else showToastMessage(`Отправлено. В очереди осталось: ${left}`, "info");
        } finally {
            offlineQueueSyncBtn.disabled = false;
            updateOfflineQueueUI();
        }
    });

    window.addEventListener("online", async () => {
        updateOnlineStatus();
        const result = await syncOfflineQueue();
        updateOfflineQueueUI();
        const sent = result?.sent || 0;
        const conflicts = result?.conflicts || 0;
        if (sent > 0) {
            const msg = sent === 1 ? "Сеть появилась. Отправлен 1 скан." : `Сеть появилась. Отправлено ${sent} сканов.`;
            showToastMessage(msg, "success");
        } else if (conflicts > 0) {
            showToastMessage(`Сеть появилась, но есть конфликтные записи: ${conflicts}`, "warning");
        } else if (OfflineQueue.getAll().length === 0) {
            showToastMessage("Очередь синхронизирована", "success");
        }
    });
    window.addEventListener("offline", updateOnlineStatus);
    window.setInterval(async () => {
        if (!navigator.onLine) return;
        if (document.hidden) return;
        if (!OfflineQueue.getAll().length) return;
        const result = await syncOfflineQueue();
        updateOfflineQueueUI();
        if ((result?.sent || 0) > 0) updateOnlineStatus();
    }, OFFLINE_AUTO_SYNC_INTERVAL_MS);
}

function initAdminLoginPage() {
    const form = document.getElementById("adminLoginForm");
    if (!form) return;

    const passwordInput = document.getElementById("adminPasswordInput");
    const alertBox = document.getElementById("adminLoginAlert");
    const toggleBtn = document.getElementById("toggleAdminPassword");

    toggleBtn?.addEventListener("click", () => {
        if (!passwordInput) return;
        passwordInput.type = passwordInput.type === "password" ? "text" : "password";
        toggleBtn.innerHTML = passwordInput.type === "password" ? '<i class="bi bi-eye"></i>' : '<i class="bi bi-eye-slash"></i>';
    });

    form.addEventListener("submit", async (event) => {
        event.preventDefault();
        hideAlert(alertBox);

        const password = passwordInput.value.trim();
        if (!password) {
            showAlert(alertBox, "Введите пароль");
            return;
        }

        const { ok, data } = await API.post("/api/admin/auth", {
            badge: "ADMIN",
            password,
        });

        if (!ok || !data.success) {
            showAlert(alertBox, data.error || "Ошибка авторизации");
            return;
        }

        sessionStorage.setItem("isAdmin", "1");
        showAlert(alertBox, "Авторизация успешна, перенаправляем...", "success");
        window.location.href = "/admin/dashboard";
    });
}

function renderList(container, items, renderItem) {
    if (!container) return;
    if (!items?.length) {
        const isListGroup = container.classList.contains("list-group");
        container.innerHTML = isListGroup
            ? '<div class="list-group-item text-muted small admin-empty-list"><i class="bi bi-inbox me-2"></i>Нет данных</div>'
            : '<div class="text-muted small">Нет данных</div>';
        return;
    }
    container.innerHTML = items.map(renderItem).join("");
}

function initAdminDashboard() {
    const isAdmin = sessionStorage.getItem("isAdmin") === "1";
    if (!isAdmin) {
        console.warn("Admin session not detected");
    }

    const overallScannedValue = document.getElementById("overallScannedValue");
    const overallDiscrepancyValue = document.getElementById("overallDiscrepancyValue");
    const overallEmployeesValue = document.getElementById("overallEmployeesValue");
    const overallPlacesValue = document.getElementById("overallPlacesValue");
    const overallAccuracyLabel = document.getElementById("overallAccuracyLabel");
    const weeklyDelta = document.getElementById("weeklyDelta");
    const topEmployeesList = document.getElementById("topEmployeesList");
    const discrepancyTypesList = document.getElementById("discrepancyTypesList");
    const problemZonesTableBody = document.getElementById("problemZonesTableBody");
    const activeTasksTableBody = document.getElementById("activeTasksTableBody");
    const adminActivityList = document.getElementById("adminActivityList");
    const clearAdminLogBtn = document.getElementById("clearLogBtn") || document.getElementById("clearAdminLog");
    const refreshAdminBtn = document.getElementById("refreshAdminBtn");
    const photoGalleryCount = document.getElementById("photoGalleryCount");
    const adminPhotoGrid = document.getElementById("adminPhotoGrid");
    const dailyChartCanvas = document.getElementById("dailyChart");
    const statusChartCanvas = document.getElementById("statusChart");
    const assignTaskForm = document.getElementById("assignTaskForm");
    const assignTaskModalEl = document.getElementById("assignTaskModal");
    const extendTaskButtons = document.getElementById("activeTasksTableBody");
    const reportsTableBody = document.getElementById("reportsTableBody");
    const reportsModalEl = document.getElementById("reportsModal");
    const photoPreviewModalEl = document.getElementById("photoPreviewModal");
    const photoPreviewImg = document.getElementById("photoPreviewImg");
    const photoPreviewMeta = document.getElementById("photoPreviewMeta");
    const photoPreviewModalLabel = document.getElementById("photoPreviewModalLabel");
    const photoPreviewThumbs = document.getElementById("photoPreviewThumbs");
    const deletePhotoBtn = document.getElementById("deletePhotoBtn");
    const downloadPhotoBtn = document.getElementById("downloadPhotoBtn");
    const qualityTableBody = document.getElementById("qualityTableBody");
    const qualityReviewList = document.getElementById("qualityReviewList");
    const qualityReviewForm = document.getElementById("qualityReviewForm");
    const ticketsBoard = document.getElementById("ticketsBoard");
    const scrollToProblemZonesBtn = document.getElementById("scrollToProblemZones");
    const scrollToTicketsBtn = document.getElementById("scrollToTickets");
    const problemZonesBlock = document.getElementById("problemZonesBlock");
    const ticketsBlock = document.getElementById("ticketsBlock");
    const dashboardBlockErrorsSelect = document.getElementById("dashboardBlockErrorsSelect");
    const dashboardBlockErrorsBody = document.getElementById("dashboardBlockErrorsBody");
    const dashboardBlockErrorsEmpty = document.getElementById("dashboardBlockErrorsEmpty");
    const dashboardBlockErrorsTableWrap = document.getElementById("dashboardBlockErrorsTableWrap");
    const dashboardBlockErrorsCount = document.getElementById("dashboardBlockErrorsCount");
    const adminStatusBar = document.getElementById("adminStatusBar");
    const adminStatusBarText = document.getElementById("adminStatusBarText");
    const adminStatusChipAccuracy = document.getElementById("adminStatusChipAccuracy");
    const adminStatusChipWorkload = document.getElementById("adminStatusChipWorkload");
    const adminStatusChipRisk = document.getElementById("adminStatusChipRisk");
    const employeesTableBody = document.getElementById("employeesTableBody");
    const hourlyChartCanvas = document.getElementById("hourlyChart");
    const adminPeriodSelect = document.getElementById("adminPeriodSelect");
    const exportEmployeesBtn = document.getElementById("exportEmployeesBtn");
    const ticketsCountBadge = document.getElementById("ticketsCountBadge");
    const hourHeatmap = document.getElementById("hourHeatmap");

    let dailyChartInstance = null;
    let statusChartInstance = null;
    let hourlyChartInstance = null;
    let assignTaskModal = null;
    let reportsModal = null;
    let photoPreviewModal = null;

    function getAdminPeriod() {
        return (adminPeriodSelect && adminPeriodSelect.value) || "7d";
    }

    async function loadAdminStats() {
        const period = getAdminPeriod();
        const { ok, data } = await API.get(`/api/admin/stats?period=${encodeURIComponent(period)}`);
        if (!ok || data.error) {
            if (overallAccuracyLabel) overallAccuracyLabel.textContent = data.error || "Ошибка загрузки статистики";
            return;
        }

        const overall = data.overall || {};
        const employees = data.employees || [];
        const discrepancyTypes = data.discrepancy_types || [];
        const hourlyStats = data.hourly_stats || [];

        overallScannedValue.textContent = overall.total_scanned ?? 0;
        overallDiscrepancyValue.textContent = overall.with_discrepancy ?? 0;
        overallEmployeesValue.textContent = overall.total_employees ?? 0;
        overallPlacesValue.textContent = overall.unique_places ?? 0;

        document.querySelectorAll(".admin-kpi-card").forEach((card) => {
            card.classList.add("kpi-just-loaded");
            setTimeout(() => card.classList.remove("kpi-just-loaded"), 500);
        });

        const accuracy = overall.total_scanned
            ? (((overall.no_discrepancy || 0) / overall.total_scanned) * 100).toFixed(1)
            : "0.0";
        overallAccuracyLabel.textContent = `Точность ${accuracy}%`;
        if (weeklyDelta) {
            weeklyDelta.textContent = `${accuracy}% точность`;
            weeklyDelta.className = `badge text-bg-${accuracy >= 95 ? "success" : accuracy >= 85 ? "warning" : "danger"}`;
        }

        if (adminStatusBar && adminStatusBarText) {
            adminStatusBar.classList.remove("d-none");
            const total = overall.total_scanned ?? 0;
            const withDisc = overall.with_discrepancy ?? 0;
            const activeEmps = overall.total_employees ?? 0;
            adminStatusBarText.textContent = `Период: ${total} сканов • ${withDisc} с расхождениями • ${activeEmps} сотрудников`;

            if (adminStatusChipAccuracy) adminStatusChipAccuracy.textContent = `Точность ${accuracy}%`;
            if (adminStatusChipWorkload) adminStatusChipWorkload.textContent = `Нагрузка: ${total} сканов`;
            if (adminStatusChipRisk) {
                adminStatusChipRisk.textContent = accuracy >= 95 ? "Низкие риски" : accuracy >= 85 ? "Средние риски" : "Высокие риски";
            }
        }

        if (employeesTableBody) {
            employeesTableBody.innerHTML = employees.length
                ? employees
                    .map(
                        (emp) => `
                <tr>
                    <td class="fw-semibold">${emp.badge}</td>
                    <td class="text-center">${emp.scanned}</td>
                    <td class="text-center"><span class="badge text-bg-${emp.accuracy >= 95 ? "success" : emp.accuracy >= 85 ? "warning" : "danger"}">${emp.accuracy}%</span></td>
                    <td class="text-center">${emp.total_hours ?? "—"}</td>
                </tr>`
                    )
                    .join("")
                : `<tr><td colspan="4" class="text-center text-muted py-3">Нет данных</td></tr>`;
        }

        renderHourlyChart(hourlyStats);
        renderHourHeatmap(hourlyStats);

        renderList(
            discrepancyTypesList,
            discrepancyTypes,
            (item) => `
                <div class="list-group-item d-flex justify-content-between align-items-center">
                    <span>${item.label || item.status || "—"}</span>
                    <span class="badge text-bg-danger">${item.count}</span>
                </div>`
        );
    }

    function renderHourHeatmap(hourlyStats) {
        if (!hourHeatmap) return;
        const rows = Array.isArray(hourlyStats) ? hourlyStats : [];
        const byHour = new Map();
        rows.forEach((item) => {
            if (!item?.hour) return;
            const h = new Date(item.hour).getHours();
            byHour.set(h, Number(item.count || 0));
        });
        const vals = Array.from({ length: 24 }, (_, h) => byHour.get(h) || 0);
        const max = Math.max(...vals, 1);
        hourHeatmap.innerHTML = vals.map((v, h) => {
            const lvl = v === 0 ? 0 : v >= max * 0.75 ? 4 : v >= max * 0.5 ? 3 : v >= max * 0.25 ? 2 : 1;
            return `<div class="hour-heat-cell hour-heat-lvl-${lvl}" title="${h.toString().padStart(2,"0")}:00 — ${v} сканов">
                <span>${h.toString().padStart(2,"0")}</span><span class="v">${v}</span>
            </div>`;
        }).join("");
    }

    function renderHourlyChart(hourlyStats) {
        if (!hourlyChartCanvas || typeof Chart === "undefined") return;
        const labels = (hourlyStats || []).map((item) => {
            if (!item.hour) return "";
            const d = new Date(item.hour);
            return d.getHours().toString().padStart(2, "0") + ":00";
        });
        const counts = (hourlyStats || []).map((item) => item.count || 0);
        if (hourlyChartInstance) hourlyChartInstance.destroy();
        hourlyChartInstance = new Chart(hourlyChartCanvas, {
            type: "bar",
            data: {
                labels,
                datasets: [{ label: "Сканов", data: counts, backgroundColor: "rgba(110, 43, 98, 0.6)", borderColor: "#6E2B62", borderWidth: 1 }],
            },
            options: {
                responsive: true,
                plugins: { legend: { display: false } },
                scales: { y: { beginAtZero: true } },
            },
        });
    }

    async function loadAnalytics() {
        const period = getAdminPeriod();
        const { ok, data } = await API.get(`/api/admin/analytics?period=${encodeURIComponent(period)}`);
        if (!ok || data.error) {
            if (dailyChartCanvas) {
                dailyChartCanvas.innerHTML = `<div class="text-danger">${data.error || "Ошибка загрузки аналитики"}</div>`;
            }
            return;
        }

        const dailyStats = data.daily_stats || [];
        const problemZones = data.problem_zones || [];
        renderDailyChart(dailyStats);
        if (statusChartCanvas) renderStatusChart(dailyStats);

        if (problemZonesTableBody) {
            problemZonesTableBody.innerHTML = problemZones.length
                ? problemZones
                      .map(
                          (zone) => `
                <tr data-place-cod="${zone.place_cod || ""}">
                    <td class="text-primary text-decoration-underline" style="cursor:pointer;">
                        ${zone.place_name || zone.place_cod || "—"}
                    </td>
                    <td class="text-center">${zone.scan_count}</td>
                    <td class="text-center text-danger">${zone.error_count} (${zone.error_rate}%)</td>
                </tr>`
                      )
                      .join("")
                : `<tr><td colspan="3" class="text-center text-muted py-3">Нет данных</td></tr>`;
        }
    }

    let currentPhotoContext = {
        placeCod: null,
        resultId: null,
    };

    async function loadPlacePhotos(placeCod) {
        if (!photoPreviewModalEl || !photoPreviewThumbs) return;
        const { ok, data } = await API.get(`/api/admin/place/${encodeURIComponent(placeCod)}/photos`);
        if (!ok || data.error) {
            photoPreviewThumbs.innerHTML = `<div class="text-danger small">${data.error || "Ошибка загрузки фото по месту"}</div>`;
            photoPreviewImg.src = "";
            photoPreviewImg.classList.add("d-none");
            photoPreviewMeta.textContent = "";
            photoPreviewModalLabel.textContent = `Фотофиксация — МХ ${placeCod}`;
            if (!photoPreviewModal) {
                photoPreviewModal = new bootstrap.Modal(photoPreviewModalEl);
            }
            photoPreviewModal.show();
            return;
        }

        const photos = data.photos || [];
        if (!photos.length) {
            photoPreviewThumbs.innerHTML = `<div class="text-muted small">Для места ${placeCod} нет фотофиксаций</div>`;
            photoPreviewImg.src = "";
            photoPreviewImg.classList.add("d-none");
            photoPreviewMeta.textContent = "";
            photoPreviewModalLabel.textContent = `Фотофиксация — МХ ${placeCod}`;
            if (!photoPreviewModal) {
                photoPreviewModal = new bootstrap.Modal(photoPreviewModalEl);
            }
            photoPreviewModal.show();
            return;
        }

        photoPreviewModalLabel.textContent = `Фотофиксация — МХ ${placeCod}`;

        // Первое фото показываем крупно
        const first = photos[0];
        currentPhotoContext.placeCod = placeCod;
        currentPhotoContext.resultId = first.id || null;
        if (first.photo_url) {
            photoPreviewImg.src = first.photo_url;
            photoPreviewImg.classList.remove("d-none");
        } else {
            photoPreviewImg.src = "";
            photoPreviewImg.classList.add("d-none");
        }
        photoPreviewMeta.innerHTML = `
            <div>МХ: <strong>${first.place_name || first.place_cod || "—"}</strong></div>
            <div>Бэйдж: <strong>${first.badge || "—"}</strong></div>
            <div>Статус: <span class="badge text-bg-${first.status === "ok" ? "success" : "danger"}">${first.status}</span></div>
            <div class="text-muted">Время: ${formatDate(first.created_at)}</div>
        `;

        // Генерируем превью сбоку
        photoPreviewThumbs.innerHTML = photos
            .map(
                (p, idx) => `
            <div class="photo-card ${idx === 0 ? "border border-primary" : ""}" data-photo-thumb
                 data-result-id="${p.id || ""}"
                 data-photo-url="${p.photo_url || ""}"
                 data-place-cod="${p.place_cod || ""}"
                 data-badge="${p.badge || ""}"
                 data-status="${p.status || ""}"
                 data-timestamp="${p.created_at || ""}">
                ${
                    p.photo_url
                        ? `<img src="${p.photo_url}" alt="${p.place_name || p.place_cod}" class="w-100 rounded-3 mb-1" style="height:80px;object-fit:cover;">`
                        : '<div class="text-muted small mb-1">Нет фото</div>'
                }
                <div class="small text-muted">${formatDate(p.created_at)}</div>
            </div>`
            )
            .join("");

        if (!photoPreviewModal) {
            photoPreviewModal = new bootstrap.Modal(photoPreviewModalEl);
        }
        photoPreviewModal.show();
    }

    async function loadActiveTasks() {
        if (!activeTasksTableBody) return;
        const { ok, data } = await API.get("/api/tasks/active");
        if (!ok || data.error) {
            activeTasksTableBody.innerHTML = `<tr><td colspan="6" class="text-center text-danger py-4">${data.error || "Ошибка загрузки"}</td></tr>`;
            return;
        }

        const tasks = data.tasks || [];
        activeTasksTableBody.innerHTML = tasks.length
            ? tasks
                  .map(
                      (task) => `
                <tr>
                    <td>#${task.task_id}</td>
                    <td>${task.badge}</td>
                    <td>${task.zone}</td>
                    <td>${formatDate(task.assigned_at)}</td>
                    <td>${formatDate(task.expires_at)}</td>
                    <td>
                        <span class="badge text-bg-${task.hours_left > 0 ? "success" : "danger"}">${task.hours_left > 0 ? "Активно" : "Просрочено"}</span>
                        <div class="btn-group btn-group-sm d-flex mt-2">
                            <button class="btn btn-outline-secondary extend-task" data-id="${task.task_id}" data-hours="1">
                                +1 час
                            </button>
                            <button class="btn btn-outline-danger close-task" data-id="${task.task_id}">
                                Закрыть
                            </button>
                        </div>
                    </td>
                </tr>`
                  )
                  .join("")
            : `<tr><td colspan="6" class="text-center text-muted py-4">Нет заданий</td></tr>`;
        refreshAnimations();
    }

    async function loadLatestScans() {
        const { ok, data } = await API.get("/api/admin/latest_scans");
        if (!ok || data.error) {
            if (adminPhotoGrid) {
                adminPhotoGrid.innerHTML = `<div class="text-danger small">${data.error || "Ошибка загрузки фото"}</div>`;
            }
            return;
        }

        const scans = data.scans || [];
        if (photoGalleryCount) photoGalleryCount.textContent = scans.length;

        if (!adminPhotoGrid) return;

        // Показываем все фото в галерее (не только первые 4)
        adminPhotoGrid.innerHTML = scans.length
            ? scans
                  .map(
                      (scan) => `
                    <div class="photo-card" data-photo-card data-photo-url="${scan.photo_url || ""}" data-place-cod="${scan.place_cod || ""}" data-badge="${scan.badge || ""}" data-status="${scan.status || ""}" data-timestamp="${scan.created_at || ""}">
                        ${
                            scan.photo_url
                                ? `<img src="${scan.photo_url}" alt="${scan.place_name || scan.place_cod}" class="w-100 rounded-3 mb-2" style="height:160px;object-fit:cover;">`
                                : '<div class="text-muted small mb-2">Нет фото</div>'
                        }
                        <div class="d-flex justify-content-between small">
                            <span class="fw-semibold">${scan.place_name || scan.place_cod || "—"}</span>
                            <span class="text-muted">${scan.badge}</span>
                        </div>
                        <div class="small text-muted">${formatDate(scan.created_at)}</div>
                    </div>`
                  )
                  .join("")
            : '<div class="text-muted small">Нет фото</div>';
        refreshAnimations();
    }

    async function loadQualityData() {
        if (!qualityTableBody && !qualityReviewList) return;
        const { ok, data } = await API.get("/api/admin/reviews");
        if (!ok || data.error) {
            if (qualityTableBody) qualityTableBody.innerHTML = `<tr><td colspan="4" class="text-center text-danger py-3">${data?.error || "Ошибка загрузки"}</td></tr>`;
            if (qualityReviewList) qualityReviewList.innerHTML = '<div class="list-group-item text-muted small admin-empty-list"><i class="bi bi-clipboard-check me-2"></i>Ревизии не загружены</div>';
            return;
        }

        const aggregates = data.aggregates || [];
        if (qualityTableBody) qualityTableBody.innerHTML = aggregates.length
            ? aggregates
                  .map(
                      (item) => `
                <tr>
                    <td>${item.zone || "—"}</td>
                    <td class="text-center">${item.scan_count}</td>
                    <td class="text-center text-danger">${item.errors}</td>
                    <td>${formatDate(item.last_scan)}</td>
                </tr>`
                  )
                  .join("")
            : `<tr><td colspan="4" class="text-center text-muted py-3">Нет данных</td></tr>`;

        const reviews = data.reviews || [];
        if (qualityReviewList) qualityReviewList.innerHTML = reviews.length
            ? reviews
                  .map(
                      (review) => `
                <div class="list-group-item">
                    <div class="d-flex justify-content-between">
                        <strong>${review.zone}</strong>
                        <span class="badge text-bg-${review.status === "completed" ? "success" : review.status === "in_progress" ? "warning text-dark" : "secondary"}">
                            ${review.status}
                        </span>
                    </div>
                    <small class="text-muted">${formatDate(review.created_at)} • ${review.reviewer || "—"}</small>
                    <div>${review.summary || "Без комментариев"}</div>
                </div>`
                  )
                  .join("")
            : '<div class="list-group-item text-muted small admin-empty-list"><i class="bi bi-clipboard-check me-2"></i>Нет ревизий</div>';
        refreshAnimations();
    }

    async function loadTickets() {
        if (!ticketsBoard) return;
        const { ok, data } = await API.get("/api/admin/tickets");
        if (!ok || data.error) {
            ticketsBoard.innerHTML = `<div class="text-danger small">${data?.error || "Ошибка загрузки"}</div>`;
            return;
        }

        const tickets = data.tickets || [];
        if (ticketsCountBadge) ticketsCountBadge.textContent = tickets.length;
        ticketsBoard.innerHTML = tickets.length
            ? tickets
                  .map(
                      (ticket) => `
                <div class="ticket-card" data-ticket-id="${ticket.id}">
                    <div class="d-flex justify-content-between align-items-start mb-2">
                        <div>
                            <div class="fw-semibold">#${ticket.id} • ${ticket.badge}</div>
                            <div class="ticket-meta">МХ: ${ticket.place_name || ticket.place_cod || "—"}</div>
                        </div>
                        <span class="badge text-bg-${ticket.priority === "high" ? "danger" : ticket.priority === "low" ? "secondary" : "warning text-dark"}">${ticket.priority}</span>
                    </div>
                    <p class="mb-2">${ticket.description}</p>
                    <div class="d-flex justify-content-between align-items-center">
                        <small class="text-muted">${formatDate(ticket.created_at)}</small>
                        ${
                            ticket.status === "resolved"
                                ? `<span class="badge text-bg-success">Закрыто</span>`
                                : `<button class="btn btn-sm btn-outline-success resolve-ticket" data-id="${ticket.id}">Закрыть</button>`
                        }
                    </div>
                </div>`
                  )
                  .join("")
            : '<div class="text-muted small">Нет тикетов</div>';
        refreshAnimations();
    }

    async function loadActivityLog() {
        if (!adminActivityList) return;
        const { ok, data } = await API.get("/api/admin/activity");
        if (!ok || data.error) {
            adminActivityList.innerHTML = `<div class="text-danger small">${data.error || "Ошибка загрузки журнала"}</div>`;
            return;
        }

        const events = data.events || [];
        adminActivityList.innerHTML = events.length
            ? events
                  .map(
                      (event) => `
                <div class="admin-activity-item">
                    <span class="${event.type === "report" ? "text-info" : event.type === "scan" ? "text-success" : "text-muted"}">
                        ${event.message}
                    </span>
                    <span class="time">${formatDate(event.timestamp)}</span>
                </div>`
                  )
                  .join("")
            : "Событий нет";
        refreshAnimations();
    }

    function renderDailyChart(dataset) {
        if (!dailyChartCanvas || typeof Chart === "undefined") return;
        const labels = dataset.map((item) => item.date);
        const totals = dataset.map((item) => item.total);
        const errors = dataset.map((item) => item.errors);
        if (dailyChartInstance) {
            dailyChartInstance.destroy();
        }
        dailyChartInstance = new Chart(dailyChartCanvas, {
            type: "line",
            data: {
                labels,
                datasets: [
                    {
                        label: "Всего",
                        data: totals,
                        borderColor: "#2563eb",
                        fill: false,
                        tension: 0.4,
                    },
                    {
                        label: "Расхождения",
                        data: errors,
                        borderColor: "#ef4444",
                        borderDash: [5, 5],
                        fill: false,
                        tension: 0.4,
                    },
                ],
            },
            options: {
                responsive: true,
                plugins: { legend: { display: true } },
                scales: {
                    y: { beginAtZero: true },
                },
            },
        });
    }

    function renderStatusChart(dataset) {
        if (!statusChartCanvas || typeof Chart === "undefined") return;
        const totalOk = dataset.reduce((acc, item) => acc + (item.ok || 0), 0);
        const totalErr = dataset.reduce((acc, item) => acc + (item.errors || 0), 0);
        if (statusChartInstance) {
            statusChartInstance.destroy();
        }
        statusChartInstance = new Chart(statusChartCanvas, {
            type: "doughnut",
            data: {
                labels: ["Без расхождений", "Расхождения"],
                datasets: [
                    {
                        data: [totalOk, totalErr],
                        backgroundColor: ["#22c55e", "#ef4444"],
                        borderWidth: 0,
                    },
                ],
            },
            options: {
                cutout: "70%",
                plugins: {
                    legend: { position: "bottom" },
                },
            },
        });
    }

    loadAdminStats();
    loadAnalytics();
    loadActiveTasks();
    loadLatestScans();
    loadActivityLog();
    loadQualityData();
    loadWhIds();
    if (ticketsBoard) loadTickets();

    dashboardBlockErrorsSelect?.addEventListener("change", () => {
        loadDashboardBlockErrors(dashboardBlockErrorsSelect.value?.trim() || "");
    });

    adminPeriodSelect?.addEventListener("change", () => {
        loadAdminStats();
        loadAnalytics();
    });

    exportEmployeesBtn?.addEventListener("click", () => {
        const period = getAdminPeriod();
        window.open(`/api/admin/export/employees?period=${encodeURIComponent(period)}`, "_blank");
        showToastMessage("Выгрузка сводки по сотрудникам запущена", "info");
    });

    refreshAdminBtn?.addEventListener("click", () => {
        loadAdminStats();
        loadAnalytics();
        loadActiveTasks();
        loadLatestScans();
        loadActivityLog();
        loadQualityData();
        loadWhIds();
        if (dashboardBlockErrorsSelect?.value) loadDashboardBlockErrors(dashboardBlockErrorsSelect.value);
        if (ticketsBoard) loadTickets();
        showToastMessage("Данные админки обновлены", "info");
    });

    clearAdminLogBtn?.addEventListener("click", () => {
        adminActivityList.innerHTML = "Событий нет";
    });

    reportsModalEl?.addEventListener("show.bs.modal", () => {
        // Не трогаем DOM во время открытия модалки — откладываем всё, чтобы не блокировать Bootstrap
        requestAnimationFrame(() => {
            if (reportsTableBody) reportsTableBody.innerHTML = `<tr><td colspan="5" class="text-center text-muted py-3">Загрузка…</td></tr>`;
            setTimeout(() => loadReports(), 250);
        });
    });

    problemZonesTableBody?.addEventListener("click", (event) => {
        const row = event.target.closest("tr[data-place-cod]");
        if (!row) return;
        const placeCod = row.dataset.placeCod;
        if (!placeCod) return;
        loadPlacePhotos(placeCod);
    });

    photoPreviewThumbs?.addEventListener("click", (event) => {
        const thumb = event.target.closest("[data-photo-thumb]");
        if (!thumb) return;

        // подсветка выбранной миниатюры
        photoPreviewThumbs.querySelectorAll("[data-photo-thumb]").forEach((el) => {
            el.classList.remove("border", "border-primary");
        });
        thumb.classList.add("border", "border-primary");

        const url = thumb.dataset.photoUrl || "";
        const placeCod = thumb.dataset.placeCod || "";
        const badge = thumb.dataset.badge || "";
        const status = thumb.dataset.status || "";
        const ts = thumb.dataset.timestamp || "";
        const resultId = thumb.dataset.resultId || null;

        currentPhotoContext.placeCod = placeCod || currentPhotoContext.placeCod;
        currentPhotoContext.resultId = resultId || currentPhotoContext.resultId;

        if (url) {
            photoPreviewImg.src = url;
        } else {
            photoPreviewImg.src = "";
        }
        photoPreviewMeta.innerHTML = `
            <div>МХ: <strong>${placeCod || "—"}</strong></div>
            <div>Бэйдж: <strong>${badge || "—"}</strong></div>
            <div>Статус: <span class="badge text-bg-${status === "ok" ? "success" : "danger"}">${status || "—"}</span></div>
            <div class="text-muted">Время: ${formatDate(ts)}</div>
        `;
    });

    deletePhotoBtn?.addEventListener("click", async () => {
        if (!currentPhotoContext.resultId) {
            showToastMessage("Фото не выбрано", "warning");
            return;
        }
        const confirmed = window.confirm("Удалить это фото? Действие необратимо.");
        if (!confirmed) return;

        const { ok, data } = await API.delete(`/api/admin/photo/${currentPhotoContext.resultId}`);
        if (!ok || data.error || !data.success) {
            showToastMessage(data?.error || "Не удалось удалить фото", "danger");
            return;
        }

        showToastMessage("Фото удалено", "success");
        if (currentPhotoContext.placeCod) {
            // Перезагружаем список фото по месту
            loadPlacePhotos(currentPhotoContext.placeCod);
        } else {
            photoPreviewImg.src = "";
            photoPreviewImg.classList.add("d-none");
            photoPreviewThumbs.innerHTML = `<div class="text-muted small">Фото удалено</div>`;
        }
    });

    downloadPhotoBtn?.addEventListener("click", () => {
        if (!currentPhotoContext.resultId) {
            showToastMessage("Фото не выбрано", "warning");
            return;
        }
        const url = `/api/admin/photo/${currentPhotoContext.resultId}/download`;
        window.open(url, "_blank");
    });

    reportsTableBody?.addEventListener("click", async (event) => {
        const link = event.target.closest('a[href*="/api/admin/reports/"][href*="/download"]');
        if (!link) return;
        event.preventDefault();
        const url = link.getAttribute("href");
        if (!url) return;
        try {
            const response = await fetch(url, { method: "GET", credentials: "include" });
            if (!response.ok) {
                const err = await response.json().catch(() => ({}));
                showToastMessage(err?.error || `Ошибка ${response.status}`, "danger");
                return;
            }
            const blob = await response.blob();
            const disposition = response.headers.get("Content-Disposition");
            let filename = "report.xlsx";
            if (disposition) {
                const match = /filename\*?=(?:UTF-8'')?["']?([^"'\s;]+)/i.exec(disposition) || /filename=["']?([^"'\s;]+)/i.exec(disposition);
                if (match && match[1]) filename = match[1].trim();
            }
            const objectUrl = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = objectUrl;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            a.remove();
            URL.revokeObjectURL(objectUrl);
            showToastMessage("Отчёт скачивается", "success");
        } catch (e) {
            showToastMessage(e?.message || "Не удалось скачать отчёт", "danger");
        }
    });

    qualityReviewForm?.addEventListener("submit", async (event) => {
        event.preventDefault();
        const formData = new FormData(qualityReviewForm);
        const payload = {
            zone: formData.get("reviewZone"),
            status: formData.get("reviewStatus"),
            summary: formData.get("reviewSummary"),
        };
        const { ok, data } = await API.post("/api/admin/reviews", payload);
        if (!ok || data.error || !data.success) {
            showToastMessage(data?.error || "Ошибка создания ревизии", "danger");
            return;
        }
        showToastMessage("Ревизия создана", "success");
        qualityReviewForm.reset();
        const modal = bootstrap.Modal.getInstance(document.getElementById("qualityReviewModal"));
        modal?.hide();
        loadQualityData();
    });

    ticketsBoard?.addEventListener("click", async (event) => {
        const button = event.target.closest(".resolve-ticket");
        if (!button) return;
        const ticketId = button.dataset.id;
        const { ok, data } = await API.post(`/api/admin/tickets/${ticketId}/resolve`, {});
        if (!ok || data.error || !data.success) {
            showToastMessage(data?.error || "Не удалось закрыть тикет", "danger");
            return;
        }
        showToastMessage("Тикет закрыт", "success");
        loadTickets();
    });

    scrollToProblemZonesBtn?.addEventListener("click", () => {
        if (!problemZonesBlock) return;
        problemZonesBlock.scrollIntoView({ behavior: "smooth", block: "start" });
    });

    scrollToTicketsBtn?.addEventListener("click", () => {
        if (!ticketsBlock) return;
        ticketsBlock.scrollIntoView({ behavior: "smooth", block: "start" });
    });

    assignTaskForm?.addEventListener("submit", async (event) => {
        event.preventDefault();
        const formData = new FormData(assignTaskForm);
        const payload = {
            badge: formData.get("badge"),
            zone: formData.get("zone"),
            hours: Number(formData.get("hours") || 2),
        };
        const { ok, data } = await API.post("/api/admin/tasks/assign", payload);
        if (!ok || data.error || !data.success) {
            showToastMessage(data.error || "Ошибка назначения", "danger");
            return;
        }
        showToastMessage("Задача назначена", "success");
        if (!assignTaskModal && typeof bootstrap !== "undefined" && assignTaskModalEl) {
            assignTaskModal = new bootstrap.Modal(assignTaskModalEl);
        }
        assignTaskModal?.hide();
        assignTaskForm.reset();
        loadActiveTasks();
    });

    extendTaskButtons?.addEventListener("click", async (event) => {
        const extendBtn = event.target.closest(".extend-task");
        const closeBtn = event.target.closest(".close-task");
        if (extendBtn) {
            const taskId = extendBtn.dataset.id;
            const hours = extendBtn.dataset.hours || 1;
            const { ok, data } = await API.post("/api/admin/tasks/extend", { task_id: taskId, hours: Number(hours) });
            if (!ok || data.error || !data.success) {
                showToastMessage(data.error || "Не удалось продлить", "danger");
                return;
            }
            showToastMessage("Задача продлена", "info");
            loadActiveTasks();
        } else if (closeBtn) {
            const taskId = closeBtn.dataset.id;
            const { ok, data } = await API.post("/api/admin/tasks/close", { task_id: taskId });
            if (!ok || data.error || !data.success) {
                showToastMessage(data.error || "Не удалось закрыть", "danger");
                return;
            }
            showToastMessage("Задача закрыта", "warning");
            loadActiveTasks();
        }
    });

    function openPhotoPreview(data) {
        if (!photoPreviewModal && typeof bootstrap !== "undefined" && photoPreviewModalEl) {
            photoPreviewModal = new bootstrap.Modal(photoPreviewModalEl);
        }
        const { photoUrl, placeCod, badge, status } = data;
        if (photoUrl) {
            photoPreviewImg.src = photoUrl;
            photoPreviewImg.classList.remove("d-none");
        } else {
            photoPreviewImg.src = "";
            photoPreviewImg.classList.add("d-none");
        }
        if (photoPreviewModalLabel) {
            photoPreviewModalLabel.textContent = `Место ${placeCod || "—"}`;
        }
        if (photoPreviewMeta) {
            photoPreviewMeta.innerHTML = `
                <div>Сотрудник: <strong>${badge || "—"}</strong></div>
                <div>Статус: <strong>${status || "—"}</strong></div>
            `;
        }
        photoPreviewModal?.show();
    }

    async function loadWhIds() {
        const { ok, data } = await API.get("/api/admin/wh_ids");
        const list = ok && !data.error ? (data.wh_ids || []) : [];
        const optionHtml = (value, text) => `<option value="${value}">${text}</option>`;
        const getWhLabel = (item) => {
            const name = (item.warehouse_name || "").trim();
            if (!name) return `Склад ${item.wh_id}`;
            if (/^\d+$/.test(name)) return `Склад ${item.wh_id}`;
            return name;
        };
        if (dashboardBlockErrorsSelect) {
            dashboardBlockErrorsSelect.innerHTML = "<option value=\"\">Выберите склад</option>" + list.map((item) => optionHtml(item.wh_id, getWhLabel(item))).join("");
        }
    }

    async function loadDashboardBlockErrors(whId) {
        if (!dashboardBlockErrorsBody) return;
        if (!whId) {
            if (dashboardBlockErrorsEmpty) dashboardBlockErrorsEmpty.classList.remove("d-none");
            if (dashboardBlockErrorsTableWrap) dashboardBlockErrorsTableWrap.classList.add("d-none");
            if (dashboardBlockErrorsCount) dashboardBlockErrorsCount.textContent = "";
            return;
        }
        if (dashboardBlockErrorsEmpty) dashboardBlockErrorsEmpty.classList.add("d-none");
        if (dashboardBlockErrorsTableWrap) dashboardBlockErrorsTableWrap.classList.remove("d-none");
        const skeletonRows = Array(4).fill("<tr><td colspan=\"5\" class=\"px-3 py-2\"><div class=\"skeleton skeleton-line\"></div><div class=\"skeleton skeleton-line\"></div><div class=\"skeleton skeleton-line\"></div></td></tr>").join("");
        dashboardBlockErrorsBody.innerHTML = skeletonRows;
        if (dashboardBlockErrorsCount) dashboardBlockErrorsCount.textContent = "";
        const { ok, data } = await API.get(`/api/admin/block/errors?wh_id=${encodeURIComponent(whId)}`);
        if (!ok || data.error) {
            dashboardBlockErrorsBody.innerHTML = `<tr><td colspan="5" class="text-center text-danger py-2">${data.error || "Ошибка загрузки"}</td></tr>`;
            return;
        }
        const places = data.places || [];
        if (places.length === 0) {
            dashboardBlockErrorsBody.innerHTML = "<tr><td colspan=\"5\" class=\"text-center text-muted py-3 small\">Нет ошибок по этому складу</td></tr>";
        } else {
            dashboardBlockErrorsBody.innerHTML = places.map((p) => {
                const status = p.place_status || "";
                const rowClass = status === "repaired" ? "table-success" : status === "in_work" ? "table-warning" : "";
                return `<tr class="${rowClass}" data-place-cod="${p.place_cod}">
                    <td>${(p.place_name || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")} / ${String(p.place_cod)}</td>
                    <td>${p.floor != null ? p.floor : "—"}</td>
                    <td>${p.row_num != null ? p.row_num : "—"}</td>
                    <td>${p.section != null ? p.section : "—"}</td>
                    <td>
                        <select class="form-select form-select-sm dashboard-place-status-select" data-place-cod="${p.place_cod}" aria-label="Статус">
                            <option value=""${status === "" ? " selected" : ""}>—</option>
                            <option value="in_work"${status === "in_work" ? " selected" : ""}>В работе</option>
                            <option value="repaired"${status === "repaired" ? " selected" : ""}>Исправлено</option>
                        </select>
                    </td>
                </tr>`;
            }).join("");
            dashboardBlockErrorsBody.querySelectorAll(".dashboard-place-status-select").forEach((sel) => {
                sel.addEventListener("change", async function () {
                    const placeCod = this.dataset.placeCod;
                    const newStatus = this.value;
                    const row = this.closest("tr");
                    const currentWhId = dashboardBlockErrorsSelect?.value?.trim();
                    if (!currentWhId) return;
                    if (newStatus === "") {
                        const res = await fetch(`/api/admin/block/repaired?wh_id=${encodeURIComponent(currentWhId)}&place_cod=${encodeURIComponent(placeCod)}`, { method: "DELETE", credentials: "include" });
                        const json = await res.json().catch(() => ({}));
                        if (res.ok && json.success) {
                            row.classList.remove("table-success", "table-warning");
                        } else {
                            this.value = this.dataset.prevStatus || "";
                        }
                    } else {
                        const res = await fetch("/api/admin/block/repaired", {
                            method: "POST",
                            credentials: "include",
                            headers: { "Content-Type": "application/json" },
                            body: JSON.stringify({ wh_id: parseInt(currentWhId, 10), place_cod: parseInt(placeCod, 10), status: newStatus }),
                        });
                        const json = await res.json().catch(() => ({}));
                        if (res.ok && json.success) {
                            row.classList.remove("table-success", "table-warning");
                            if (newStatus === "repaired") row.classList.add("table-success");
                            if (newStatus === "in_work") row.classList.add("table-warning");
                        } else {
                            this.value = this.dataset.prevStatus || "";
                        }
                    }
                    this.dataset.prevStatus = this.value;
                });
                sel.dataset.prevStatus = sel.value;
            });
        }
        const inWorkCount = places.filter((p) => p.place_status === "in_work").length;
        const repairedCount = places.filter((p) => p.place_status === "repaired").length;
        if (dashboardBlockErrorsCount) dashboardBlockErrorsCount.textContent = `Мест: ${places.length}, в работе: ${inWorkCount}, исправлено: ${repairedCount}.`;
        refreshAnimations();
    }

    const dashboardExportBlockBtn = document.getElementById("dashboardExportBlockBtn");
    dashboardExportBlockBtn?.addEventListener("click", async () => {
        const whId = dashboardBlockErrorsSelect?.value?.trim();
        if (!whId) {
            showToastMessage("Выберите склад", "warning");
            return;
        }
        const globalProgressBar = document.getElementById("globalProgressBar");
        if (globalProgressBar) globalProgressBar.classList.remove("d-none");
        dashboardExportBlockBtn.disabled = true;
        try {
            const base = window.location.origin;
            const url = `${base}/api/admin/export/block?wh_id=${encodeURIComponent(whId)}`;
            const res = await fetch(url, { credentials: "include" });
            if (!res.ok) {
                const err = await res.json().catch(() => ({}));
                showToastMessage(err.error || `Ошибка ${res.status}`, "danger");
                return;
            }
            const blob = await res.blob();
            const name = res.headers.get("Content-Disposition")?.match(/filename="?([^";]+)"?/)?.[1] || `errors_wh_id_${whId}.xlsx`;
            const a = document.createElement("a");
            a.href = URL.createObjectURL(blob);
            a.download = name;
            a.click();
            URL.revokeObjectURL(a.href);
            showToastMessage("Файл скачан", "success");
        } catch (e) {
            showToastMessage("Ошибка: " + (e.message || "сеть"), "danger");
        } finally {
            if (globalProgressBar) globalProgressBar.classList.add("d-none");
            dashboardExportBlockBtn.disabled = false;
        }
    });

    async function loadReports() {
        if (!reportsTableBody) return;
        try {
            const { ok, data } = await API.get("/api/admin/reports");
            if (!ok || data.error) {
                reportsTableBody.innerHTML = `<tr><td colspan="5" class="text-danger text-center py-3">${(data && data.error) || "Ошибка загрузки"}</td></tr>`;
                return;
            }
            const reports = Array.isArray(data.reports) ? data.reports : [];
            reportsTableBody.innerHTML = reports.length
                ? reports
                      .map(
                          (report) => {
                              const rid = report.report_id != null ? report.report_id : null;
                              const downloadBtn =
                                  rid != null
                                      ? `<a href="/api/admin/reports/${rid}/download" target="_blank" rel="noopener" class="btn btn-sm btn-outline-primary"><i class="bi bi-download me-1"></i>Скачать</a>`
                                      : "<span class=\"text-muted\">—</span>";
                              return `
                <tr>
                    <td>#${rid != null ? rid : "—"}</td>
                    <td>${report.filename != null ? String(report.filename) : "—"}</td>
                    <td>${report.badge != null ? String(report.badge) : "—"}</td>
                    <td>${report.total_scanned != null ? report.total_scanned : "—"}</td>
                    <td>${downloadBtn}</td>
                </tr>`;
                          }
                      )
                      .join("")
                : `<tr><td colspan="5" class="text-center text-muted py-3">Отчеты отсутствуют</td></tr>`;
        } catch (err) {
            console.error("loadReports error", err);
            reportsTableBody.innerHTML = `<tr><td colspan="5" class="text-danger text-center py-3">Ошибка: ${err.message || "не удалось загрузить"}</td></tr>`;
        }
    }
}

document.addEventListener("DOMContentLoaded", () => {
    initThemeToggle();
    initTopbarClock();
    initCommandPalette();
    initMenuBadgeAndLogout();
    initAnimations();
    initMagneticButtons();
    const page = document.body.dataset.page;
    const map = {
        index: initLoginPage,
        work: initWorkPage,
        admin: initAdminLoginPage,
        admin_login_legacy: initAdminLoginPage,
        admin_dashboard: initAdminDashboard,
    };

    const init = map[page];
    if (init) {
        init();
    }
});

