const ANIMATE_SELECTORS = [
    ".card",
    ".progress-card",
    ".hero-card",
    ".place-card",
    ".photo-card",
    ".notification-panel",
    ".admin-activity-item",
    ".stats-progress > *",
];
let animationObserver = null;

const PLACE_CACHE_KEY = "inventory-mx-cache";
const PLACE_CACHE_TTL_MS = 60 * 60 * 1000; // 1 час
const PLACE_CACHE_MAX = 500;
const OFFLINE_QUEUE_KEY = "inventory-offline-queue";

// Ограничение размера фото для отправки (Vercel лимит тела запроса 4.5 MB)
const MAX_PHOTO_DIMENSION = 1280;
const PHOTO_JPEG_QUALITY = 0.82;
const MAX_PHOTO_BASE64_BYTES = 3 * 1024 * 1024; // ~3 MB на фото, чтобы несколько фото + JSON укладывались в 4.5 MB

const PlaceCache = {
    get(key) {
        try {
            const raw = localStorage.getItem(PLACE_CACHE_KEY);
            if (!raw) return null;
            const store = JSON.parse(raw);
            const k = String(key).trim().toUpperCase();
            const entry = store.items?.[k];
            if (!entry || !entry.data) return null;
            if (Date.now() - (entry.ts || 0) > PLACE_CACHE_TTL_MS) return null;
            return entry.data;
        } catch {
            return null;
        }
    },
    set(key, data) {
        try {
            const raw = localStorage.getItem(PLACE_CACHE_KEY);
            const store = raw ? JSON.parse(raw) : { items: {}, order: [] };
            store.items = store.items || {};
            store.order = store.order || [];
            const k = String(key).trim().toUpperCase();
            store.items[k] = { data, ts: Date.now() };
            if (!store.order.includes(k)) store.order.push(k);
            const numId = data.place_cod != null ? String(data.place_cod) : null;
            const strCode = data.place_name ? String(data.place_name).trim().toUpperCase() : null;
            if (numId && numId !== k) {
                store.items[numId] = { data, ts: Date.now() };
                if (!store.order.includes(numId)) store.order.push(numId);
            }
            if (strCode && strCode !== k && strCode !== numId) {
                store.items[strCode] = { data, ts: Date.now() };
                if (!store.order.includes(strCode)) store.order.push(strCode);
            }
            while (store.order.length > PLACE_CACHE_MAX) {
                const old = store.order.shift();
                if (old) delete store.items[old];
            }
            localStorage.setItem(PLACE_CACHE_KEY, JSON.stringify(store));
        } catch (e) {
            console.warn("PlaceCache set error", e);
        }
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

const OfflineQueue = {
    push(payload) {
        try {
            const raw = localStorage.getItem(OFFLINE_QUEUE_KEY);
            const queue = raw ? JSON.parse(raw) : [];
            queue.push({ ...payload, ts: Date.now() });
            localStorage.setItem(OFFLINE_QUEUE_KEY, JSON.stringify(queue));
            return queue.length;
        } catch (e) {
            return 0;
        }
    },
    getAll() {
        try {
            const raw = localStorage.getItem(OFFLINE_QUEUE_KEY);
            return raw ? JSON.parse(raw) : [];
        } catch {
            return [];
        }
    },
    clear() {
        localStorage.removeItem(OFFLINE_QUEUE_KEY);
    },
    set(items) {
        localStorage.setItem(OFFLINE_QUEUE_KEY, JSON.stringify(items));
    },
};

async function syncOfflineQueue() {
    const queue = OfflineQueue.getAll();
    if (!queue.length || !navigator.onLine) return;
    const remaining = [];
    for (const item of queue) {
        try {
            const res = await fetch("/api/scan/complete", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                credentials: "include",
                body: JSON.stringify(item.body || (item.place_cod ? item : item.body) || {}),
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok || data.error) remaining.push(item);
        } catch (_) {
            remaining.push(item);
        }
    }
    OfflineQueue.set(remaining);
    if (remaining.length === 0) OfflineQueue.clear();
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

        try {
            const response = await fetch(path, config);
            const data = await response.json().catch(() => ({}));

            return {
                ok: response.ok,
                status: response.status,
                data,
            };
        } catch (error) {
            if (!navigator.onLine && path.includes("/api/scan/complete") && (options.method === "POST" || config.method === "POST")) {
                try {
                    const body = typeof config.body === "string" ? JSON.parse(config.body) : {};
                    OfflineQueue.push({ path, body });
                    return { ok: false, status: 0, data: { error: "Сохранено в очередь. Синхронизация при появлении сети." } };
                } catch (_) {}
            }
            console.error("API error", error);
            return {
                ok: false,
                status: 0,
                data: { error: "Не удалось связаться с сервером" },
            };
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

function showAlert(element, message, type = "danger") {
    if (!element) {
        return;
    }
    element.textContent = message;
    element.classList.remove("d-none", "alert-danger", "alert-success", "alert-info");
    element.classList.add(`alert-${type}`);
}

function hideAlert(element) {
    if (!element) return;
    element.classList.add("d-none");
}

function showToastMessage(message, type = "success") {
    const container = document.getElementById("toastContainer");
    if (!container || typeof bootstrap === "undefined") return;
    const icons = {
        success: "bi-check-circle-fill",
        danger: "bi-x-circle-fill",
        warning: "bi-exclamation-triangle-fill",
        info: "bi-info-circle-fill",
    };
    const toast = document.createElement("div");
    toast.className = `toast toast-visual align-items-center text-bg-${type} border-0`;
    toast.role = "alert";
    toast.innerHTML = `
        <div class="d-flex">
            <div class="toast-body">
                <i class="bi ${icons[type] || icons.info}"></i>
                <span>${message}</span>
            </div>
            <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
        </div>`;
    container.appendChild(toast);
    const bsToast = new bootstrap.Toast(toast, { delay: 3000 });
    toast.addEventListener("hidden.bs.toast", () => toast.remove());
    bsToast.show();
}

function applyTheme(theme) {
    const body = document.body;
    if (!body) return;
    body.dataset.theme = theme;
    body.setAttribute("data-bs-theme", theme);
}

function initThemeToggle() {
    const stored = localStorage.getItem("inventory-theme");
    applyTheme(stored || "light");
    const toggle = document.getElementById("themeToggle");
    toggle?.addEventListener("click", () => {
        const current = document.body.dataset.theme === "dark" ? "dark" : "light";
        const next = current === "dark" ? "light" : "dark";
        applyTheme(next);
        localStorage.setItem("inventory-theme", next);
    });
}

function markAnimateTargets() {
    ANIMATE_SELECTORS.forEach((selector) => {
        document.querySelectorAll(selector).forEach((element) => {
            element.classList.add("animate-on-scroll");
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
    const form = document.getElementById("loginForm");
    if (!form) return;

    const badgeInput = document.getElementById("badgeInput");
    const passwordInput = document.getElementById("passwordInput");
    const passwordGroup = document.getElementById("passwordGroup");
    const rememberCheckbox = document.getElementById("remember");
    const alertBox = document.getElementById("loginAlert");
    const connectionBadge = document.getElementById("connectionStatusBadge");
    const deviceStatusInfo = document.getElementById("deviceStatusInfo");
    const loginCard = document.getElementById("loginCard");
    const adminToggleLink = document.getElementById("adminToggleLink");
    const titleEl = form.closest(".card")?.querySelector("h2");
    const subtitleEl = form.closest(".card")?.querySelector("p.text-muted");

    const storedBadge = localStorage.getItem("badge");
    if (storedBadge && badgeInput) {
        badgeInput.value = storedBadge;
    }

    // стартовая анимация карточки
    if (loginCard) {
        loginCard.classList.add("login-card-enter");
        setTimeout(() => loginCard.classList.remove("login-card-enter"), 600);
    }

    let isAdminMode = false;

    function switchToAdminMode() {
        if (isAdminMode) return;
        isAdminMode = true;

        if (titleEl) titleEl.textContent = "Вход администратора";
        if (subtitleEl) subtitleEl.textContent = "Введите админ-пароль для доступа к панели управления";

        if (badgeInput) {
            badgeInput.value = "ADMIN";
            badgeInput.readOnly = true;
        }

        passwordGroup?.classList.remove("d-none");
        rememberCheckbox?.closest(".form-check")?.classList.add("d-none");

        if (adminToggleLink) {
            adminToggleLink.textContent = "← Назад к рабочему входу";
        }

        if (deviceStatusInfo) {
            deviceStatusInfo.textContent = "Доступ к камере настраивается уже внутри админ-панели.";
        }
    }

    function switchToUserMode() {
        if (!isAdminMode) return;
        isAdminMode = false;

        if (titleEl) titleEl.textContent = "Вход по бэйджу";
        if (subtitleEl) subtitleEl.textContent = "Введите номер пропуска сотрудника";

        if (badgeInput) {
            const stored = localStorage.getItem("badge");
            badgeInput.readOnly = false;
            badgeInput.value = stored || "";
        }

        passwordGroup?.classList.add("d-none");
        rememberCheckbox?.closest(".form-check")?.classList.remove("d-none");

        if (adminToggleLink) {
            adminToggleLink.textContent = "Вход для администратора";
        }

        if (deviceStatusInfo) {
            const cameraOk = !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia);
            deviceStatusInfo.textContent = cameraOk
                ? "Камера устройства поддерживается этим браузером. Можно использовать сканер."
                : "Браузер не даёт доступ к камере. Используйте сканер штрихкодов или ручной ввод.";
        }
    }

    function isCameraApiAvailableSimple() {
        return !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia);
    }

    async function updateConnectionStatus() {
        if (!connectionBadge) return;

        // базовая проверка браузерного offline
        if (!navigator.onLine) {
            connectionBadge.textContent = "Оффлайн: нет сети";
            connectionBadge.className = "badge rounded-pill text-bg-danger";
            return;
        }

        connectionBadge.textContent = "Проверяем соединение…";
        connectionBadge.className = "badge rounded-pill text-bg-secondary";

        const { ok, data } = await API.get("/api/health");
        if (ok && data?.ok) {
            connectionBadge.textContent = "Онлайн: связь с сервером";
            connectionBadge.className = "badge rounded-pill text-bg-success";
            connectionBadge.classList.add("badge-pulse-once");
            setTimeout(() => connectionBadge.classList.remove("badge-pulse-once"), 1300);
        } else {
            connectionBadge.textContent = data?.error || "Проблемы соединения";
            connectionBadge.className = "badge rounded-pill text-bg-warning";
        }
    }

    // первичная проверка и реакция на смену статуса сети
    updateConnectionStatus();
    window.addEventListener("online", updateConnectionStatus);
    window.addEventListener("offline", updateConnectionStatus);
    connectionBadge?.addEventListener("click", updateConnectionStatus);

    // информация об устройстве (камера / браузер)
    if (deviceStatusInfo) {
        const cameraOk = isCameraApiAvailableSimple();
        if (cameraOk) {
            deviceStatusInfo.textContent = "Камера устройства поддерживается этим браузером. Можно использовать сканер.";
        } else {
            deviceStatusInfo.textContent =
                "Браузер не даёт доступ к камере. Используйте сканер штрихкодов или ручной ввод.";
        }
    }

    adminToggleLink?.addEventListener("click", (event) => {
        event.preventDefault();
        if (isAdminMode) {
            switchToUserMode();
        } else {
            switchToAdminMode();
        }
    });

    form.addEventListener("submit", async (event) => {
        event.preventDefault();
        form.classList.add("was-validated");
        if (!form.checkValidity()) return;

        hideAlert(alertBox);
        const badge = badgeInput.value.trim();
        const password = passwordInput.value.trim();

        const { ok, data } = await API.post("/api/auth", { badge, password });
        if (!ok || data.error) {
            showAlert(alertBox, data.error || "Ошибка авторизации");
            return;
        }

        if (data.require_password) {
            passwordGroup?.classList.remove("d-none");
            showAlert(alertBox, data.message || "Введите пароль администратора", "info");
            return;
        }

        if (rememberCheckbox?.checked) {
            localStorage.setItem("badge", data.badge);
        } else {
            localStorage.removeItem("badge");
        }

        sessionStorage.setItem("badge", data.badge);
        sessionStorage.setItem("isAdmin", data.is_admin ? "1" : "0");
        // При входе на рабочую страницу — начать новую смену (статистика с нуля)
        if (!data.is_admin && (data.redirect || "/work") === "/work") {
            sessionStorage.setItem("inventory_new_shift", "1");
        }

        const redirectTarget = data.redirect || (data.is_admin ? "/admin/dashboard" : "/work");
        window.location.href = redirectTarget;
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
    const todayLabel = document.getElementById("todayStatsLabel");
    const totalScans = document.getElementById("totalScans");
    const totalDiscrepancy = document.getElementById("totalDiscrepancy");
    const totalOk = document.getElementById("totalOk");
    const sessionsTableBody = document.getElementById("sessionsTableBody");
    const newShiftBtn = document.getElementById("newShiftBtn");
    const newShiftBtnScanOnly = document.getElementById("newShiftBtnScanOnly");
    const refreshStatsBtn = document.getElementById("refreshStatsBtn");
    const placeForm = document.getElementById("scanForm");
    const placeInput = document.getElementById("placeInput");
    const placeAlert = document.getElementById("placeAlert");
    const placeTitle = document.getElementById("placeTitle");
    const placeUpdatedLabel = document.getElementById("placeUpdatedLabel");
    const clearPlaceBtn = document.getElementById("clearPlaceBtn");
    const refreshPlaceBtn = document.getElementById("refreshPlaceBtn");
    const lastPlacesGrid = document.getElementById("lastPlacesGrid");
    const lastPlacesCount = document.getElementById("lastPlacesCount");
    const statusButtonsContainer = document.getElementById("statusButtons");
    const statusLabel = document.getElementById("statusLabel");
    const commentInput = document.getElementById("commentInput");
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
    const photoSelectBtn = document.getElementById("photoSelectBtn");
    const photoPreview = document.getElementById("photoPreview");
    const photoHint = document.getElementById("photoHint");
    const saveScanBtn = document.getElementById("saveScanBtn");
    const saveSpinner = document.getElementById("saveSpinner");
    const qrModalEl = document.getElementById("qrModal");
    const qrReaderEl = document.getElementById("qrReader");
    const qrStatusBadge = document.getElementById("qrStatus");
    const openQrScannerBtn = document.getElementById("openQrScannerBtn");
    const notificationList = document.getElementById("notificationList");
    const clearLogBtn = document.getElementById("clearLogBtn");
    const fabScanBtn = document.getElementById("fabScanBtn");
    const progressCompletionValue = document.getElementById("progressCompletionValue");
    const progressCompletionBar = document.getElementById("progressCompletionBar");
    const progressAccuracyValue = document.getElementById("progressAccuracyValue");
    const progressAccuracyBar = document.getElementById("progressAccuracyBar");
    const progressPhotoValue = document.getElementById("progressPhotoValue");
    const progressPhotoBar = document.getElementById("progressPhotoBar");
    const routeSuggestionsEl = document.getElementById("routeSuggestions");
    const routeMapEl = document.getElementById("routeMap");
    const refreshRouteBtn = document.getElementById("refreshRouteBtn");
    const routeFilterSelect = document.getElementById("routeFilterSelect");
    const quickScanModeCheck = document.getElementById("quickScanMode");
    const voiceSearchBtn = document.getElementById("voiceSearchBtn");
    const incidentForm = document.getElementById("incidentForm");
    const incidentStatus = document.getElementById("incidentStatus");
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

    badgeLabel.textContent = badge;

    function applyScanOnlyMode() {
        const on = state.scanOnlyMode;
        try { localStorage.setItem(SCAN_ONLY_STORAGE_KEY, on ? "1" : "0"); } catch (e) {}
        document.body.classList.toggle("scan-only-mode", on);
        if (scanOnlyToggle) scanOnlyToggle.checked = on;
    }

    function setTodayDates() {
        const today = new Date().toISOString().slice(0, 10);
        if (historyFromInput && !historyFromInput.value) historyFromInput.value = today;
        if (historyToInput && !historyToInput.value) historyToInput.value = today;
    }

    async function updateOnlineStatus() {
        if (!onlineBadge) return;
        if (!navigator.onLine) {
            onlineBadge.textContent = "Офлайн";
            onlineBadge.className = "badge bg-danger rounded-pill mt-2";
            return;
        }
        const { ok, data } = await API.get("/api/health");
        if (ok && data?.ok) {
            onlineBadge.textContent = "Онлайн";
            onlineBadge.className = "badge bg-success rounded-pill mt-2";
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
    };
    let qrScanner = null;
    let qrModalInstance = null;

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
            { value: 'no_container', text: 'Нет тары', detailPlaceholder: 'ID короба' },
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
            }
        }
    }

    function renderLastPlaces() {
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

    async function loadStats() {
        const shiftStart = (typeof sessionStorage !== "undefined" && sessionStorage.getItem(SHIFT_START_KEY)) || "";
        const url = shiftStart ? `/api/user/stats/${encodeURIComponent(badge)}?since=${shiftStart}` : `/api/user/stats/${encodeURIComponent(badge)}`;
        const { ok, data } = await API.get(url);
        if (!ok || data.error) {
            showAlert(placeAlert, data.error || "Не удалось загрузить статистику");
            return;
        }

        hideAlert(placeAlert);
        const overall = data.overall || {};
        const today = data.today || {};
        const sessions = data.sessions || [];

        totalScans.textContent = overall.total_scanned ?? 0;
        totalDiscrepancy.textContent = overall.with_discrepancy ?? 0;
        totalOk.textContent = overall.no_discrepancy ?? 0;
        todayLabel.textContent = `Сегодня: ${today.today_scanned ?? 0} сканов, расхождений ${today.today_discrepancy ?? 0}`;
        lastSyncLabel.textContent = `Последняя синхронизация: ${formatDate(new Date())}`;

        if (Array.isArray(data.last_places)) {
            state.lastPlaces = data.last_places;
            data.last_places.forEach((p) => {
                const k = (p.place_name || p.place_cod || "").toString().trim().toUpperCase();
                if (k) state.scannedPlaceCodes.add(k);
            });
            renderLastPlaces();
        }

        sessionsTableBody.innerHTML = sessions.length
            ? sessions
                  .map(
                      (session) => `
                <tr>
                    <td>${session.session_id}</td>
                    <td>${formatDate(session.login_time)}</td>
                    <td>${formatDate(session.logout_time)}</td>
                    <td class="text-center">${session.total_scanned ?? 0}</td>
                    <td class="text-center">${session.with_discrepancy ?? 0}</td>
                    <td class="text-center">${session.is_active ? '<span class="badge text-bg-success">Активна</span>' : '<span class="badge text-bg-secondary">Закрыта</span>'}</td>
                </tr>`
                  )
                  .join("")
            : `
                <tr>
                    <td colspan="6" class="text-center text-muted py-4">Нет данных</td>
                </tr>`;

        updateProgress(overall, today);
        refreshAnimations();
        loadUserDailyChart();
    }

    let userDailyChart = null;
    function loadUserDailyChart() {
        const canvas = document.getElementById("userDailyChart");
        if (!canvas || typeof Chart === "undefined") return;
        API.get(`/api/user/daily-stats/${badge}`).then(({ ok, data }) => {
            if (!ok || data.error) return;
            const daily = data.daily || [];
            const labels = daily.map((d) => d.date?.slice(5) || "");
            const totals = daily.map((d) => d.total || 0);
            const errors = daily.map((d) => d.errors || 0);
            const okData = daily.map((d) => d.ok || 0);
            if (userDailyChart) userDailyChart.destroy();
            const ctx = canvas.getContext("2d");
            userDailyChart = new Chart(ctx, {
                type: "bar",
                data: {
                    labels,
                    datasets: [
                        { label: "Всего", data: totals, backgroundColor: "rgba(110, 43, 98, 0.6)" },
                        { label: "Без расх.", data: okData, backgroundColor: "rgba(5, 150, 105, 0.6)" },
                        { label: "Расхожд.", data: errors, backgroundColor: "rgba(220, 38, 38, 0.6)" },
                    ],
                },
                options: {
                    responsive: true,
                    scales: { x: { stacked: false }, y: { beginAtZero: true } },
                    plugins: { legend: { position: "bottom" } },
                },
            });
        });
    }

    function updateProgress(overall, today) {
        const plan = 120;
        const completion = Math.min(100, Math.round(((today?.today_scanned || 0) / plan) * 100));
        const total = overall?.total_scanned || 0;
        const accurate = total ? Math.round(((overall?.no_discrepancy || 0) / total) * 100) : 100;
        const photoPercent = state.savedCount
            ? Math.min(100, Math.round((state.photoUploads / state.savedCount) * 100))
            : 0;

        if (progressCompletionValue) progressCompletionValue.textContent = `${completion}%`;
        if (progressCompletionBar) progressCompletionBar.style.width = `${completion}%`;
        if (progressAccuracyValue) progressAccuracyValue.textContent = `${accurate}%`;
        if (progressAccuracyBar) progressAccuracyBar.style.width = `${accurate}%`;
        if (progressPhotoValue) progressPhotoValue.textContent = `${photoPercent}%`;
        if (progressPhotoBar) progressPhotoBar.style.width = `${photoPercent}%`;
    }

    async function loadHistory() {
        if (!historyTableBody) return;
        const params = new URLSearchParams();
        params.set("badge", badge);
        const from = historyFromInput?.value;
        const to = historyToInput?.value;
        if (from) params.set("from", from);
        if (to) params.set("to", to);

        historyTableBody.innerHTML =
            '<tr><td colspan="5" class="text-center text-muted py-4">Загружаем историю…</td></tr>';

        const { ok, data } = await API.get(`/api/user/history?${params.toString()}`);
        if (!ok || data.error) {
            historyTableBody.innerHTML = `<tr><td colspan="5" class="text-center text-danger py-4">${
                data?.error || "Не удалось загрузить историю"
            }</td></tr>`;
            return;
        }

        const history = data.history || [];
        if (!history.length) {
            historyTableBody.innerHTML =
                '<tr><td colspan="5" class="text-center text-muted py-4">Записей за выбранный период нет</td></tr>';
            return;
        }

        historyTableBody.innerHTML = history
            .map(
                (item) => `
            <tr>
                <td>${formatDate(item.created_at)}</td>
                <td class="fw-semibold">${item.place_name || "—"}</td>
                <td>${item.status || "—"}</td>
                <td>${item.has_discrepancy ? "Да" : "Нет"}</td>
                <td>${item.has_photo ? '<i class="bi bi-camera-fill text-muted"></i>' : "—"}</td>
            </tr>`
            )
            .join("");
    }

    function applyPlaceData(data, placeCod, fromCache = false) {
        placeTitle.textContent = data.place_name ?? placeCod;
        const mxFloor = document.getElementById("mxFloor");
        const mxRow = document.getElementById("mxRow");
        const mxSection = document.getElementById("mxSection");
        const setPlaceValue = (el, value) => {
            if (!el) return;
            el.textContent = value ?? "—";
            el.classList.toggle("place-value-empty", value == null || value === "—");
        };
        setPlaceValue(mxFloor, data.floor != null ? String(data.floor) : null);
        setPlaceValue(mxRow, data.row_num != null ? String(data.row_num) : null);
        setPlaceValue(mxSection, data.section != null ? String(data.section) : null);
        placeUpdatedLabel.textContent = data.updated_at ? `Обновлено ${formatDate(data.updated_at)}` : "—";
        state.lastMxCode = data.place_cod;
        state.currentPlace = { place_cod: data.place_cod, place_name: data.place_name, qty_db: data.qty_shk, mx_type: data.mx_type };
        showAlert(placeAlert, fromCache ? "Карточка МХ (из кэша)" : "Карточка МХ загружена", "success");
        placeInput.classList.add("is-valid");
        const placeCard = document.getElementById("placeCard");
        if (placeCard) placeCard.classList.add("is-loaded");
        if (state.scanOnlyMode && placeCardSwipeHint && !fromCache) {
            placeCardSwipeHint.classList.remove("d-none");
            setTimeout(() => placeCardSwipeHint?.classList.add("d-none"), 3500);
        }
        logEvent(`Загружено место ${data.place_name ?? data.place_cod}${fromCache ? " [кэш]" : ""}`, "info");
        loadRouteSuggestions();
    }

    async function loadPlace(placeCod, options = {}) {
        hideAlert(placeAlert);
        if (!placeCod) {
            showAlert(placeAlert, "Введите код МХ");
            return;
        }
        placeForm?.classList.add("was-validated");
        placeInput.classList.remove("is-valid", "is-invalid");

        if (!options.skipCache) {
            const cached = PlaceCache.get(placeCod);
            if (cached) {
                applyPlaceData(cached, placeCod, true);
                return;
            }
        }

        const { ok, data } = await API.get(`/api/place/${placeCod}`);
        if (!ok || data.error) {
            const errorMessage = data.error || "Место не найдено";
            showAlert(placeAlert, errorMessage);
            logEvent(errorMessage, "danger");
            placeInput.classList.add("is-invalid");
            SoundFeedback.playError();
            return;
        }
        PlaceCache.set(placeCod, data);
        applyPlaceData(data, placeCod, false);
    }

    function clearPlaceCard() {
        placeTitle.textContent = "—";
        const mxFloor = document.getElementById("mxFloor");
        const mxRow = document.getElementById("mxRow");
        const mxSection = document.getElementById("mxSection");
        [mxFloor, mxRow, mxSection].forEach((el) => {
            if (el) { el.textContent = "—"; el.classList.add("place-value-empty"); }
        });
        placeUpdatedLabel.textContent = "—";
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
        const skipReasonValidation = state.scanOnlyMode && (state.currentStatus === 'ok' || state.currentStatus === 'error');
        if (!skipReasonValidation && state.currentStatus && state.currentStatus !== 'ok') {
            if (!discrepancyReasonSelect?.value) {
                showAlert(placeAlert, "Выберите причину расхождения");
                logEvent("Выберите причину расхождения", "warning");
                return;
            }
        }

        // Формируем причину: основной текст + подпункт (выпадающий или поле ввода) или «Другое»
        let discrepancyReason = '';
        if (skipReasonValidation && state.currentStatus === 'error') discrepancyReason = null;
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
            // Для обратной совместимости отправляем первое фото как photo,
            // а также полный массив photos для новой логики на сервере
            photo: state.photos[0] || state.photoData,
            photos: state.photos,
        };

        saveSpinner?.classList.remove("d-none");
        saveScanBtn?.setAttribute("disabled", "disabled");
        const scanOnlyBtns = document.querySelectorAll(".scan-only-btn");
        scanOnlyBtns.forEach((b) => b?.setAttribute("disabled", "disabled"));
        const { ok, data } = await API.post("/api/scan/complete", payload);
        saveSpinner?.classList.add("d-none");
        saveScanBtn?.removeAttribute("disabled");
        scanOnlyBtns.forEach((b) => b?.removeAttribute("disabled"));

        if (!ok || data.error || !data.success) {
            const errorMessage = data.error || "Не удалось сохранить результат";
            const isQueued = data.error && data.error.includes("очередь");
            showAlert(placeAlert, errorMessage, isQueued ? "info" : "danger");
            logEvent(errorMessage, isQueued ? "info" : "danger");
            if (!isQueued) SoundFeedback.playError();
            if (isQueued) {
                const nextMx = state.quickScanMode && state.suggestions.length
                    ? state.suggestions.find((s) => !state.scannedPlaceCodes.has((s.mx_code || "").toString().trim().toUpperCase()))?.mx_code
                    : null;
                clearPlaceCard();
                loadStats();
                loadRouteSuggestions();
                if (nextMx && placeInput) {
                    placeInput.value = nextMx;
                    placeInput.classList.add("is-valid");
                    loadPlace(nextMx);
                }
                placeInput?.focus();
            }
            return;
        }

        SoundFeedback.playSuccess();
        hideAlert(placeAlert);
        const result = data.result;
        if (result) {
            const placeKey = (result.place_name || result.place_cod || "").toString().trim().toUpperCase();
            if (placeKey) state.scannedPlaceCodes.add(placeKey);
            state.lastPlaces = [result, ...state.lastPlaces].slice(0, 5);
            renderLastPlaces();
        }
        state.savedCount += 1;
        if (state.photoData) {
            state.photoUploads += 1;
        }
        showToastMessage("Результат сохранен");
        logEvent(`Место ${state.lastMxCode} сохранено со статусом ${state.currentStatus}`, "success");
        const nextMx = state.quickScanMode && state.suggestions.length
            ? state.suggestions.find((s) => !state.scannedPlaceCodes.has((s.mx_code || "").toString().trim().toUpperCase()))?.mx_code
            : null;
        clearPlaceCard();
        loadStats();
        loadRouteSuggestions();
        if (nextMx && placeInput) {
            placeInput.value = nextMx;
            placeInput.classList.add("is-valid");
            loadPlace(nextMx);
        }
        placeInput?.focus();
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
        if (!qrStatusBadge) return;
        qrStatusBadge.textContent = text;
        qrStatusBadge.className = `badge text-bg-${type}`;
    }

    function isCameraApiAvailable() {
        return !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia);
    }

    async function stopQrScanner() {
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
        updateQrStatus("Сканер остановлен", "secondary");
    }

    async function startQrScanner() {
        // Проверяем, что контейнер для видео есть на странице
        if (!qrReaderEl) {
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

        if (!qrModalInstance && typeof bootstrap !== "undefined" && qrModalEl) {
            qrModalInstance = new bootstrap.Modal(qrModalEl);
        }
        qrModalInstance?.show();
        updateQrStatus("Инициализация камеры...", "warning");

        if (qrScanner) {
            await stopQrScanner();
        }

        qrScanner = new Html5Qrcode("qrReader");
        try {
            const cameras = await Html5Qrcode.getCameras();
            if (!cameras || !cameras.length) {
                throw new Error("Камера не найдена");
            }

            // Пытаемся выбрать тыльную камеру:
            // 1) по facingMode=environment (если браузер поддерживает),
            // 2) по label (back/rear/тыл/задняя и т.п.),
            // 3) иначе — первая доступная.
            let cameraConfig;

            if ("mediaDevices" in navigator && "getUserMedia" in navigator.mediaDevices) {
                cameraConfig = { facingMode: "environment" };
            } else {
                const lower = (s) => (s || "").toLowerCase();
                const backCamera =
                    cameras.find((cam) => {
                        const label = lower(cam.label);
                        return (
                            label.includes("back") ||
                            label.includes("rear") ||
                            label.includes("зад") ||
                            label.includes("тыл")
                        );
                    }) || cameras[0];
                cameraConfig = { deviceId: { exact: backCamera.id } };
            }

            const qrbox = Math.min(qrReaderEl.offsetWidth || 280, 320);

            await qrScanner.start(
                cameraConfig,
                { fps: 10, qrbox },
                (decodedText) => {
                    if (!decodedText) return;
                    handleQrResult(decodedText);
                },
                () => {
                    /* ignore scan errors */
                }
            );
            updateQrStatus("Наведите камеру на QR-код", "success");
        } catch (error) {
            console.error("QR scanner error", error);
            updateQrStatus(error?.message || "Ошибка камеры", "danger");
            showAlert(
                placeAlert,
                "Не удалось запустить камеру. Попробуйте другой браузер/устройство или используйте сканер штрихкодов/ручной ввод.",
                "danger"
            );
        }
    }

    function handleQrResult(decodedText) {
        if (!decodedText) {
            updateQrStatus("QR не содержит данных", "danger");
            return;
        }

        // Убираем префикс PLCE, извлекаем код МХ (числовой ID или буквенный код)
        const cleanedText = decodedText.trim();
        const match = cleanedText.replace(/^PLCE\s*/i, "").match(/[А-ЯЁA-Z0-9\.]+/);
        if (!match) {
            updateQrStatus("QR не содержит корректный код МХ", "danger");
            return;
        }
        const placeCod = normalizePlaceCode(match[0]) || match[0].toUpperCase();
        placeInput.value = placeCod;
        showToastMessage("QR-код считан");
        qrModalInstance?.hide();
        stopQrScanner();
        loadPlace(placeCod);
    }

    function startNewShift() {
        try {
            sessionStorage.setItem(SHIFT_START_KEY, String(Date.now()));
        } catch (e) {}
        state.lastPlaces = [];
        state.scannedPlaceCodes.clear();
        clearPlaceCard();
        loadStats();
        showAlert(placeAlert, "Новая смена начата. Статистика и отчёты считаются с этого момента.", "info");
        logEvent("Начата новая смена", "info");
    }
    newShiftBtn?.addEventListener("click", startNewShift);
    newShiftBtnScanOnly?.addEventListener("click", startNewShift);

    refreshStatsBtn?.addEventListener("click", () => {
        loadStats();
        logEvent("Статистика обновлена", "info");
    });
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
    voiceSearchBtn?.addEventListener("click", async () => {
        if (!("webkitSpeechRecognition" in window) && !("SpeechRecognition" in window)) {
            showAlert(placeAlert, "Голосовой ввод не поддерживается в этом браузере", "warning");
            return;
        }
        const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
        const rec = new SpeechRecognition();
        rec.lang = "ru-RU";
        rec.continuous = false;
        rec.interimResults = false;
        voiceSearchBtn.classList.add("disabled");
        rec.onresult = (e) => {
            let t = (e.results[0]?.[0]?.transcript || "").trim();
            t = t.replace(/\s+точка\s+/gi, ".").replace(/\s+/g, ".");
            const code = t.replace(/[^А-ЯЁA-Z0-9\.]/gi, "").toUpperCase();
            if (code && placeInput) {
                placeInput.value = code;
                placeInput.classList.add("is-valid");
                loadPlace(code);
                logEvent(`Голос: ${code}`, "info");
            }
            voiceSearchBtn.classList.remove("disabled");
        };
        rec.onerror = rec.onend = () => voiceSearchBtn.classList.remove("disabled");
        rec.start();
    });
    refreshPlaceBtn?.addEventListener("click", () => {
        if (state.lastMxCode) {
            loadPlace(state.lastMxCode, { skipCache: true });
        } else {
            showAlert(placeAlert, "Нет последнего МХ для обновления", "info");
            logEvent("Нет последнего МХ для обновления", "warning");
        }
    });
    clearPlaceBtn?.addEventListener("click", clearPlaceCard);
    historyReloadBtn?.addEventListener("click", () => {
        loadHistory();
        logEvent("История сканов обновлена", "info");
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
    qrModalEl?.addEventListener("hidden.bs.modal", stopQrScanner);
    incidentForm?.addEventListener("submit", async (event) => {
        event.preventDefault();
        const formData = new FormData(incidentForm);
        const payload = {
            badge,
            place_cod: formData.get("ticketPlace") || state.lastMxCode,
            priority: formData.get("ticketPriority") || "medium",
            description: formData.get("ticketDescription"),
        };
        if (!payload.description) {
            incidentStatus.textContent = "Опишите проблему";
            return;
        }
        incidentStatus.textContent = "Отправка...";
        const { ok, data } = await API.post("/api/tickets", payload);
        if (!ok || data.error || !data.success) {
            incidentStatus.textContent = data?.error || "Ошибка отправки";
            logEvent(incidentStatus.textContent, "danger");
            return;
        }
        incidentStatus.textContent = "Отправлено!";
        incidentForm.reset();
        logEvent("Создан тикет на инцидент", "warning");
        setTimeout(() => (incidentStatus.textContent = ""), 4000);
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
        if (!placeCod || !/^[А-ЯЁA-Z0-9\.]+$/i.test(placeCod)) {
            showAlert(placeAlert, "Код МХ должен содержать только буквы, цифры и точки");
            return;
        }
        placeInput.value = placeCod;
        loadPlace(placeCod);
    });

    placeInput?.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
            event.preventDefault();
            const placeCod = normalizePlaceCode(placeInput.value) || placeInput.value.trim().toUpperCase();
            if (placeCod && /^[А-ЯЁA-Z0-9\.]+$/i.test(placeCod)) {
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
    loadStats();
    loadRouteSuggestions();
    renderLastPlaces();
    updateStatusLabel();
    updateOnlineStatus();
    loadHistory();
    refreshAnimations();
    placeInput?.focus();
    window.addEventListener("online", () => {
        updateOnlineStatus();
        syncOfflineQueue();
    });
    window.addEventListener("offline", updateOnlineStatus);
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
        container.innerHTML = '<div class="text-muted small">Нет данных</div>';
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
    const whIdExportSelect = document.getElementById("whIdExportSelect");
    const exportBlockBtn = document.getElementById("exportBlockBtn");
    const blockExportStatus = document.getElementById("blockExportStatus");
    const blockRepairedSection = document.getElementById("blockRepairedSection");
    const blockPlacesBody = document.getElementById("blockPlacesBody");
    const blockPlacesCount = document.getElementById("blockPlacesCount");
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
    const adminStatusBar = document.getElementById("adminStatusBar");
    const adminStatusBarText = document.getElementById("adminStatusBarText");
    const adminStatusChipAccuracy = document.getElementById("adminStatusChipAccuracy");
    const adminStatusChipWorkload = document.getElementById("adminStatusChipWorkload");
    const adminStatusChipRisk = document.getElementById("adminStatusChipRisk");

    let dailyChartInstance = null;
    let statusChartInstance = null;
    let assignTaskModal = null;
    let reportsModal = null;
    let photoPreviewModal = null;

    async function loadAdminStats() {
        const { ok, data } = await API.get("/api/admin/stats");
        if (!ok || data.error) {
            overallAccuracyLabel.textContent = data.error || "Ошибка загрузки статистики";
            return;
        }

        const overall = data.overall || {};
        const employees = data.employees || [];
        const discrepancyTypes = data.discrepancy_types || [];

        overallScannedValue.textContent = overall.total_scanned ?? 0;
        overallDiscrepancyValue.textContent = overall.with_discrepancy ?? 0;
        overallEmployeesValue.textContent = overall.total_employees ?? 0;
        overallPlacesValue.textContent = overall.unique_places ?? 0;

        const accuracy = overall.total_scanned
            ? (((overall.no_discrepancy || 0) / overall.total_scanned) * 100).toFixed(1)
            : "0.0";
        overallAccuracyLabel.textContent = `Точность ${accuracy}%`;
        if (weeklyDelta) {
            weeklyDelta.textContent = `${accuracy}% точность`;
            weeklyDelta.className = `badge text-bg-${accuracy >= 95 ? "success" : accuracy >= 85 ? "warning" : "danger"}`;
        }

        // обновляем нижнюю статус-линию
        if (adminStatusBar && adminStatusBarText) {
            adminStatusBar.classList.remove("d-none");
            const total = overall.total_scanned ?? 0;
            const withDisc = overall.with_discrepancy ?? 0;
            const activeEmps = overall.total_employees ?? 0;
            adminStatusBarText.textContent = `Сегодня: ${total} сканов • ${withDisc} с расхождениями • ${activeEmps} активных сотрудников`;

            if (adminStatusChipAccuracy) {
                adminStatusChipAccuracy.textContent = `Точность ${accuracy}%`;
            }
            if (adminStatusChipWorkload) {
                adminStatusChipWorkload.textContent = `Нагрузка: ${total} сканов`;
            }
            if (adminStatusChipRisk) {
                const risk = accuracy >= 95 ? "Низкие риски" : accuracy >= 85 ? "Средние риски" : "Высокие риски";
                adminStatusChipRisk.textContent = risk;
            }
        }

        renderList(
            topEmployeesList,
            employees.slice(0, 5),
            (employee) => `
                <div class="d-flex justify-content-between align-items-center">
                    <div>
                        <div class="fw-semibold">${employee.badge}</div>
                        <small class="text-muted">${employee.scanned} сканов, ${employee.total_hours}ч</small>
                    </div>
                    <span class="badge text-bg-${employee.accuracy >= 95 ? "success" : "warning"}">${employee.accuracy}%</span>
                </div>`
        );

        renderList(
            discrepancyTypesList,
            discrepancyTypes,
            (item) => `
                <div class="list-group-item d-flex justify-content-between align-items-center">
                    <span>${item.status || "—"}</span>
                    <span class="badge text-bg-danger">${item.count}</span>
                </div>`
        );
    }

    async function loadAnalytics() {
        const { ok, data } = await API.get("/api/admin/analytics");
        if (!ok || data.error) {
            if (dailyChartCanvas) {
                dailyChartCanvas.innerHTML = `<div class="text-danger">${data.error || "Ошибка загрузки аналитики"}</div>`;
            }
            return;
        }

        const dailyStats = data.daily_stats || [];
        const problemZones = data.problem_zones || [];
        renderDailyChart(dailyStats);
        renderStatusChart(dailyStats);

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
        const { ok, data } = await API.get("/api/admin/reviews");
        if (!ok || data.error) {
            qualityTableBody.innerHTML = `<tr><td colspan="4" class="text-center text-danger py-3">${data?.error || "Ошибка загрузки"}</td></tr>`;
            qualityReviewList.innerHTML = '<div class="list-group-item text-muted">Ревизии не назначены</div>';
            return;
        }

        const aggregates = data.aggregates || [];
        qualityTableBody.innerHTML = aggregates.length
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
        qualityReviewList.innerHTML = reviews.length
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
            : '<div class="list-group-item text-muted">Ревизии не назначены</div>';
        refreshAnimations();
    }

    async function loadTickets() {
        const { ok, data } = await API.get("/api/admin/tickets");
        if (!ok || data.error) {
            ticketsBoard.innerHTML = `<div class="text-danger small">${data?.error || "Ошибка загрузки"}</div>`;
            return;
        }

        const tickets = data.tickets || [];
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
    loadTickets();

    refreshAdminBtn?.addEventListener("click", () => {
        loadAdminStats();
        loadAnalytics();
        loadActiveTasks();
        loadLatestScans();
        loadActivityLog();
        loadQualityData();
        loadTickets();
        showToastMessage("Данные админки обновлены", "info");
    });

    clearAdminLogBtn?.addEventListener("click", () => {
        adminActivityList.innerHTML = "Событий нет";
    });

    reportsModalEl?.addEventListener("show.bs.modal", () => {
        loadReports();
        loadWhIds();
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

    reportsTableBody?.addEventListener("click", (event) => {
        const btn = event.target.closest("[data-report-id]");
        if (!btn) return;
        const reportId = btn.dataset.reportId;
        window.open(`/api/admin/reports/${reportId}/download`, "_blank");
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
        if (!whIdExportSelect) return;
        const { ok, data } = await API.get("/api/admin/wh_ids");
        whIdExportSelect.innerHTML = "<option value=\"\">Выберите wh_id</option>";
        if (!ok || data.error) return;
        const list = data.wh_ids || [];
        list.forEach((item) => {
            const opt = document.createElement("option");
            opt.value = item.wh_id;
            opt.textContent = item.warehouse_name
                ? `${item.wh_id} — ${item.warehouse_name}`
                : String(item.wh_id);
            whIdExportSelect.appendChild(opt);
        });
    }

    async function loadBlockPlaces(whId) {
        if (!blockRepairedSection || !blockPlacesBody || !blockPlacesCount) return;
        blockRepairedSection.style.display = "none";
        blockPlacesBody.innerHTML = "<tr><td colspan=\"5\" class=\"text-center text-muted py-2\">Загрузка…</td></tr>";
        const { ok, data } = await API.get(`/api/admin/block/errors?wh_id=${encodeURIComponent(whId)}`);
        if (!ok || data.error) {
            blockPlacesBody.innerHTML = `<tr><td colspan="5" class="text-center text-danger py-2">${data.error || "Ошибка загрузки"}</td></tr>`;
            blockRepairedSection.style.display = "block";
            return;
        }
        const places = data.places || [];
        if (places.length === 0) {
            blockPlacesBody.innerHTML = "<tr><td colspan=\"5\" class=\"text-center text-muted py-2\">Нет мест с ошибками по этому складу</td></tr>";
            blockPlacesCount.textContent = "";
        } else {
            blockPlacesBody.innerHTML = places.map((p) => {
                const repaired = p.is_repaired ? "checked" : "";
                const rowClass = p.is_repaired ? "table-success" : "";
                return `<tr class="${rowClass}" data-place-cod="${p.place_cod}">
                    <td>${(p.place_name || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")} / ${String(p.place_cod)}</td>
                    <td>${p.floor != null ? p.floor : "—"}</td>
                    <td>${p.row_num != null ? p.row_num : "—"}</td>
                    <td>${p.section != null ? p.section : "—"}</td>
                    <td class="text-center">
                        <input type="checkbox" class="form-check-input repaired-cb" ${repaired} data-place-cod="${p.place_cod}" aria-label="Починено">
                    </td>
                </tr>`;
            }).join("");
            const repairedCount = places.filter((p) => p.is_repaired).length;
            blockPlacesCount.textContent = `Мест: ${places.length}, починено: ${repairedCount}. Починенные не попадают в выгрузку.`;
            blockPlacesBody.querySelectorAll(".repaired-cb").forEach((cb) => {
                cb.addEventListener("change", async function () {
                    const placeCod = this.dataset.placeCod;
                    const isRepaired = this.checked;
                    const method = isRepaired ? "POST" : "DELETE";
                    const body = isRepaired ? JSON.stringify({ wh_id: parseInt(whId, 10), place_cod: parseInt(placeCod, 10) }) : undefined;
                    const url = isRepaired ? "/api/admin/block/repaired" : `/api/admin/block/repaired?wh_id=${encodeURIComponent(whId)}&place_cod=${encodeURIComponent(placeCod)}`;
                    const res = await fetch(url, { method, credentials: "include", headers: body ? { "Content-Type": "application/json" } : {}, body });
                    const json = await res.json().catch(() => ({}));
                    if (res.ok && json.success) {
                        const row = this.closest("tr");
                        if (row) row.classList.toggle("table-success", isRepaired);
                    } else {
                        this.checked = !isRepaired;
                    }
                });
            });
        }
        blockRepairedSection.style.display = "block";
    }

    whIdExportSelect?.addEventListener("change", () => {
        const whId = whIdExportSelect.value?.trim();
        if (whId) loadBlockPlaces(whId);
        else if (blockRepairedSection) blockRepairedSection.style.display = "none";
    });

    exportBlockBtn?.addEventListener("click", async () => {
        const whId = whIdExportSelect?.value?.trim();
        if (!blockExportStatus) return;
        blockExportStatus.textContent = "";
        if (!whId) {
            blockExportStatus.textContent = "Выберите wh_id";
            blockExportStatus.className = "small text-warning";
            return;
        }
        exportBlockBtn.disabled = true;
        blockExportStatus.className = "small text-muted";
        blockExportStatus.textContent = "Формируем отчёт…";
        try {
            const base = window.location.origin;
            const url = `${base}/api/admin/export/block?wh_id=${encodeURIComponent(whId)}`;
            const res = await fetch(url, { credentials: "include" });
            if (!res.ok) {
                const err = await res.json().catch(() => ({}));
                blockExportStatus.textContent = err.error || `Ошибка ${res.status}`;
                blockExportStatus.className = "small text-danger";
                return;
            }
            const blob = await res.blob();
            const name = res.headers.get("Content-Disposition")?.match(/filename="?([^";]+)"?/)?.[1] || `errors_wh_id_${whId}.xlsx`;
            const a = document.createElement("a");
            a.href = URL.createObjectURL(blob);
            a.download = name;
            a.click();
            URL.revokeObjectURL(a.href);
            blockExportStatus.textContent = "Скачано";
            blockExportStatus.className = "small text-success";
        } catch (e) {
            blockExportStatus.textContent = "Ошибка: " + (e.message || "сеть");
            blockExportStatus.className = "small text-danger";
        } finally {
            exportBlockBtn.disabled = false;
        }
    });

    async function loadReports() {
        const { ok, data } = await API.get("/api/admin/reports");
        if (!ok || data.error) {
            reportsTableBody.innerHTML = `<tr><td colspan="5" class="text-danger text-center py-3">${data.error || "Ошибка загрузки"}</td></tr>`;
            return;
        }
        const reports = data.reports || [];
        reportsTableBody.innerHTML = reports.length
            ? reports
                  .map(
                      (report) => `
                <tr>
                    <td>#${report.report_id}</td>
                    <td>${report.filename}</td>
                    <td>${report.badge}</td>
                    <td>${report.total_scanned}</td>
                    <td>
                        <button class="btn btn-sm btn-outline-primary" data-report-id="${report.report_id}">
                            Скачать
                        </button>
                    </td>
                </tr>`
                  )
                  .join("")
            : `<tr><td colspan="5" class="text-center text-muted py-3">Отчеты отсутствуют</td></tr>`;
    }
}

document.addEventListener("DOMContentLoaded", () => {
    initThemeToggle();
    initAnimations();
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

