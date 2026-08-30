function escapeHtml(str) {
    return String(str)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

function formatUptime(seconds) {
    if (seconds == null || isNaN(seconds) || seconds < 0) return '0s';
    const days = Math.floor(seconds / 86400);
    const hrs = Math.floor((seconds % 86400) / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);

    if (days > 0) return `${days}d ${hrs}h ${mins}m`;
    if (hrs > 0) return `${hrs}h ${mins}m`;
    if (mins > 0) return `${mins}m ${secs}s`;
    return `${secs}s`;
}

// ==========================================
// TELEMETRY STATE & LOGIC
// ==========================================
// TELEMETRY STATE & LOGIC
// ==========================================
let activeServicesCache = [];
let statusPollInterval = null;
let liveTimerInterval = null;

function startLiveTimerLoop() {
    if (liveTimerInterval) clearInterval(liveTimerInterval);

    function tick() {
        const timerEls = document.querySelectorAll('.timer-text[data-started]');
        if (timerEls.length === 0) return;
        const now = Date.now();

        timerEls.forEach(el => {
            const startedAt = new Date(el.getAttribute('data-started')).getTime();
            if (isNaN(startedAt)) return;

            const elapsedSec = Math.max(0, Math.floor((now - startedAt) / 1000));
            const mins = String(Math.floor(elapsedSec / 60)).padStart(2, '0');
            const secs = String(elapsedSec % 60).padStart(2, '0');
            el.textContent = `⏱ ${mins}:${secs}s`;
        });
    }

    liveTimerInterval = setInterval(tick, 1000);
}

function getApiBase() {
    const path = window.location.pathname.replace(/\/+$/, '');
    return path.endsWith('/dashboard') ? path : (path + '/dashboard').replace(/\/+/g, '/').replace(/\/+$/, '');
}

async function fetchStatus() {
    try {
        const res = await fetch(getApiBase() + '/api/status');
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        const refreshEl = document.getElementById('last-refresh');
        if (refreshEl) refreshEl.textContent = new Date(data.system_time).toLocaleTimeString();
        
        const overallStatusEl = document.getElementById('overall-status');
        if (overallStatusEl) overallStatusEl.textContent = escapeHtml(data.cluster_status.toUpperCase());
        
        // --- 1. RENDER DEPLOYMENT PIPELINE ---
        const deploysContainer = document.getElementById('deployments-container');
        const deployBadge = document.getElementById('deploy-count-badge');
        const deploys = data.deployments || [];

        const hasFailed = deploys.some(dep => dep.stage === 'failed');
        const isBuilding = deploys.some(dep => dep.stage === 'building' || dep.stage === 'queued');
        const isSwapping = deploys.some(dep => dep.stage === 'swapping');
        const isAwaitingPull = deploys.some(dep => dep.stage === 'awaiting_pull');
        const activeDeploys = deploys.filter(dep => dep.stage !== 'live' && dep.stage !== 'completed');

        if (deployBadge) {
            if (hasFailed) {
                deployBadge.textContent = `🚨 CI BUILD FAILED`;
                deployBadge.className = 'section-badge failed';
            } else if (isBuilding) {
                deployBadge.textContent = `⚡ 1 CI BUILD ACTIVE`;
                deployBadge.className = 'section-badge building';
            } else if (isSwapping) {
                deployBadge.textContent = `🔄 WATCHTOWER SWAPPING`;
                deployBadge.className = 'section-badge swapping';
            } else if (isAwaitingPull) {
                deployBadge.textContent = `⬇️ AWAITING WATCHTOWER PULL`;
                deployBadge.className = 'section-badge active';
            } else if (activeDeploys.length > 0) {
                deployBadge.textContent = `${activeDeploys.length} IN PROGRESS`;
                deployBadge.className = 'section-badge active';
            } else if (deploys.length > 0) {
                deployBadge.textContent = `ALL SERVICES LIVE (${deploys.length} SYNCED)`;
                deployBadge.className = 'section-badge live';
            } else {
                deployBadge.textContent = 'SYSTEM IN SYNC';
                deployBadge.className = 'section-badge';
            }
        }

        if (deploysContainer) {
            deploysContainer.innerHTML = '';

            if (deploys.length === 0) {
                deploysContainer.innerHTML = `
                    <div class="deploy-idle-card">
                        <div class="idle-indicator">
                            <span class="pulse-dot healthy"></span>
                            <span>ALL SERVICES IN SYNC // NO PENDING DEPLOYS</span>
                        </div>
                        <div>WATCHTOWER POLLING GHCR (60s)</div>
                    </div>
                `;
            } else {
                deploys.forEach(dep => {
                    const isLive = dep.stage === 'live';
                    const isFailed = dep.stage === 'failed';
                    const isBuildingStage = dep.stage === 'building' || dep.stage === 'queued';

                    const steps = dep.steps || [
                        { name: "Commit Trigger", icon: "📦", status: "completed" },
                        { name: "CI Build & GHCR", icon: "⚙️", status: "completed" },
                        { name: "Watchtower Pull", icon: "⬇️", status: "completed" },
                        { name: "Container Swap", icon: "🔄", status: isLive ? "completed" : "active" },
                        { name: "Health Check", icon: "🩺", status: isLive ? "completed" : "pending" }
                    ];

                    const stepsHTML = steps.map(step => {
                        let matrixHTML = '';
                        if (step.name.includes("CI Build") && Array.isArray(dep.matrix_jobs) && dep.matrix_jobs.length > 0) {
                            matrixHTML = `
                                <div class="matrix-chips-container">
                                    ${dep.matrix_jobs.map(chip => {
                                        const chipClass = chip.status === 'completed' ? 'chip-done' : chip.status === 'active' ? 'chip-running' : chip.status === 'failed' ? 'chip-failed' : 'chip-queued';
                                        const chipIcon = chip.status === 'completed' ? '✓' : chip.status === 'active' ? '⚡' : chip.status === 'failed' ? '✕' : '○';
                                        return `<span class="matrix-chip ${chipClass}" title="${escapeHtml(chip.name)}: ${chip.status}">${escapeHtml(chip.name)} ${chipIcon}</span>`;
                                    }).join('')}
                                </div>
                            `;
                        }

                        const statusBadge = step.status === 'completed' ? '✓ DONE' : step.status === 'active' ? '⚡ RUNNING' : step.status === 'failed' ? '✕ FAILED' : '○ PENDING';

                        return `
                            <div class="step-panel step-${escapeHtml(step.status)}">
                                <div class="step-header">
                                    <span class="step-icon">${escapeHtml(step.icon)}</span>
                                    <span class="step-status-badge">${statusBadge}</span>
                                </div>
                                <div class="step-name">${escapeHtml(step.name)}</div>
                                ${matrixHTML}
                            </div>
                        `;
                    }).join('');

                    const safeCommit = escapeHtml(dep.commit || 'latest');
                    const commitMarkup = dep.commit && dep.commit !== 'latest'
                        ? `<a href="https://github.com/azylman/aerial/commit/${safeCommit}" target="_blank" rel="noopener" class="deploy-commit-link" title="View commit on GitHub">${safeCommit} ↗</a>`
                        : `<span class="deploy-commit">${safeCommit}</span>`;

                    let runLinkMarkup = '';
                    if (dep.html_url && typeof dep.html_url === 'string' && dep.html_url.startsWith('https://github.com/')) {
                        const safeUrl = escapeHtml(dep.html_url);
                        runLinkMarkup = `<a href="${safeUrl}" target="_blank" rel="noopener" class="deploy-run-link" title="Open GitHub Actions Run">RUN LOGS ↗</a>`;
                    }

                    let timerMarkup = '';
                    if (isBuildingStage && dep.started_at) {
                        timerMarkup = `
                            <div class="deploy-timer-badge">
                                <span class="pulse-indicator"></span>
                                <span class="timer-text" data-started="${escapeHtml(dep.started_at)}">⏱ 00:00s</span>
                            </div>
                        `;
                    }

                    const cardClass = isLive ? 'stage-live' : isFailed ? 'stage-failed' : isBuildingStage ? 'stage-building' : 'stage-active';
                    const badgeClass = isLive ? 'live' : isFailed ? 'failed' : 'active';

                    const card = document.createElement('div');
                    card.className = `deploy-card ${cardClass}`;
                    card.innerHTML = `
                        ${isBuildingStage ? '<div class="deploy-card-laser"></div>' : ''}
                        <div class="deploy-card-header">
                            <div class="deploy-target">
                                <span class="deploy-service-name">${escapeHtml(dep.service.toUpperCase())}</span>
                                ${commitMarkup}
                                ${runLinkMarkup}
                            </div>
                            <div style="display: flex; align-items: center; gap: 8px;">
                                ${timerMarkup}
                                <span class="deploy-stage-badge ${badgeClass}">⚡ ${escapeHtml(dep.stage.toUpperCase())}</span>
                            </div>
                        </div>
                        <div class="deploy-steps-grid">
                            ${stepsHTML}
                        </div>
                        <div class="deploy-progress-bg">
                            <div class="deploy-progress-fill" style="width: ${dep.progress}%;"></div>
                        </div>
                        <div class="deploy-footer">
                            <span>${dep.commit_msg ? `"${escapeHtml(dep.commit_msg)}"` : `STAGE: ${escapeHtml(dep.stage.toUpperCase())} (${dep.progress}%)`}</span>
                            <span>STARTED: ${new Date(dep.started_at).toLocaleTimeString()}</span>
                        </div>
                    `;
                    deploysContainer.appendChild(card);
                });
            }
        }

        // --- 2. RENDER SERVICES GRID ---
        activeServicesCache = data.services || [];
        const grid = document.getElementById('services-grid');
        if (grid) {
            grid.innerHTML = '';
            let healthyCount = 0;
            activeServicesCache.forEach((svc, index) => {
                if (svc.status === 'healthy') healthyCount++;
                
                const safeName = escapeHtml(svc.name);
                const safeStatus = escapeHtml(svc.status);
                const formattedUptime = formatUptime(svc.uptime_seconds);

                const card = document.createElement('div');
                card.className = 'service-card';
                card.onclick = () => openDiagnosticDrawer(index);

                card.innerHTML = `
                    <div class="header">
                        <span class="title">${safeName}</span>
                        <div class="status-badge-container">
                            <span class="pulse-dot ${safeStatus}"></span>
                            <span class="badge ${safeStatus}">${safeStatus.toUpperCase()}</span>
                        </div>
                    </div>
                    <div class="service-metrics">
                        <span>UPTIME: ${formattedUptime}</span>
                        <span>PORT: READY</span>
                    </div>
                    <div class="card-action-hint">
                        CLICK FOR DIAGNOSTICS &rarr;
                    </div>
                `;
                grid.appendChild(card);
            });

            // Update active count
            const total = activeServicesCache.length;
            const activeCountEl = document.getElementById('active-count');
            if (activeCountEl) activeCountEl.textContent = `${healthyCount} / ${total}`;

            // Permet Score Calculation
            const healthRatio = total > 0 ? (healthyCount / total) : 0;
            const permetBar = document.getElementById('permet-bar-fill');
            const permetVal = document.getElementById('permet-score-val');
            
            if (permetBar && permetVal) {
                const percent = Math.round(healthRatio * 100);
                permetBar.style.width = `${percent}%`;

                if (healthRatio === 1) {
                    permetVal.textContent = 'LVL 5 // OPTIMAL';
                    permetVal.style.color = 'var(--neon-cyan)';
                } else if (healthRatio > 0.7) {
                    permetVal.textContent = 'LVL 3 // STABLE';
                    permetVal.style.color = 'var(--neon-amber)';
                } else {
                    permetVal.textContent = 'LVL 1 // WARNING';
                    permetVal.style.color = 'var(--neon-red)';
                }
            }
        }

    } catch (err) {
        console.error('Failed to fetch system status:', err);
        const overallStatusEl = document.getElementById('overall-status');
        if (overallStatusEl) {
            overallStatusEl.textContent = 'OFFLINE';
            overallStatusEl.className = 'value text-danger';
        }
        const clusterSubEl = document.getElementById('cluster-sub');
        if (clusterSubEl) clusterSubEl.textContent = 'SYSTEM DISCONNECTED';
    }
}

// Drawer functionality
function openDiagnosticDrawer(serviceIndex) {
    const svc = activeServicesCache[serviceIndex];
    if (!svc) return;

    const drawer = document.getElementById('diagnostic-drawer');
    const overlay = document.getElementById('drawer-overlay');
    const title = document.getElementById('drawer-service-name');
    const body = document.getElementById('drawer-body');

    if (title) title.textContent = `DIAGNOSTICS // ${svc.name.toUpperCase()}`;

    const hours = Math.floor(svc.uptime_seconds / 3600);
    const mins = Math.floor((svc.uptime_seconds % 3600) / 60);
    const secs = svc.uptime_seconds % 60;

    if (body) {
        body.innerHTML = `
            <div class="diag-grid">
                <div class="diag-item">
                    <span class="lbl">SERVICE NAME</span>
                    <span class="val">${escapeHtml(svc.name)}</span>
                </div>
                <div class="diag-item">
                    <span class="lbl">HEALTH STATUS</span>
                    <span class="val ${svc.status === 'healthy' ? 'text-success' : 'text-danger'}">${escapeHtml(svc.status.toUpperCase())}</span>
                </div>
                <div class="diag-item">
                    <span class="lbl">UPTIME CONTEXT</span>
                    <span class="val">${formatUptime(svc.uptime_seconds)}</span>
                </div>
                <div class="diag-item">
                    <span class="lbl">DISCOVERY NODE</span>
                    <span class="val">192.168.1.14 (aerial-net)</span>
                </div>
            </div>
            <div class="console-box">
                <div>[TELEMETRY LOG STREAM]</div>
                <div>&gt; Service node ${escapeHtml(svc.name)} initialized via Docker Supervisor.</div>
                <div>&gt; Healthcheck ping: 200 OK (latency &lt; 2ms)</div>
                <div>&gt; Permet link established. Memory footprint nominal.</div>
            </div>
        `;
    }

    if (drawer) drawer.classList.add('active');
    if (overlay) overlay.classList.add('active');
}

function closeDiagnosticDrawer() {
    const drawer = document.getElementById('diagnostic-drawer');
    const overlay = document.getElementById('drawer-overlay');
    if (drawer) drawer.classList.remove('active');
    if (overlay) overlay.classList.remove('active');
}

const drawerCloseBtn = document.getElementById('drawer-close-btn');
if (drawerCloseBtn) drawerCloseBtn.addEventListener('click', closeDiagnosticDrawer);
const drawerOverlay = document.getElementById('drawer-overlay');
if (drawerOverlay) drawerOverlay.addEventListener('click', closeDiagnosticDrawer);


// ==========================================
// PERMET MEMORY ARCHIVE STATE & LOGIC
// ==========================================
const memoryState = {
    facts: [],
    filteredFacts: [],
    selectedCategory: 'ALL',
    searchQuery: '',
    isLoading: false,
    error: null
};

async function fetchFacts() {
    if (memoryState.isLoading) return;
    memoryState.isLoading = true;
    memoryState.error = null;
    renderFactsSkeleton();

    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 6000);

        const res = await fetch(getApiBase() + '/api/facts?limit=100', { signal: controller.signal });
        clearTimeout(timeoutId);

        const data = await res.json();
        if (!res.ok) {
            throw new Error(data.error || `HTTP ${res.status}: Brain proxy unavailable`);
        }

        memoryState.facts = Array.isArray(data.facts) ? data.facts : [];
        memoryState.isLoading = false;

        updateMemoryMetrics();
        renderCategoryPills();
        applyFilters();

    } catch (err) {
        console.error('Failed to fetch memory facts:', err);
        memoryState.isLoading = false;
        memoryState.error = err.message || 'Brain service unreachable';
        renderMemoryError(memoryState.error);
    }
}

function renderFactsSkeleton() {
    const grid = document.getElementById('memory-cards-grid');
    if (!grid) return;
    grid.innerHTML = Array(6).fill(0).map(() => `
        <div class="fact-card-skeleton">
            <div class="skeleton-box" style="width: 32%; height: 18px;"></div>
            <div class="skeleton-box" style="width: 100%; height: 50px; margin: 12px 0;"></div>
            <div class="skeleton-box" style="width: 48%; height: 14px;"></div>
        </div>
    `).join('');
}

function renderMemoryError(errMsg) {
    const grid = document.getElementById('memory-cards-grid');
    if (!grid) return;
    grid.innerHTML = `
        <div class="permet-alert-box">
            <h3>⚡ PERMET LINK SEVERED // BRAIN OFFLINE</h3>
            <p>${escapeHtml(errMsg)}</p>
            <button class="cyber-btn-secondary" onclick="fetchFacts()" style="margin: 0 auto;">
                <span class="refresh-icon">⚡</span> RETRY SYNCHRONIZATION
            </button>
        </div>
    `;
    const statusBadge = document.getElementById('memory-status-badge');
    if (statusBadge) {
        statusBadge.textContent = 'DEGRADED';
        statusBadge.className = 'value text-danger';
    }
}

function updateMemoryMetrics() {
    const total = memoryState.facts.length;
    const totalCountEl = document.getElementById('memory-total-count');
    if (totalCountEl) totalCountEl.textContent = total;

    const categories = new Set(memoryState.facts.map(f => (f.category || 'general').toLowerCase()));
    const catCountEl = document.getElementById('memory-category-count');
    if (catCountEl) catCountEl.textContent = `${categories.size} CATEGORIES`;

    const avgImp = total > 0
        ? (memoryState.facts.reduce((acc, f) => acc + (f.importance || 1.0), 0) / total).toFixed(2)
        : '0.00';
    const avgImpEl = document.getElementById('memory-avg-importance');
    if (avgImpEl) avgImpEl.textContent = avgImp;

    const statusBadge = document.getElementById('memory-status-badge');
    if (statusBadge) {
        statusBadge.textContent = 'OPTIMAL';
        statusBadge.className = 'value text-success';
    }
    const lastSyncEl = document.getElementById('memory-last-sync');
    if (lastSyncEl) lastSyncEl.textContent = `SYNCED ${new Date().toLocaleTimeString()}`;
}

function renderCategoryPills() {
    const counts = { ALL: memoryState.facts.length };
    memoryState.facts.forEach(f => {
        const cat = (f.category || 'general').toUpperCase().replace(' ', '_');
        counts[cat] = (counts[cat] || 0) + 1;
    });

    const pillsBar = document.getElementById('category-pills-bar');
    if (!pillsBar) return;
    const categories = Object.keys(counts);

    pillsBar.innerHTML = categories.map(cat => `
        <button class="cat-pill ${memoryState.selectedCategory === cat ? 'active' : ''}" data-category="${escapeHtml(cat)}">
            ${escapeHtml(cat.replace('_', ' '))} <span class="pill-count">${counts[cat]}</span>
        </button>
    `).join('');

    pillsBar.querySelectorAll('.cat-pill').forEach(pill => {
        pill.addEventListener('click', () => {
            memoryState.selectedCategory = pill.getAttribute('data-category');
            renderCategoryPills();
            applyFilters();
        });
    });
}

function applyFilters() {
    const query = memoryState.searchQuery.toLowerCase().trim();
    const cat = memoryState.selectedCategory;

    memoryState.filteredFacts = memoryState.facts.filter(f => {
        const fCat = (f.category || 'general').toUpperCase().replace(' ', '_');
        const matchesCat = (cat === 'ALL') || (fCat === cat);
        if (!matchesCat) return false;

        if (!query) return true;
        const text = (f.fact_text || '').toLowerCase();
        const category = (f.category || '').toLowerCase();
        const thread = (f.thread_id || '').toLowerCase();
        return text.includes(query) || category.includes(query) || thread.includes(query);
    });

    const counterEl = document.getElementById('results-count-text');
    if (counterEl) {
        counterEl.textContent = `SHOWING ${memoryState.filteredFacts.length} / ${memoryState.facts.length} NODES`;
    }

    renderFactCards();
}

function renderFactCards() {
    const grid = document.getElementById('memory-cards-grid');
    if (!grid) return;

    if (memoryState.filteredFacts.length === 0) {
        grid.innerHTML = `
            <div class="empty-state-box">
                <div style="font-size: 1.6rem; margin-bottom: 0.6rem;">🌌</div>
                <div style="font-weight: 700; color: #fff;">NO PERMET MEMORY NODES MATCH CRITERIA</div>
                <div style="font-size: 0.75rem; color: var(--text-dim); margin-top: 6px;">Try adjusting your keyword filter or switching category pills.</div>
            </div>
        `;
        return;
    }

    const query = memoryState.searchQuery.trim();

    grid.innerHTML = memoryState.filteredFacts.map((f, idx) => {
        const importance = Number(f.importance) || 1.0;
        const isHigh = importance >= 0.8;
        const cat = (f.category || 'GENERAL').toUpperCase();
        const catSlug = (f.category || 'general').toLowerCase().replace(/[^a-z0-9_-]/g, '_');
        const timeAgo = formatTimeAgo(f.created_at);
        const rawText = f.fact_text || '';
        const highlightedText = query ? highlightSearch(rawText, query) : escapeHtml(rawText);
        const importancePct = Math.min(100, Math.round(importance * 100));

        let threadMarkup = '';
        if (f.thread_id && /^\d+$/.test(f.thread_id.trim())) {
            const safeThread = escapeHtml(f.thread_id.trim());
            threadMarkup = ` • <a href="/conversations/?thread=${safeThread}" class="fact-thread-link" target="_blank" rel="noopener">#${safeThread.slice(0, 8)} ↗</a>`;
        }

        return `
            <div class="fact-card ${isHigh ? 'high-importance' : ''}">
                <div class="fact-card-header">
                    <span class="fact-category-tag cat-${escapeHtml(catSlug)}">${escapeHtml(cat)}</span>
                    <div class="fact-importance-gauge">
                        <span>IMP ${importance.toFixed(2)}</span>
                        <div class="fact-importance-bar-mini">
                            <div class="fact-importance-fill-mini" style="width: ${importancePct}%;"></div>
                        </div>
                    </div>
                </div>
                <div class="fact-body">${highlightedText}</div>
                <div class="fact-footer">
                    <span>⏱ ${timeAgo}${threadMarkup}</span>
                    <button class="fact-copy-btn" onclick="copyFactText(${idx}, this)" aria-label="Copy fact text">
                        📋 COPY
                    </button>
                </div>
            </div>
        `;
    }).join('');
}

function highlightSearch(text, query) {
    if (!query) return escapeHtml(text);
    const escapedText = escapeHtml(text);
    const escapedQuery = escapeHtml(query);
    const regex = new RegExp(`(${escapedQuery.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
    return escapedText.replace(regex, '<mark class="cyber-highlight">$1</mark>');
}

function formatTimeAgo(dateStr) {
    if (!dateStr) return 'RECENT';
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return 'RECENT';
    const sec = Math.floor((Date.now() - date.getTime()) / 1000);
    if (sec < 60) return `${sec}s AGO`;
    const min = Math.floor(sec / 60);
    if (min < 60) return `${min}m AGO`;
    const hrs = Math.floor(min / 60);
    if (hrs < 24) return `${hrs}h AGO`;
    return `${Math.floor(hrs / 24)}d AGO`;
}

async function copyFactText(index, btnElement) {
    const fact = memoryState.filteredFacts[index];
    if (!fact) return;
    try {
        await navigator.clipboard.writeText(fact.fact_text);
        btnElement.textContent = '✓ COPIED';
        btnElement.classList.add('copied');
        setTimeout(() => {
            btnElement.textContent = '📋 COPY';
            btnElement.classList.remove('copied');
        }, 1800);
    } catch (e) {
        console.warn('Clipboard write failed:', e);
    }
}


// ==========================================
// SPA TAB NAVIGATION & ROUTING
// ==========================================
function setupTabs() {
    const telemetryBtn = document.getElementById('tab-telemetry-btn');
    const memoryBtn = document.getElementById('tab-memory-btn');
    const telemetryView = document.getElementById('telemetry-view');
    const memoryView = document.getElementById('memory-view');

    function switchView(tab) {
        if (tab === 'memory') {
            if (telemetryBtn) {
                telemetryBtn.classList.remove('active');
                telemetryBtn.setAttribute('aria-selected', 'false');
            }
            if (memoryBtn) {
                memoryBtn.classList.add('active');
                memoryBtn.setAttribute('aria-selected', 'true');
            }
            if (telemetryView) telemetryView.style.display = 'none';
            if (memoryView) memoryView.style.display = 'block';

            // Pause status polling while browsing memory
            if (statusPollInterval) {
                clearInterval(statusPollInterval);
                statusPollInterval = null;
            }

            if (memoryState.facts.length === 0) {
                fetchFacts();
            }
        } else {
            if (memoryBtn) {
                memoryBtn.classList.remove('active');
                memoryBtn.setAttribute('aria-selected', 'false');
            }
            if (telemetryBtn) {
                telemetryBtn.classList.add('active');
                telemetryBtn.setAttribute('aria-selected', 'true');
            }
            if (memoryView) memoryView.style.display = 'none';
            if (telemetryView) telemetryView.style.display = 'block';

            // Resume status polling
            if (!statusPollInterval) {
                fetchStatus();
                statusPollInterval = setInterval(fetchStatus, 5000);
            }
        }
    }

    if (telemetryBtn) {
        telemetryBtn.addEventListener('click', () => {
            window.location.hash = '#telemetry';
            switchView('telemetry');
        });
    }

    if (memoryBtn) {
        memoryBtn.addEventListener('click', () => {
            window.location.hash = '#memory';
            switchView('memory');
        });
    }

    window.addEventListener('hashchange', () => {
        if (window.location.hash === '#memory') {
            switchView('memory');
        } else {
            switchView('telemetry');
        }
    });

    // Check initial hash on load
    if (window.location.hash === '#memory') {
        switchView('memory');
    } else {
        switchView('telemetry');
    }
}

function setupMemoryControls() {
    const searchInput = document.getElementById('memory-search-input');
    const clearBtn = document.getElementById('memory-search-clear');
    const refreshBtn = document.getElementById('memory-refresh-btn');

    let debounceTimer = null;
    if (searchInput) {
        searchInput.addEventListener('input', (e) => {
            memoryState.searchQuery = e.target.value;
            if (clearBtn) clearBtn.style.display = memoryState.searchQuery ? 'block' : 'none';
            clearTimeout(debounceTimer);
            debounceTimer = setTimeout(applyFilters, 150);
        });
    }

    if (clearBtn) {
        clearBtn.addEventListener('click', () => {
            if (searchInput) searchInput.value = '';
            memoryState.searchQuery = '';
            clearBtn.style.display = 'none';
            applyFilters();
            if (searchInput) searchInput.focus();
        });
    }

    if (refreshBtn) {
        refreshBtn.addEventListener('click', fetchFacts);
    }

    window.addEventListener('keydown', (e) => {
        if (e.key === '/' && document.activeElement !== searchInput && window.location.hash === '#memory') {
            e.preventDefault();
            if (searchInput) searchInput.focus();
        }
        if (e.key === 'Escape' && document.activeElement === searchInput) {
            if (searchInput) searchInput.value = '';
            memoryState.searchQuery = '';
            if (clearBtn) clearBtn.style.display = 'none';
            applyFilters();
            if (searchInput) searchInput.blur();
        }
    });
}

// Initial bootstrap
setupTabs();
setupMemoryControls();
startLiveTimerLoop();
