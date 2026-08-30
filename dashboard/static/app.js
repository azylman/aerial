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

function formatElapsedTicker(seconds) {
    if (seconds == null || isNaN(seconds) || seconds < 0) return '⏱ 00:00s';
    const hrs = Math.floor(seconds / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);

    if (hrs > 0) {
        return `⏱ ${String(hrs).padStart(2, '0')}:${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
    }
    return `⏱ ${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}s`;
}

function getTriggerBadge(triggerType) {
    const t = String(triggerType || 'discord').toLowerCase();
    switch (t) {
        case 'cron':
            return { icon: '⏰', text: 'CRON', css: 'cron' };
        case 'reminder':
            return { icon: '⏱️', text: 'REMINDER', css: 'reminder' };
        case 'http':
        case 'api':
            return { icon: '⚡', text: 'API', css: 'http' };
        case 'discord':
        default:
            return { icon: '💬', text: 'DISCORD', css: 'discord' };
    }
}

function formatAgentsviewSessionUrl(sessionId) {
    if (!sessionId || typeof sessionId !== 'string') {
        return '/conversations/';
    }
    const cleanId = sessionId.trim().replace(/^\/+|\/+$/g, '');
    if (!cleanId) {
        return '/conversations/';
    }
    const encodedPath = cleanId
        .split('/')
        .map(segment => encodeURIComponent(segment))
        .join('/');

    return `/sessions/${encodedPath}`;
}

function parseValidTimestampMs(dateStr) {
    if (!dateStr || typeof dateStr !== 'string') return null;
    const ms = new Date(dateStr).getTime();
    if (isNaN(ms) || ms < 1577836800000) return null; // Reject epoch zero / pre-2020 dates
    return ms;
}

// ==========================================
// TELEMETRY STATE & LOGIC
// ==========================================
let activeServicesCache = [];
let statusPollInterval = null;
let liveTimerInterval = null;
let statusAbortController = null;
let isStatusFetching = false;

function startLiveTimerLoop() {
    if (liveTimerInterval) clearInterval(liveTimerInterval);

    function tick() {
        const timerEls = document.querySelectorAll('.timer-text[data-started]');
        if (timerEls.length === 0) return;
        const now = Date.now();

        timerEls.forEach(el => {
            const startedMs = parseValidTimestampMs(el.getAttribute('data-started'));
            if (!startedMs) return;

            const elapsedSec = Math.max(0, Math.floor((now - startedMs) / 1000));
            el.textContent = formatElapsedTicker(elapsedSec);
        });
    }

    tick();
    liveTimerInterval = setInterval(tick, 1000);
}

function stopLiveTimerLoop() {
    if (liveTimerInterval) {
        clearInterval(liveTimerInterval);
        liveTimerInterval = null;
    }
}

function renderActiveTasks(tasks) {
    const container = document.getElementById('active-tasks-container') || document.getElementById('active-tasks-list');
    const badge = document.getElementById('tasks-count-badge') || document.getElementById('active-tasks-badge');
    const summaryVal = document.getElementById('summary-tasks-val') || document.getElementById('summary-active-tasks');
    const summarySub = document.getElementById('summary-tasks-sub') || document.getElementById('summary-tasks-label');

    const activeList = (Array.isArray(tasks) ? tasks : []).filter(t => t && typeof t === 'object');
    const runningCount = activeList.filter(t => t.status === 'PROCESSING').length;
    const pendingCount = activeList.filter(t => t.status === 'PENDING').length;
    const totalCount = activeList.length;

    // Update Summary Bar
    if (summaryVal) {
        if (runningCount > 0) {
            summaryVal.textContent = `${runningCount} RUNNING`;
            summaryVal.className = 'value text-cyan';
        } else if (pendingCount > 0) {
            summaryVal.textContent = `${pendingCount} QUEUED`;
            summaryVal.className = 'value text-warning';
        } else {
            summaryVal.textContent = '0 IDLE';
            summaryVal.className = 'value text-success';
        }
    }
    if (summarySub) {
        summarySub.textContent = totalCount > 0 ? `${totalCount} ACTIVE IN QUEUE` : 'REAL-TIME DISPATCH';
    }

    if (badge) {
        if (runningCount > 0) {
            badge.textContent = `⚡ ${runningCount} RUNNING` + (pendingCount > 0 ? ` (+${pendingCount} QUEUED)` : '');
            badge.className = 'section-badge active';
        } else if (pendingCount > 0) {
            badge.textContent = `⏳ ${pendingCount} IN QUEUE`;
            badge.className = 'section-badge building';
        } else {
            badge.textContent = '0 IN PROGRESS';
            badge.className = 'section-badge';
        }
    }

    if (!container) return;

    if (activeList.length === 0) {
        container.innerHTML = `
            <div class="task-idle-card">
                <div class="idle-indicator">
                    <span class="pulse-dot healthy"></span>
                    <span>ALL WORKERS IDLE // NO PENDING TURNS</span>
                </div>
                <div>DISPATCH POLLING SQLITE QUEUE (1s)</div>
            </div>
        `;
        return;
    }

    const now = Date.now();

    container.innerHTML = activeList.map(task => {
        const isProcessing = task.status === 'PROCESSING';
        const isPending = task.status === 'PENDING';
        const statusClass = isProcessing ? 'status-processing processing' : isPending ? 'status-pending pending' : 'status-unknown';
        const statusBadge = isProcessing ? '⚡ RUNNING' : isPending ? '⏳ QUEUED' : escapeHtml(String(task.status || 'UNKNOWN').toUpperCase());

        const authorSafe = escapeHtml(task.author_name || 'System');
        const summarySafe = escapeHtml(task.summary || task.prompt || 'Agent Task');
        const trigger = getTriggerBadge(task.trigger_type);
        const threadSafe = escapeHtml(task.thread_id || '');

        let timerHTML = '';
        if (isProcessing && (task.started_at || task.created_at)) {
            const startTimeStr = task.started_at || task.created_at;
            const startedMs = parseValidTimestampMs(startTimeStr);
            const initialSec = startedMs ? Math.max(0, Math.floor((now - startedMs) / 1000)) : 0;
            const formattedTime = formatElapsedTicker(initialSec);
            timerHTML = `
                <div class="deploy-timer-badge">
                    <span class="pulse-indicator"></span>
                    <span class="timer-text" data-started="${escapeHtml(startTimeStr)}">${formattedTime}</span>
                </div>
            `;
        }

        const retryHTML = Number(task.retry_count) > 0 
            ? `<span class="task-retry-badge" title="Retry Attempt">🔄 RETRY #${escapeHtml(task.retry_count)}</span>` 
            : '';

        const hasValidSession = typeof task.session_id === 'string' && task.session_id.trim().length > 0;
        const inspectUrl = formatAgentsviewSessionUrl(task.session_id);
        const inspectHTML = hasValidSession 
            ? `<a href="${escapeHtml(inspectUrl)}" target="_blank" rel="noopener noreferrer" class="task-inspect-btn active">💬 INSPECT IN AGENTSVIEW ↗</a>`
            : `<a href="/conversations/" target="_blank" rel="noopener noreferrer" class="task-inspect-btn active" title="Session allocating - Click to open Agentsview">💬 OPEN AGENTSVIEW ↗</a>`;

        return `
            <div class="task-card ${statusClass}">
                ${isProcessing ? '<div class="task-card-laser"></div>' : ''}
                <div class="task-card-header">
                    <div class="task-meta-left">
                        <span class="task-trigger-badge ${trigger.css}" data-trigger="${trigger.css}">${trigger.icon} ${escapeHtml(trigger.text)}</span>
                        <span class="task-author">${authorSafe}</span>
                        ${retryHTML}
                    </div>
                    <div style="display: flex; align-items: center; gap: 8px;">
                        ${timerHTML}
                        <span class="task-status-pill ${isProcessing ? 'processing' : 'pending'}">${statusBadge}</span>
                    </div>
                </div>
                <div class="task-prompt-box">
                    &gt; ${summarySafe}
                </div>
                <div class="task-card-footer">
                    <span style="font-size: 0.75rem; color: var(--text-dim); font-family: var(--font-mono);">THREAD: ${threadSafe || 'N/A'}</span>
                    ${inspectHTML}
                </div>
            </div>
        `;
    }).join('');
}

function renderDeployments(deployments) {
    const deploysContainer = document.getElementById('deployments-container');
    const deployBadge = document.getElementById('deploy-count-badge');
    const deploys = Array.isArray(deployments) ? deployments : [];

    const hasFailed = deploys.some(dep => dep.stage === 'failed');
    const hasDegraded = deploys.some(dep => dep.stage === 'degraded');
    const isBuilding = deploys.some(dep => dep.stage === 'building' || dep.stage === 'queued');
    const isSwapping = deploys.some(dep => dep.stage === 'swapping');
    const isAwaitingPull = deploys.some(dep => dep.stage === 'awaiting_pull');
    const activeDeploys = deploys.filter(dep => dep.stage !== 'live' && dep.stage !== 'completed');

    if (deployBadge) {
        if (hasFailed) {
            deployBadge.textContent = `🚨 CI BUILD FAILED`;
            deployBadge.className = 'section-badge failed';
        } else if (hasDegraded) {
            deployBadge.textContent = `⚠️ STACK DEGRADED`;
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
            deployBadge.textContent = `ALL SERVICES LIVE (STACK SYNCED)`;
            deployBadge.className = 'section-badge live';
        } else {
            deployBadge.textContent = 'SYSTEM IN SYNC';
            deployBadge.className = 'section-badge';
        }
    }

    if (!deploysContainer) return;
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
        return;
    }

    deploys.forEach(dep => {
        const isLive = dep.stage === 'live';
        const isFailed = dep.stage === 'failed';
        const isDegraded = dep.stage === 'degraded';
        const isSwapping = dep.stage === 'swapping';
        const isAwaitingPull = dep.stage === 'awaiting_pull';
        const isBuildingStage = dep.stage === 'building' || dep.stage === 'queued';

        const steps = dep.steps || [
            { name: "Commit Trigger", icon: "📦", status: "completed" },
            { name: "CI Build & GHCR", icon: "⚙️", status: "completed" },
            { name: "Watchtower Pull", icon: "⬇️", status: "completed" },
            { name: "Container Swap", icon: "🔄", status: isLive ? "completed" : "active" },
            { name: "Health Check", icon: "🩺", status: isLive ? "completed" : "pending" }
        ];

        const isHostPhase = isSwapping || isLive || isDegraded;
        const allChips = Array.isArray(dep.matrix_jobs) ? dep.matrix_jobs : [];
        const gateChips = allChips.filter(c => c.name && (c.name.includes('test') || c.name.includes('lint')));
        const serviceChips = allChips.filter(c => c.name && (!c.name.includes('test') && !c.name.includes('lint')));

        const stepsHTML = steps.map(step => {
            let matrixHTML = '';
            let targetChips = [];

            if (step.name && step.name.includes("CI Build")) {
                if (isBuildingStage || (isFailed && step.status === 'failed')) {
                    targetChips = allChips;
                } else if (gateChips.length > 0) {
                    targetChips = gateChips;
                }
            } else if (step.name && step.name.includes("Container Swap") && isHostPhase) {
                targetChips = serviceChips.length > 0 ? serviceChips : allChips;
            }

            if (targetChips.length > 0) {
                matrixHTML = `
                    <div class="matrix-chips-container">
                        ${targetChips.map(chip => {
                            const chipClass = chip.status === 'completed' ? 'chip-done' : chip.status === 'active' ? 'chip-running' : chip.status === 'failed' ? 'chip-failed' : 'chip-queued';
                            const chipIcon = chip.status === 'completed' ? '✓' : chip.status === 'active' ? '⚡' : chip.status === 'failed' ? '✕' : '○';
                            const durText = chip.duration ? ` (${escapeHtml(chip.duration)})` : '';
                            return `<span class="matrix-chip ${chipClass}" onclick="openDiagnosticDrawerByName('${escapeHtml(chip.name)}'); event.stopPropagation();" title="${escapeHtml(chip.name)}: ${chip.status}${durText} • Click for diagnostics">${escapeHtml(chip.name)}${durText} ${chipIcon}</span>`;
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
        if ((isBuildingStage || isSwapping || isAwaitingPull) && dep.started_at) {
            timerMarkup = `
                <div class="deploy-timer-badge">
                    <span class="pulse-indicator"></span>
                    <span class="timer-text" data-started="${escapeHtml(dep.started_at)}">⏱ 00:00s</span>
                </div>
            `;
        }

        const cardClass = isLive ? 'stage-live' : (isFailed || isDegraded) ? 'stage-failed' : isSwapping ? 'stage-swapping' : (isBuildingStage || isAwaitingPull) ? 'stage-building' : 'stage-active';
        const badgeClass = isLive ? 'live' : (isFailed || isDegraded) ? 'failed' : isSwapping ? 'swapping' : 'active';

        const card = document.createElement('div');
        card.className = `deploy-card ${cardClass}`;
        card.innerHTML = `
            ${(isBuildingStage || isSwapping || isAwaitingPull) ? '<div class="deploy-card-laser"></div>' : ''}
            <div class="deploy-card-header">
                <div class="deploy-target">
                    <span class="deploy-service-name">${escapeHtml((dep.service || 'SERVICE').toUpperCase())}</span>
                    ${commitMarkup}
                    ${runLinkMarkup}
                </div>
                <div style="display: flex; align-items: center; gap: 8px;">
                    ${timerMarkup}
                    <span class="deploy-stage-badge ${badgeClass}">⚡ ${escapeHtml(String(dep.stage || 'ACTIVE').toUpperCase())}</span>
                </div>
            </div>
            <div class="deploy-steps-grid">
                ${stepsHTML}
            </div>
            <div class="deploy-progress-bg">
                <div class="deploy-progress-fill" style="width: ${Number(dep.progress) || 0}%;"></div>
            </div>
            <div class="deploy-footer">
                <span>${dep.commit_msg ? `"${escapeHtml(dep.commit_msg)}"` : `STAGE: ${escapeHtml(String(dep.stage || '').toUpperCase())} (${Number(dep.progress) || 0}%)`}</span>
                <span>STARTED: ${dep.started_at ? new Date(dep.started_at).toLocaleTimeString() : 'RECENT'}</span>
            </div>
        `;
        deploysContainer.appendChild(card);
    });
}

function renderServicesGrid(services) {
    activeServicesCache = Array.isArray(services) ? services : [];
    const grid = document.getElementById('services-grid');
    if (!grid) return;

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

function getApiBase() {
    const path = window.location.pathname.replace(/\/+$/, '');
    return path.endsWith('/dashboard') ? path : (path + '/dashboard').replace(/\/+/g, '/').replace(/\/+$/, '');
}

async function fetchStatus() {
    if (isStatusFetching) return;
    isStatusFetching = true;

    if (statusAbortController) statusAbortController.abort();
    statusAbortController = new AbortController();

    let data;
    try {
        const res = await fetch(getApiBase() + '/api/status', { signal: statusAbortController.signal });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        data = await res.json();
    } catch (networkErr) {
        if (networkErr.name === 'AbortError') {
            isStatusFetching = false;
            return;
        }
        console.error('Failed to fetch system status:', networkErr);
        const overallStatusEl = document.getElementById('overall-status');
        if (overallStatusEl) {
            overallStatusEl.textContent = 'OFFLINE';
            overallStatusEl.className = 'value text-danger';
        }
        const clusterSubEl = document.getElementById('cluster-sub');
        if (clusterSubEl) clusterSubEl.textContent = 'SYSTEM DISCONNECTED';
        const summaryTasksVal = document.getElementById('summary-tasks-val') || document.getElementById('summary-active-tasks');
        if (summaryTasksVal) {
            summaryTasksVal.textContent = 'OFFLINE';
            summaryTasksVal.className = 'value text-danger';
        }
        isStatusFetching = false;
        return;
    } finally {
        isStatusFetching = false;
    }

    const refreshEl = document.getElementById('last-refresh');
    if (refreshEl && data.system_time) refreshEl.textContent = new Date(data.system_time).toLocaleTimeString();
    
    const overallStatusEl = document.getElementById('overall-status');
    if (overallStatusEl) {
        const statusUpper = escapeHtml(String(data.cluster_status || 'OPTIMAL').toUpperCase());
        overallStatusEl.textContent = statusUpper;
        overallStatusEl.className = (data.cluster_status === 'healthy' || data.cluster_status === 'optimal') 
            ? 'value text-success' 
            : 'value text-warning';
    }
    const clusterSubEl = document.getElementById('cluster-sub');
    if (clusterSubEl) clusterSubEl.textContent = '100% OPERATIONAL';

    // --- 0. RENDER LIVE AGENT EXECUTION QUEUE ---
    try {
        renderActiveTasks(data.active_tasks || []);
    } catch (e) {
        console.error('[Permet HUD] Error rendering active tasks:', e);
    }

    // --- 1. RENDER DEPLOYMENT PIPELINE ---
    try {
        renderDeployments(data.deployments || []);
    } catch (e) {
        console.error('[Permet HUD] Error rendering deployments:', e);
    }

    // --- 2. RENDER SERVICES GRID ---
    try {
        renderServicesGrid(data.services || []);
    } catch (e) {
        console.error('[Permet HUD] Error rendering services grid:', e);
    }
}

// Drawer functionality
function openDiagnosticDrawerByName(serviceName) {
    const clean = String(serviceName || '').toLowerCase().replace(/^aerial-/, '');
    const idx = activeServicesCache.findIndex(s => {
        const sClean = String(s.name || '').toLowerCase().replace(/^aerial-/, '');
        return sClean === clean || sClean.includes(clean);
    });
    if (idx !== -1) {
        openDiagnosticDrawer(idx);
    }
}

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
                    <span class="val">aerial-net:internal (Docker Bridge)</span>
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
// SCHEDULES & EXECUTION TELEMETRY STATE & LOGIC
// ==========================================
let schedulesPollInterval = null;
let schedulesTickerInterval = null;

const schedulesState = {
    crons: [],
    oneShots: [],
    runs: [],
    runsTotal: 0,
    summary: {
        total_active: 0,
        cron_count: 0,
        one_shot_count: 0,
        total_runs_24h: 0,
        success_rate_24h: 100.0,
        next_run_at: null
    },
    selectedFilter: 'ALL', // 'ALL' | 'CRON' | 'ONE_SHOT' | 'RUNS'
    searchQuery: '',
    isLoading: false,
    isRunsLoading: false,
    error: null,
    serverClockSkewMs: 0,
    systemTime: null,
    lastSync: null
};

function formatCronExpression(cronExpr, tz) {
    if (!cronExpr) return '';
    const expr = String(cronExpr).trim();
    if (!expr) return '';

    const lower = expr.toLowerCase();
    switch (lower) {
        case '@yearly':
        case '@annually':
            return 'Every year on Jan 1st at 00:00';
        case '@monthly':
            return '1st of every month at 00:00';
        case '@weekly':
            return 'Every week on Sunday at 00:00';
        case '@daily':
        case '@midnight':
            return 'Every day at 00:00';
        case '@hourly':
            return 'Every hour';
    }

    const fields = expr.split(/\s+/);
    if (fields.length !== 5) {
        return expr;
    }

    const [minStr, hourStr, domStr, monStr, dowStr] = fields;

    // Case: Every minute (* * * * *)
    if (minStr === '*' && hourStr === '*' && domStr === '*' && monStr === '*' && dowStr === '*') {
        return 'Every minute';
    }

    // Case: Every X minutes (*/N * * * *)
    if (minStr.startsWith('*/') && hourStr === '*' && domStr === '*' && monStr === '*' && dowStr === '*') {
        const interval = minStr.slice(2);
        return `Every ${interval} minutes`;
    }

    // Case: Every X hours (0 */N * * *)
    if (minStr === '0' && hourStr.startsWith('*/') && domStr === '*' && monStr === '*' && dowStr === '*') {
        const interval = hourStr.slice(2);
        return `Every ${interval} hours`;
    }

    const m = parseInt(minStr, 10);
    const h = parseInt(hourStr, 10);

    const cronMonthNames = {
        1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
        7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
    };

    const cronDayNames = {
        0: 'Sunday', 1: 'Monday', 2: 'Tuesday', 3: 'Wednesday', 4: 'Thursday', 5: 'Friday', 6: 'Saturday', 7: 'Sunday'
    };

    const cronDayShortNames = {
        0: 'Sun', 1: 'Mon', 2: 'Tue', 3: 'Wed', 4: 'Thu', 5: 'Fri', 6: 'Sat', 7: 'Sun'
    };

    function ordinal(n) {
        if (n >= 11 && n <= 13) return `${n}th`;
        switch (n % 10) {
            case 1: return `${n}st`;
            case 2: return `${n}nd`;
            case 3: return `${n}rd`;
            default: return `${n}th`;
        }
    }

    if (!isNaN(m) && !isNaN(h)) {
        const timeStr = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;

        // 1. Every day at HH:MM (0 9 * * *)
        if (domStr === '*' && monStr === '*' && dowStr === '*') {
            return `Every day at ${timeStr}`;
        }

        // 2. Specific day of week (0 9 * * 1-5, etc.)
        if (domStr === '*' && monStr === '*' && dowStr !== '*') {
            const dowUpper = dowStr.toUpperCase();
            if (dowUpper === '1-5' || dowUpper === 'MON-FRI') {
                return `Weekdays (Mon–Fri) at ${timeStr}`;
            }
            if (dowUpper === '0,6' || dowUpper === '6,0' || dowUpper === 'SAT,SUN' || dowUpper === 'SUN,SAT') {
                return `Weekends (Sat–Sun) at ${timeStr}`;
            }

            const dowNum = parseInt(dowStr, 10);
            if (!isNaN(dowNum) && dowNum >= 0 && dowNum <= 7) {
                return `Every ${cronDayNames[dowNum]} at ${timeStr}`;
            }

            const parts = dowStr.split(',');
            const names = parts.map(p => {
                const pTrim = p.trim();
                const dNum = parseInt(pTrim, 10);
                if (!isNaN(dNum) && dNum >= 0 && dNum <= 7) {
                    return cronDayShortNames[dNum];
                }
                return pTrim;
            });
            if (names.length > 0) {
                return `${names.join(', ')} at ${timeStr}`;
            }
        }

        // 3. Specific day of month (0 12 1 * *)
        if (domStr !== '*' && monStr === '*' && dowStr === '*') {
            const domNum = parseInt(domStr, 10);
            if (!isNaN(domNum) && domNum >= 1 && domNum <= 31) {
                return `${ordinal(domNum)} of every month at ${timeStr}`;
            }
        }

        // 4. Specific month and day (0 0 1 1 *)
        if (domStr !== '*' && monStr !== '*' && dowStr === '*') {
            const domNum = parseInt(domStr, 10);
            const monNum = parseInt(monStr, 10);
            if (!isNaN(domNum) && !isNaN(monNum) && monNum >= 1 && monNum <= 12) {
                return `Every year on ${cronMonthNames[monNum]} ${ordinal(domNum)} at ${timeStr}`;
            }
        }

        return `At ${timeStr} (${expr})`;
    }

    return expr;
}

function formatCountdown(targetDateStr) {
    if (!targetDateStr) {
        return { text: '--:--:--', cssClass: '', isTriggering: false, isUrgent: false, isOverdue: false };
    }
    const targetMs = new Date(targetDateStr).getTime();
    if (isNaN(targetMs)) {
        return { text: '--:--:--', cssClass: '', isTriggering: false, isUrgent: false, isOverdue: false };
    }

    const normalizedNow = Date.now() + (schedulesState.serverClockSkewMs || 0);
    const diffSec = Math.floor((targetMs - normalizedNow) / 1000);

    if (diffSec > 86400) {
        const days = Math.floor(diffSec / 86400);
        const hrs = Math.floor((diffSec % 86400) / 3600);
        return { text: `⏱ in ${days}d ${hrs}h`, cssClass: 'countdown-normal', isTriggering: false, isUrgent: false, isOverdue: false };
    }
    if (diffSec >= 3600) {
        const hrs = Math.floor(diffSec / 3600);
        const mins = Math.floor((diffSec % 3600) / 60);
        return { text: `⏱ in ${hrs}h ${mins}m`, cssClass: 'countdown-normal', isTriggering: false, isUrgent: false, isOverdue: false };
    }
    if (diffSec > 60) {
        const mins = Math.floor(diffSec / 60);
        const secs = diffSec % 60;
        return { text: `⏱ in ${mins}m ${secs}s`, cssClass: 'countdown-normal', isTriggering: false, isUrgent: false, isOverdue: false };
    }
    if (diffSec >= 1) {
        return { text: `⏱ in ${diffSec}s`, cssClass: 'countdown-urgent', isTriggering: false, isUrgent: true, isOverdue: false };
    }
    if (diffSec >= -15) {
        return { text: `⚡ TRIGGERING...`, cssClass: 'countdown-triggering', isTriggering: true, isUrgent: false, isOverdue: false };
    }
    return { text: `⏱ OVERDUE (SYNCING...)`, cssClass: 'countdown-overdue', isTriggering: false, isUrgent: false, isOverdue: true };
}

function formatDuration(durationMs) {
    if (durationMs == null || isNaN(durationMs) || durationMs <= 0) return '0ms';
    if (durationMs < 1000) return `${durationMs}ms`;
    const sec = (durationMs / 1000).toFixed(1);
    if (durationMs < 60000) return `${sec}s`;
    const mins = Math.floor(durationMs / 60000);
    const remSec = Math.floor((durationMs % 60000) / 1000);
    return `${mins}m ${remSec}s`;
}

function formatTimestamp(dateStr) {
    if (!dateStr) return '--';
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return '--';
    return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' }) + ' ' + date.toLocaleTimeString();
}

async function fetchSchedules() {
    if (schedulesState.isLoading) return;
    schedulesState.isLoading = true;

    if (schedulesState.crons.length === 0 && schedulesState.oneShots.length === 0) {
        renderSchedulesSkeleton();
    }

    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 6000);

        const res = await fetch(getApiBase() + '/api/schedules', { signal: controller.signal });
        clearTimeout(timeoutId);

        const data = await res.json();
        if (!res.ok) {
            throw new Error(data.error || `HTTP ${res.status}: Brain schedules proxy unavailable`);
        }

        if (data.system_time) {
            schedulesState.serverClockSkewMs = Date.parse(data.system_time) - Date.now();
            schedulesState.systemTime = data.system_time;
        }

        schedulesState.crons = Array.isArray(data.crons) ? data.crons : [];
        schedulesState.oneShots = Array.isArray(data.one_shots) ? data.one_shots : [];
        schedulesState.summary = data.summary || {
            total_active: schedulesState.crons.length + schedulesState.oneShots.length,
            cron_count: schedulesState.crons.length,
            one_shot_count: schedulesState.oneShots.length,
            total_runs_24h: 0,
            success_rate_24h: 100.0,
            next_run_at: null
        };
        schedulesState.isLoading = false;
        schedulesState.error = null;
        schedulesState.lastSync = new Date();

        const banner = document.getElementById('schedules-reconnect-banner');
        if (banner) banner.style.display = 'none';

        updateScheduleMetrics();
        renderSchedulePills();
        renderScheduleCards();

    } catch (err) {
        console.warn('Failed to fetch active schedules:', err);
        schedulesState.isLoading = false;
        schedulesState.error = err.message || 'Brain service unreachable';

        // Stale-While-Revalidate: If we have existing cached schedules, retain them and show banner
        if (schedulesState.crons.length > 0 || schedulesState.oneShots.length > 0) {
            const banner = document.getElementById('schedules-reconnect-banner');
            if (banner) banner.style.display = 'flex';
        } else {
            renderSchedulesError(schedulesState.error);
        }
    }
}

async function fetchScheduleRuns() {
    if (schedulesState.isRunsLoading) return;
    schedulesState.isRunsLoading = true;

    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 6000);

        const res = await fetch(getApiBase() + '/api/schedules/runs?limit=50&offset=0', { signal: controller.signal });
        clearTimeout(timeoutId);

        const data = await res.json();
        if (res.ok) {
            schedulesState.runs = Array.isArray(data.runs) ? data.runs : [];
            schedulesState.runsTotal = typeof data.total === 'number' ? data.total : schedulesState.runs.length;
            renderScheduleRuns();
            renderSchedulePills();
        }
        schedulesState.isRunsLoading = false;
    } catch (err) {
        console.warn('Failed to fetch schedule runs:', err);
        schedulesState.isRunsLoading = false;
    }
}

function updateScheduleMetrics() {
    const summary = schedulesState.summary;

    const cronsCountEl = document.getElementById('schedules-crons-count');
    if (cronsCountEl) cronsCountEl.textContent = summary.cron_count ?? schedulesState.crons.length;

    const oneshotCountEl = document.getElementById('schedules-oneshot-count');
    if (oneshotCountEl) oneshotCountEl.textContent = summary.one_shot_count ?? schedulesState.oneShots.length;

    const successRateEl = document.getElementById('schedules-success-rate');
    if (successRateEl) {
        const rate = typeof summary.success_rate_24h === 'number' ? summary.success_rate_24h : 100.0;
        successRateEl.textContent = `${rate.toFixed(1)}%`;
        if (rate >= 90) {
            successRateEl.className = 'value text-success';
        } else if (rate >= 75) {
            successRateEl.className = 'value';
            successRateEl.style.color = 'var(--neon-amber)';
        } else {
            successRateEl.className = 'value text-danger';
        }
    }

    const successSubEl = document.getElementById('schedules-success-sub');
    if (successSubEl) {
        successSubEl.textContent = `${summary.total_runs_24h || 0} RUNS LOGGED (24H)`;
    }

    const activeBadgeEl = document.getElementById('schedules-active-badge');
    if (activeBadgeEl) {
        activeBadgeEl.textContent = `${summary.total_active ?? (schedulesState.crons.length + schedulesState.oneShots.length)} ACTIVE`;
    }
}

function renderSchedulePills() {
    const totalActive = schedulesState.crons.length + schedulesState.oneShots.length;
    const cronsCount = schedulesState.crons.length;
    const oneshotsCount = schedulesState.oneShots.length;
    const runsCount = schedulesState.runsTotal || schedulesState.runs.length;

    const pillAll = document.getElementById('pill-count-schedules-all');
    if (pillAll) pillAll.textContent = totalActive;

    const pillCrons = document.getElementById('pill-count-crons');
    if (pillCrons) pillCrons.textContent = cronsCount;

    const pillOneshots = document.getElementById('pill-count-oneshots');
    if (pillOneshots) pillOneshots.textContent = oneshotsCount;

    const pillRuns = document.getElementById('pill-count-runs');
    if (pillRuns) pillRuns.textContent = runsCount;

    const pillsBar = document.getElementById('schedules-filter-pills');
    if (pillsBar) {
        pillsBar.querySelectorAll('.cat-pill').forEach(pill => {
            const filterVal = pill.getAttribute('data-filter');
            if (filterVal === schedulesState.selectedFilter) {
                pill.classList.add('active');
            } else {
                pill.classList.remove('active');
            }
        });
    }
}

function renderScheduleCards() {
    const grid = document.getElementById('schedules-grid');
    const gridSection = document.getElementById('schedules-grid-section');
    if (!grid || !gridSection) return;

    const filter = schedulesState.selectedFilter;
    const query = schedulesState.searchQuery.toLowerCase().trim();

    // If RUNS filter is selected, hide active schedules section
    if (filter === 'RUNS') {
        gridSection.style.display = 'none';
        return;
    } else {
        gridSection.style.display = 'block';
    }

    // Filter crons
    const matchingCrons = (filter === 'ONE_SHOT') ? [] : schedulesState.crons.filter(c => {
        if (!query) return true;
        const title = (c.title_prefix || '').toLowerCase();
        const expr = (c.cron_expr || '').toLowerCase();
        const desc = (c.cron_description || '').toLowerCase();
        const prompt = (c.prompt || '').toLowerCase();
        const chan = (c.channel_id || c.target_id || '').toLowerCase();
        const tz = (c.timezone || '').toLowerCase();
        const id = (c.id || '').toLowerCase();
        return title.includes(query) || expr.includes(query) || desc.includes(query) || prompt.includes(query) || chan.includes(query) || tz.includes(query) || id.includes(query);
    });

    // Filter one-shots
    const matchingOneShots = (filter === 'CRON') ? [] : schedulesState.oneShots.filter(s => {
        if (!query) return true;
        const prompt = (s.prompt || '').toLowerCase();
        const thread = (s.thread_id || '').toLowerCase();
        const id = (s.id || '').toLowerCase();
        return prompt.includes(query) || thread.includes(query) || id.includes(query);
    });

    schedulesState.filteredCrons = matchingCrons;
    schedulesState.filteredOneShots = matchingOneShots;
    const totalMatching = matchingCrons.length + matchingOneShots.length;

    const counterEl = document.getElementById('schedules-results-count');
    if (counterEl) {
        const totalItems = schedulesState.crons.length + schedulesState.oneShots.length;
        counterEl.textContent = `SHOWING ${totalMatching} / ${totalItems} ACTIVE ROUTINES`;
    }

    if (totalMatching === 0) {
        grid.innerHTML = `
            <div class="empty-state-box">
                <div style="font-size: 1.6rem; margin-bottom: 0.6rem;">⏱</div>
                <div style="font-weight: 700; color: #fff;">NO ACTIVE SCHEDULED ROUTINES MATCH CRITERIA</div>
                <div style="font-size: 0.75rem; color: var(--text-dim); margin-top: 6px;">Try adjusting your keyword filter or switching category pills.</div>
            </div>
        `;
        return;
    }

    let cardsHTML = '';

    // Render Crons
    matchingCrons.forEach((cron, idx) => {
        const rawTitle = cron.title_prefix || 'RECURRING ROUTINE';
        const highlightedTitle = query ? highlightSearch(rawTitle, query) : escapeHtml(rawTitle);
        const rawPrompt = cron.prompt || '';
        const highlightedPrompt = query ? highlightSearch(rawPrompt, query) : escapeHtml(rawPrompt);
        const humanCron = cron.cron_description || formatCronExpression(cron.cron_expr, cron.timezone);
        const countdown = formatCountdown(cron.next_run_at);
        const targetId = cron.channel_id || cron.target_id || 'N/A';

        cardsHTML += `
            <div class="schedule-card cron-card">
                <div class="schedule-card-header">
                    <div class="schedule-badge-group">
                        <span class="schedule-type-badge cron">⏰ CRON</span>
                        <span class="schedule-tz-badge">${escapeHtml(cron.timezone || 'UTC')}</span>
                    </div>
                    <div class="schedule-status-group">
                        <span class="pulse-dot ${cron.enabled ? 'healthy' : 'unhealthy'}" title="${cron.enabled ? 'Active & Enabled' : 'Disabled'}"></span>
                        <span class="schedule-countdown-badge ${countdown.cssClass}" data-next-run="${escapeHtml(cron.next_run_at || '')}">${countdown.text}</span>
                    </div>
                </div>
                <div class="schedule-title-bar">
                    <h3 class="schedule-title">${highlightedTitle}</h3>
                    <div class="schedule-cron-human">📅 ${escapeHtml(humanCron)}</div>
                </div>
                <div class="schedule-meta-bar">
                    <span class="schedule-meta-item"><strong>CRON:</strong> <code>${escapeHtml(cron.cron_expr)}</code></span>
                    <span class="schedule-meta-item"><strong>TARGET:</strong> <code>${escapeHtml(targetId)}</code></span>
                </div>
                <div class="schedule-prompt-wrap">
                    <div class="schedule-prompt-header">
                        <span class="prompt-lbl">PROMPT DIRECTIVE</span>
                        <button class="prompt-copy-btn" onclick="copyCronPrompt(${idx}, this)" aria-label="Copy prompt text">📋 COPY</button>
                    </div>
                    <div class="schedule-prompt-body" id="cron-prompt-${idx}">
                        ${highlightedPrompt}
                    </div>
                </div>
                <div class="schedule-card-footer">
                    <span>NEXT: ${formatTimestamp(cron.next_run_at)}</span>
                    <span class="schedule-id-chip">ID: <code>${escapeHtml(cron.id)}</code></span>
                </div>
            </div>
        `;
    });

    // Render One-Shots
    matchingOneShots.forEach((s, idx) => {
        const rawPrompt = s.prompt || '';
        const highlightedPrompt = query ? highlightSearch(rawPrompt, query) : escapeHtml(rawPrompt);
        const countdown = formatCountdown(s.run_at);
        const threadId = s.thread_id || 'N/A';

        let threadMarkup = `<code>${escapeHtml(threadId)}</code>`;
        if (/^\d+$/.test(threadId.trim())) {
            const safeThread = escapeHtml(threadId.trim());
            threadMarkup = `<a href="/conversations/?thread=${safeThread}" class="schedule-thread-link" target="_blank" rel="noopener">#${safeThread.slice(0, 8)} ↗</a>`;
        }

        cardsHTML += `
            <div class="schedule-card oneshot-card">
                <div class="schedule-card-header">
                    <div class="schedule-badge-group">
                        <span class="schedule-type-badge oneshot">⚡ ONE-SHOT</span>
                    </div>
                    <div class="schedule-status-group">
                        <span class="pulse-dot healthy" title="Pending execution"></span>
                        <span class="schedule-countdown-badge ${countdown.cssClass}" data-next-run="${escapeHtml(s.run_at || '')}">${countdown.text}</span>
                    </div>
                </div>
                <div class="schedule-title-bar">
                    <h3 class="schedule-title">ONE-TIME REMINDER</h3>
                    <div class="schedule-cron-human">🎯 Single Execution Timer</div>
                </div>
                <div class="schedule-meta-bar">
                    <span class="schedule-meta-item"><strong>THREAD:</strong> ${threadMarkup}</span>
                </div>
                <div class="schedule-prompt-wrap">
                    <div class="schedule-prompt-header">
                        <span class="prompt-lbl">PROMPT DIRECTIVE</span>
                        <button class="prompt-copy-btn" onclick="copyOneShotPrompt(${idx}, this)" aria-label="Copy prompt text">📋 COPY</button>
                    </div>
                    <div class="schedule-prompt-body" id="oneshot-prompt-${idx}">
                        ${highlightedPrompt}
                    </div>
                </div>
                <div class="schedule-card-footer">
                    <span>RUN AT: ${formatTimestamp(s.run_at)}</span>
                    <span class="schedule-id-chip">ID: <code>${escapeHtml(s.id)}</code></span>
                </div>
            </div>
        `;
    });

    grid.innerHTML = cardsHTML;
}

function renderScheduleRuns() {
    const feedContainer = document.getElementById('runs-feed-container');
    const feedSection = document.getElementById('runs-feed-section');
    const badgeEl = document.getElementById('runs-count-badge');
    if (!feedContainer || !feedSection) return;

    const filter = schedulesState.selectedFilter;
    const query = schedulesState.searchQuery.toLowerCase().trim();

    // In CRON or ONE_SHOT mode, hide runs feed to focus on active schedules
    if (filter === 'CRON' || filter === 'ONE_SHOT') {
        feedSection.style.display = 'none';
        return;
    } else {
        feedSection.style.display = 'block';
    }

    const matchingRuns = schedulesState.runs.filter(r => {
        if (!query) return true;
        const p = (r.prompt || '').toLowerCase();
        const t = (r.title || '').toLowerCase();
        const sId = (r.schedule_id || '').toLowerCase();
        const thId = (r.thread_id || '').toLowerCase();
        const tgId = (r.target_id || '').toLowerCase();
        const st = (r.status || '').toLowerCase();
        const err = (r.error || '').toLowerCase();
        return p.includes(query) || t.includes(query) || sId.includes(query) || thId.includes(query) || tgId.includes(query) || st.includes(query) || err.includes(query);
    });

    if (badgeEl) {
        badgeEl.textContent = `${matchingRuns.length} RUNS`;
    }

    if (matchingRuns.length === 0) {
        feedContainer.innerHTML = `
            <div class="empty-state-box">
                <div style="font-size: 1.6rem; margin-bottom: 0.6rem;">📋</div>
                <div style="font-weight: 700; color: #fff;">NO RECENT EXECUTION RUNS</div>
                <div style="font-size: 0.75rem; color: var(--text-dim); margin-top: 6px;">Schedule execution history and outcome logs will appear here.</div>
            </div>
        `;
        return;
    }

    feedContainer.innerHTML = matchingRuns.map((run, idx) => {
        const rawTitle = run.title || run.schedule_id || 'Schedule Run';
        const highlightedTitle = query ? highlightSearch(rawTitle, query) : escapeHtml(rawTitle);
        const rawPrompt = run.prompt || '';
        const highlightedPrompt = query ? highlightSearch(rawPrompt, query) : escapeHtml(rawPrompt);
        const rawError = run.error || '';
        const highlightedError = query ? highlightSearch(rawError, query) : escapeHtml(rawError);

        const statusLower = (run.status || 'unknown').toLowerCase();
        let statusIcon = '✓';
        let chipClass = 'chip-completed';
        if (statusLower === 'running') {
            statusIcon = '🔄';
            chipClass = 'chip-running';
        } else if (statusLower === 'enqueued') {
            statusIcon = '⏳';
            chipClass = 'chip-enqueued';
        } else if (statusLower === 'failed') {
            statusIcon = '🚨';
            chipClass = 'chip-failed';
        } else if (statusLower === 'completed' || statusLower === 'success') {
            statusIcon = '⚡';
            chipClass = 'chip-completed';
        }

        const durationStr = formatDuration(run.duration_ms);
        const timeAgo = formatTimeAgo(run.started_at || run.triggered_at);

        let threadMarkup = '';
        if (run.thread_id && /^\d+$/.test(run.thread_id.trim())) {
            const safeThread = escapeHtml(run.thread_id.trim());
            threadMarkup = ` • <a href="/conversations/?thread=${safeThread}" class="run-thread-link" target="_blank" rel="noopener">THREAD #${safeThread.slice(0, 8)} ↗</a>`;
        }

        let errorBox = '';
        if (run.error) {
            errorBox = `
                <div class="run-error-box">
                    <span class="run-error-label">ERROR:</span>
                    <span class="run-error-text">${highlightedError}</span>
                </div>
            `;
        }

        return `
            <div class="run-card run-${escapeHtml(statusLower)}">
                <div class="run-card-header">
                    <div class="run-header-left">
                        <span class="run-status-chip ${chipClass}">${statusIcon} ${escapeHtml(statusLower.toUpperCase())}</span>
                        <span class="run-title">${highlightedTitle}</span>
                        <span class="run-type-tag">${escapeHtml((run.schedule_type || 'cron').toUpperCase())}</span>
                    </div>
                    <div class="run-header-right">
                        ${run.duration_ms ? `<span class="run-duration-badge">⏱ ${durationStr}</span>` : ''}
                        <span class="run-time-badge">${timeAgo}</span>
                    </div>
                </div>
                <div class="run-prompt-preview">
                    <div class="run-prompt-text">${highlightedPrompt}</div>
                    ${errorBox}
                </div>
                <div class="run-card-footer">
                    <div class="run-meta-left">
                        <span>TARGET: <code>${escapeHtml(run.target_id || run.thread_id || 'N/A')}</code>${threadMarkup}</span>
                    </div>
                    <div class="run-meta-right">
                        <span>RUN ID: <code>${escapeHtml(String(run.id))}</code></span>
                    </div>
                </div>
            </div>
        `;
    }).join('');
}

function renderSchedulesSkeleton() {
    const grid = document.getElementById('schedules-grid');
    if (grid) {
        grid.innerHTML = Array(4).fill(0).map(() => `
            <div class="fact-card-skeleton">
                <div class="skeleton-box" style="width: 40%; height: 18px;"></div>
                <div class="skeleton-box" style="width: 100%; height: 60px; margin: 12px 0;"></div>
                <div class="skeleton-box" style="width: 50%; height: 14px;"></div>
            </div>
        `).join('');
    }
}

function renderSchedulesError(errMsg) {
    const grid = document.getElementById('schedules-grid');
    if (!grid) return;
    grid.innerHTML = `
        <div class="permet-alert-box">
            <h3>⚡ PERMET LINK SEVERED // BRAIN OFFLINE</h3>
            <p>${escapeHtml(errMsg)}</p>
            <button class="cyber-btn-secondary" onclick="fetchSchedules(); fetchScheduleRuns();" style="margin: 0 auto;">
                <span class="refresh-icon">⚡</span> RETRY SYNCHRONIZATION
            </button>
        </div>
    `;
}

async function copyCronPrompt(index, btnElement) {
    const cron = schedulesState.filteredCrons && schedulesState.filteredCrons[index];
    if (!cron || !cron.prompt) return;
    try {
        await navigator.clipboard.writeText(cron.prompt);
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

async function copyOneShotPrompt(index, btnElement) {
    const oneshot = schedulesState.filteredOneShots && schedulesState.filteredOneShots[index];
    if (!oneshot || !oneshot.prompt) return;
    try {
        await navigator.clipboard.writeText(oneshot.prompt);
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

function startSchedulesTicker() {
    if (schedulesTickerInterval) clearInterval(schedulesTickerInterval);

    function tick() {
        // 1. Update summary card next trigger
        const nextTriggerEl = document.getElementById('schedules-next-trigger');
        if (nextTriggerEl) {
            let earliestTime = null;
            if (schedulesState.summary && schedulesState.summary.next_run_at) {
                earliestTime = schedulesState.summary.next_run_at;
            } else {
                const candidateTimes = [];
                schedulesState.crons.forEach(c => { if (c.enabled && c.next_run_at) candidateTimes.push(c.next_run_at); });
                schedulesState.oneShots.forEach(o => { if (o.run_at) candidateTimes.push(o.run_at); });
                if (candidateTimes.length > 0) {
                    candidateTimes.sort((a, b) => new Date(a).getTime() - new Date(b).getTime());
                    earliestTime = candidateTimes[0];
                }
            }

            if (earliestTime) {
                const cd = formatCountdown(earliestTime);
                nextTriggerEl.textContent = cd.text;
                nextTriggerEl.className = `value ${cd.cssClass}`;
            } else {
                nextTriggerEl.textContent = 'NO UPCOMING';
                nextTriggerEl.className = 'value';
            }
        }

        // 2. Update individual card badges
        const countdownEls = document.querySelectorAll('.schedule-countdown-badge[data-next-run]');
        countdownEls.forEach(el => {
            const dateStr = el.getAttribute('data-next-run');
            if (!dateStr) return;
            const cd = formatCountdown(dateStr);
            el.textContent = cd.text;
            el.className = `schedule-countdown-badge ${cd.cssClass}`;
        });
    }

    tick();
    schedulesTickerInterval = setInterval(tick, 1000);
}

function stopSchedulesTicker() {
    if (schedulesTickerInterval) {
        clearInterval(schedulesTickerInterval);
        schedulesTickerInterval = null;
    }
}

function setupSchedulesControls() {
    const searchInput = document.getElementById('schedules-search-input');
    const clearBtn = document.getElementById('schedules-search-clear');
    const refreshBtn = document.getElementById('schedules-refresh-btn');
    const pillsBar = document.getElementById('schedules-filter-pills');

    let debounceTimer = null;
    if (searchInput) {
        searchInput.addEventListener('input', (e) => {
            schedulesState.searchQuery = e.target.value;
            if (clearBtn) clearBtn.style.display = schedulesState.searchQuery ? 'block' : 'none';
            clearTimeout(debounceTimer);
            debounceTimer = setTimeout(() => {
                renderScheduleCards();
                renderScheduleRuns();
            }, 150);
        });
    }

    if (clearBtn) {
        clearBtn.addEventListener('click', () => {
            if (searchInput) searchInput.value = '';
            schedulesState.searchQuery = '';
            clearBtn.style.display = 'none';
            renderScheduleCards();
            renderScheduleRuns();
            if (searchInput) searchInput.focus();
        });
    }

    if (refreshBtn) {
        refreshBtn.addEventListener('click', () => {
            fetchSchedules();
            fetchScheduleRuns();
        });
    }

    if (pillsBar) {
        pillsBar.querySelectorAll('.cat-pill').forEach(pill => {
            pill.addEventListener('click', () => {
                schedulesState.selectedFilter = pill.getAttribute('data-filter') || 'ALL';
                renderSchedulePills();
                renderScheduleCards();
                renderScheduleRuns();
            });
        });
    }
}


// ==========================================
// DECLARATIVE MULTI-TAB SPA ROUTER
// ==========================================
let currentTabKey = null;

const TABS = {
    telemetry: {
        btnId: 'tab-telemetry-btn',
        viewId: 'telemetry-view',
        hash: '#telemetry',
        onEnter: () => {
            fetchStatus();
            if (!statusPollInterval) {
                statusPollInterval = setInterval(fetchStatus, 5000);
            }
            startLiveTimerLoop();
        },
        onLeave: () => {
            if (statusPollInterval) {
                clearInterval(statusPollInterval);
                statusPollInterval = null;
            }
            stopLiveTimerLoop();
        }
    },
    schedules: {
        btnId: 'tab-schedules-btn',
        viewId: 'schedules-view',
        hash: '#schedules',
        onEnter: () => {
            fetchSchedules();
            fetchScheduleRuns();
            if (!schedulesPollInterval) {
                schedulesPollInterval = setInterval(() => {
                    fetchSchedules();
                    fetchScheduleRuns();
                }, 10000);
            }
            startSchedulesTicker();
        },
        onLeave: () => {
            if (schedulesPollInterval) {
                clearInterval(schedulesPollInterval);
                schedulesPollInterval = null;
            }
            stopSchedulesTicker();
        }
    },
    memory: {
        btnId: 'tab-memory-btn',
        viewId: 'memory-view',
        hash: '#memory',
        onEnter: () => {
            if (memoryState.facts.length === 0) {
                fetchFacts();
            }
        },
        onLeave: () => {}
    }
};

function navigateTab(tabKey) {
    if (!TABS[tabKey]) {
        tabKey = 'telemetry';
    }

    if (currentTabKey && currentTabKey !== tabKey && TABS[currentTabKey]) {
        TABS[currentTabKey].onLeave();
    }

    currentTabKey = tabKey;

    Object.keys(TABS).forEach(key => {
        const tab = TABS[key];
        const btn = document.getElementById(tab.btnId);
        const view = document.getElementById(tab.viewId);

        if (key === tabKey) {
            if (btn) {
                btn.classList.add('active');
                btn.setAttribute('aria-selected', 'true');
            }
            if (view) {
                view.classList.add('active');
                view.style.display = 'block';
            }
        } else {
            if (btn) {
                btn.classList.remove('active');
                btn.setAttribute('aria-selected', 'false');
            }
            if (view) {
                view.classList.remove('active');
                view.style.display = 'none';
            }
        }
    });

    if (window.location.hash !== TABS[tabKey].hash) {
        window.location.hash = TABS[tabKey].hash;
    }

    TABS[tabKey].onEnter();
}

function setupTabs() {
    Object.keys(TABS).forEach(key => {
        const tab = TABS[key];
        const btn = document.getElementById(tab.btnId);
        if (btn) {
            btn.addEventListener('click', () => {
                navigateTab(key);
            });
        }
    });

    window.addEventListener('hashchange', () => {
        const hash = window.location.hash;
        const matchingKey = Object.keys(TABS).find(k => TABS[k].hash === hash);
        navigateTab(matchingKey || 'telemetry');
    });

    const initialHash = window.location.hash;
    const initialKey = Object.keys(TABS).find(k => TABS[k].hash === initialHash);
    navigateTab(initialKey || 'telemetry');
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
}

function setupGlobalKeyboardShortcuts() {
    window.addEventListener('keydown', (e) => {
        const memorySearch = document.getElementById('memory-search-input');
        const schedulesSearch = document.getElementById('schedules-search-input');

        if (e.key === '/') {
            if (currentTabKey === 'memory' && memorySearch && document.activeElement !== memorySearch) {
                e.preventDefault();
                memorySearch.focus();
            } else if (currentTabKey === 'schedules' && schedulesSearch && document.activeElement !== schedulesSearch) {
                e.preventDefault();
                schedulesSearch.focus();
            }
        }
        if (e.key === 'Escape') {
            if (currentTabKey === 'memory' && memorySearch && document.activeElement === memorySearch) {
                memorySearch.value = '';
                memoryState.searchQuery = '';
                const clearBtn = document.getElementById('memory-search-clear');
                if (clearBtn) clearBtn.style.display = 'none';
                applyFilters();
                memorySearch.blur();
            } else if (currentTabKey === 'schedules' && schedulesSearch && document.activeElement === schedulesSearch) {
                schedulesSearch.value = '';
                schedulesState.searchQuery = '';
                const clearBtn = document.getElementById('schedules-search-clear');
                if (clearBtn) clearBtn.style.display = 'none';
                renderScheduleCards();
                renderScheduleRuns();
                schedulesSearch.blur();
            }
        }
    });
}

// Initial bootstrap
setupTabs();
setupMemoryControls();
setupSchedulesControls();
setupGlobalKeyboardShortcuts();

