function escapeHtml(str) {
    return String(str)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

let activeServicesCache = [];

async function fetchStatus() {
    try {
        const res = await fetch('/api/status');
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        document.getElementById('last-refresh').textContent = new Date(data.system_time).toLocaleTimeString();
        document.getElementById('overall-status').textContent = escapeHtml(data.cluster_status.toUpperCase());
        
        // --- 1. RENDER DEPLOYMENT PIPELINE ---
        const deploysContainer = document.getElementById('deployments-container');
        const deployBadge = document.getElementById('deploy-count-badge');
        const deploys = data.deployments || [];

        deployBadge.textContent = `${deploys.length} IN PROGRESS`;
        deploysContainer.innerHTML = '';

        if (deploys.length === 0) {
            deploysContainer.innerHTML = `
                <div class="deploy-idle-card">
                    <div class="idle-indicator">
                        <span class="pulse-dot healthy"></span>
                        <span>SYSTEM IN SYNC // NO PENDING DEPLOYS</span>
                    </div>
                    <div>WATCHTOWER POLLING GHCR (60s)</div>
                </div>
            `;
        } else {
            deploys.forEach(dep => {
                const steps = dep.steps || [
                    { name: "Commit Trigger", icon: "📦", status: dep.progress >= 20 ? "completed" : "active" },
                    { name: "CI Build & GHCR", icon: "⚙️", status: dep.progress >= 40 ? "completed" : dep.progress >= 20 ? "active" : "pending" },
                    { name: "Image Pull", icon: "⬇️", status: dep.progress >= 60 ? "completed" : dep.progress >= 40 ? "active" : "pending" },
                    { name: "Container Swap", icon: "🔄", status: dep.progress >= 85 ? "completed" : dep.progress >= 60 ? "active" : "pending" },
                    { name: "Health Check", icon: "🩺", status: dep.progress >= 100 ? "completed" : dep.progress >= 85 ? "active" : "pending" }
                ];

                const stepsHTML = steps.map(step => `
                    <div class="step-panel step-${escapeHtml(step.status)}">
                        <div class="step-header">
                            <span class="step-icon">${escapeHtml(step.icon)}</span>
                            <span class="step-status-badge">${step.status === 'completed' ? '✓ DONE' : step.status === 'active' ? '⚡ RUNNING' : '○ PENDING'}</span>
                        </div>
                        <div class="step-name">${escapeHtml(step.name)}</div>
                    </div>
                `).join('');

                const card = document.createElement('div');
                card.className = 'deploy-card';
                card.innerHTML = `
                    <div class="deploy-card-header">
                        <div class="deploy-target">
                            <span class="deploy-service-name">${escapeHtml(dep.service.toUpperCase())}</span>
                            <span class="deploy-commit">${escapeHtml(dep.commit)}</span>
                        </div>
                        <span class="deploy-stage-badge ${dep.stage === 'live' ? 'live' : ''}">⚡ ${escapeHtml(dep.stage.toUpperCase())}</span>
                    </div>
                    <div class="deploy-steps-grid">
                        ${stepsHTML}
                    </div>
                    <div class="deploy-progress-bg">
                        <div class="deploy-progress-fill" style="width: ${dep.progress}%;"></div>
                    </div>
                    <div class="deploy-footer">
                        <span>STAGE: ${escapeHtml(dep.stage.toUpperCase())} (${dep.progress}%)</span>
                        <span>STARTED: ${new Date(dep.started_at).toLocaleTimeString()}</span>
                    </div>
                `;
                deploysContainer.appendChild(card);
            });
        }

        // --- 2. RENDER SERVICES GRID ---
        activeServicesCache = data.services || [];
        const grid = document.getElementById('services-grid');
        grid.innerHTML = '';

        let healthyCount = 0;
        activeServicesCache.forEach((svc, index) => {
            if (svc.status === 'healthy') healthyCount++;
            
            const safeName = escapeHtml(svc.name);
            const safeStatus = escapeHtml(svc.status);
            const hours = Math.floor(svc.uptime_seconds / 3600);
            const mins = Math.floor((svc.uptime_seconds % 3600) / 60);

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
                    <span>UPTIME: ${hours}h ${mins}m</span>
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
        document.getElementById('active-count').textContent = `${healthyCount} / ${total}`;

        // Permet Score Calculation
        const healthRatio = total > 0 ? (healthyCount / total) : 0;
        const permetBar = document.getElementById('permet-bar-fill');
        const permetVal = document.getElementById('permet-score-val');
        
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

    } catch (err) {
        console.error('Failed to fetch system status:', err);
        document.getElementById('overall-status').textContent = 'OFFLINE';
        document.getElementById('overall-status').className = 'value text-danger';
        document.getElementById('cluster-sub').textContent = 'SYSTEM DISCONNECTED';
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

    title.textContent = `DIAGNOSTICS // ${svc.name.toUpperCase()}`;

    const hours = Math.floor(svc.uptime_seconds / 3600);
    const mins = Math.floor((svc.uptime_seconds % 3600) / 60);
    const secs = svc.uptime_seconds % 60;

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
                <span class="val">${hours}h ${mins}m ${secs}s</span>
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

    drawer.classList.add('active');
    overlay.classList.add('active');
}

function closeDiagnosticDrawer() {
    document.getElementById('diagnostic-drawer').classList.remove('active');
    document.getElementById('drawer-overlay').classList.remove('active');
}

document.getElementById('drawer-close-btn').addEventListener('click', closeDiagnosticDrawer);
document.getElementById('drawer-overlay').addEventListener('click', closeDiagnosticDrawer);

// Initial call & polling
fetchStatus();
setInterval(fetchStatus, 5000);
