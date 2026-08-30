async function fetchStatus() {
    try {
        const res = await fetch('/api/status');
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        document.getElementById('last-refresh').textContent = new Date(data.system_time).toLocaleTimeString();
        document.getElementById('overall-status').textContent = data.cluster_status.toUpperCase();
        
        const grid = document.getElementById('services-grid');
        grid.innerHTML = '';

        let activeCount = 0;
        data.services.forEach(svc => {
            if (svc.status === 'healthy') activeCount++;
            
            const card = document.createElement('div');
            card.className = 'service-card';
            card.innerHTML = `
                <div class="header">
                    <span class="title">${svc.name}</span>
                    <span class="badge ${svc.status}">${svc.status.toUpperCase()}</span>
                </div>
                <div style="font-size: 0.85rem; color: #8892b0; margin-top: 0.5rem;">
                    Uptime: ${Math.floor(svc.uptime_seconds / 3600)}h ${Math.floor((svc.uptime_seconds % 3600) / 60)}m
                </div>
            `;
            grid.appendChild(card);
        });

        document.getElementById('active-count').textContent = `${activeCount} / ${data.services.length}`;
    } catch (err) {
        console.error('Failed to fetch system status:', err);
        document.getElementById('overall-status').textContent = 'OFFLINE';
        document.getElementById('overall-status').className = 'value text-danger';
    }
}

fetchStatus();
setInterval(fetchStatus, 5000);
