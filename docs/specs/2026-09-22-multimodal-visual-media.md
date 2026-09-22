# Multimodal Visual Media & Image Delivery Standards

## Overview
Aerial supports delivering rich visual artifacts, metrics visualizations, network topology diagrams, and generative UI wireframes directly to Discord as native attachments. This document details Discord Dark Mode chart theming standards, tool triage guidelines, sandboxing rules, and backend delivery mechanics.

---

## 1. Markdown Embed Syntax & Delivery

### Embedding Local Images
Local generated images and visual artifacts must be referenced in Markdown using standard embed syntax:
```markdown
![Descriptive Alt Text](/path/to/image.png)
```

### Backend Sandboxing & Multipart Streaming
- **Sandboxed Roots**: The Go execution engine (`aerial-brain`) validates image paths against sandboxed roots:
  - `/root/.gemini/antigravity-cli/brain/<conversation-id>/`
  - `/root/.gemini/antigravity-cli/scratch/`
  - `/data/scratch/`
  - `/tmp/`
- **Security**: Symlinks resolving outside approved sandboxes are rejected. Raw local disk paths are stripped from chat logs and database history.
- **Multipart Streaming**: Binary image streams are attached strictly to the final (last) message chunk via Discord multipart (`ChannelMessageSendComplex`), preventing duplicate media uploads on multi-chunk turns.
- **Remote URLs**: Remote `http://` and `https://` URLs remain standard Markdown links and are left untouched for native Discord client unfurling.

---

## 2. Modality Triage: Code vs Visuals

### Permitted Image Modalities
- **Time-Series & Telemetry Trends**: Resource utilization, database transaction rates, latency heatmaps, queue depth histories (generated via Python `matplotlib` / `seaborn`).
- **Architecture & System Topologies**: Service mesh diagrams, state machines, Docker networks, and dependency graphs (via Graphviz or diagramming scripts).
- **Generative Art & UI Wireframes**: Cyberpunk visual concepts, dashboard mockups, and mobile layout wireframes (via `generate_image`).

### Strict Negative Prohibitions
- **NEVER use images for source code snippets**: Always use syntax-highlighted Markdown code blocks.
- **NEVER use images for git diffs**: Always use `diff` code blocks or GitHub PR links.
- **NEVER use images for terminal execution logs or stack traces**: Use Markdown code blocks or attached `.log` files.
- **NEVER use images for small tabular data (<10 rows)**: Use clean, formatted key-value text lists.

---

## 3. Discord Dark Mode Chart Theming Standards

When generating visual charts via Python (`matplotlib`, `seaborn`), adhere to Discord Dark Theme aesthetics to guarantee legibility on mobile and desktop clients:

### Color Palette
- **Background**: Dark charcoal `#2B2D31` or `#1E1F22` (or transparent `facecolor='none'`).
- **Text & Labels**: High-contrast light gray `#DBDEE1` or white `#FFFFFF`.
- **Grid Lines**: Subtle muted gray `#3F4147` with alpha transparency (`alpha=0.4`).
- **Accent Lines & Data Points**: Cyberpunk neon palette:
  - Electric Violet: `#9D4EDD`
  - Neon Cyan: `#00F0FF`
  - Hot Magenta: `#FF007F`
  - Cyber Lime: `#39FF14`

### Resolution & Aspect Ratios
- **DPI**: Minimum `dpi=300` for crisp text rendering on high-DPI smartphone displays.
- **Aspect Ratio**: Standard 16:9 (`figsize=(12, 6.75)`) for horizontal telemetry trends or 4:3 (`figsize=(10, 7.5)`) for multi-panel dashboards.
- **Font Scaling**: Explicitly scale titles (`fontsize=14, weight='bold'`), axis labels (`fontsize=11`), and tick labels (`fontsize=9`).

### Python Matplotlib Boilerplate Example
```python
import matplotlib.pyplot as plt

plt.style.use('dark_background')
fig, ax = plt.subplots(figsize=(12, 6.75), dpi=300)

fig.patch.set_facecolor('#2B2D31')
ax.set_facecolor('#1E1F22')

# Data plotting
ax.plot(x, y, color='#00F0FF', linewidth=2.5, label='Queue Depth')

# Styling
ax.tick_params(colors='#DBDEE1', labelsize=10)
ax.xaxis.label.set_color('#DBDEE1')
ax.yaxis.label.set_color('#DBDEE1')
ax.grid(True, linestyle='--', alpha=0.3, color='#3F4147')
ax.legend(facecolor='#2B2D31', edgecolor='#3F4147', labelcolor='#DBDEE1')

plt.tight_layout()
plt.savefig('/tmp/telemetry_chart.png', facecolor=fig.get_facecolor(), edgecolor='none')
```

---

## 4. Graceful Degradation

If image generation or rendering dependencies (e.g. Python libraries, headless display) fail:
1. Gracefully fall back to structured text summaries, bullet points, or ASCII bar charts.
2. For workflows and entity relationships, output inline Mermaid code blocks (`mermaid`).
3. Never emit raw binary garbage, unhandled Python tracebacks, or broken local file links.
