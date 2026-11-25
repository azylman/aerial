# Discord Multimodal Visual Media & Image Delivery Standards

1. **Native Discord Attachments**:
   - Visual media referenced via `![Alt Text](/path/to/image.png)` is sandboxed (`brain/<id>/`, `scratch/`, `/tmp/`) and delivered via Discord multipart. Remote URLs remain standard markdown links.

2. **Modality Triage & Theming Standards**:
   - Images permitted for metric charts, architecture topologies, and generative UI wireframes. Telemetry charts use Discord Dark Mode palettes (`#2B2D31`/`#1E1F22` background, high-contrast labels, `alpha=0.4` gridlines) at `dpi=300`.
   - Strictly NEVER embed code snippets, diffs, configs, logs, or stack traces as images (use code blocks). Fall back to structured Markdown lists or Mermaid code blocks if image rendering is unavailable.
