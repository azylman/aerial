# Discord Multimodal Visual Media & Image Delivery Standards

1. **Native Discord Attachments**:
   - Visual media referenced via `![Alt Text](/path/to/image.png)` is sandboxed (`brain/<id>/`, `scratch/`, `/tmp/`) and delivered via Discord multipart. Remote URLs remain standard markdown links.

2. **Modality Triage**:
   - Images permitted for metric charts, architecture topologies, and generative UI wireframes.
   - Strictly NEVER embed code snippets, diffs, configs, logs, or stack traces as images (use code blocks). Full visualization specs in `docs/specs/2026-09-22-multimodal-visual-media.md`.
