---
name: ha-operations
description: Use this skill when the user asks to manage Home Assistant, check device status, create or edit automations, or triage smart home issues.
---

# Home Assistant Operations & Triage Runbook

This skill provides step-by-step procedures for managing Home Assistant using the `ha-mcp` integration.

## Key Principles
1. **Always Inspect First**: Use `call_mcp_tool` with `ServerName: "ha-mcp"` to query entity states and automations before suggesting or making changes.
2. **Safe Automation Edits**: When configuring automations via `ha_config_set_automation`, verify YAML formatting and trigger/action blocks.
3. **Report Clear Results**: When responding back to the user, summarize the state changes, entity IDs, and automation names clearly.

## Tool Reference
* `ha_get_overview`: Get an overview of configured devices and domains.
* `ha_search`: Find specific entity IDs, automations, or areas.
* `ha_config_get_automation`: Read automation YAML definitions.
* `ha_config_set_automation`: Write or update automation YAML definitions.
* `ha_call_read_tool` / `ha_call_write_tool`: Query or execute domain-specific services.