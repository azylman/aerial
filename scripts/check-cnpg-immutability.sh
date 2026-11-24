#!/bin/sh
set -eu

REPO_ROOT="${1:-${REPO_ROOT:-}}"
[ -n "$REPO_ROOT" ] || REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

command -v git >/dev/null 2>&1 || exit 0
command -v python3 >/dev/null 2>&1 || exit 0

if ! git -C "$REPO_ROOT" rev-parse --verify origin/main >/dev/null 2>&1; then
    exit 0
fi

python3 - "$REPO_ROOT" << 'EOF'
import glob
import os
import re
import subprocess
import sys

repo_root = sys.argv[1]
k8s_dir = os.path.join(repo_root, "k8s")
yaml_files = sorted(glob.glob(os.path.join(k8s_dir, "*.yaml")))

errors = []

def parse_cnpg_resources(content):
    docs = re.split(r"(?m)^---\s*$", content)
    resources = {}
    for doc in docs:
        if not re.search(r"(?m)^\s*apiVersion:\s*[\"']?postgresql\.cnpg\.io/v1", doc):
            continue
        kind_m = re.search(r"(?m)^\s*kind:\s*[\"']?(Database|ScheduledBackup)[\"']?\s*$", doc)
        if not kind_m:
            continue
        kind = kind_m.group(1).strip()

        meta_m = re.search(r"(?m)^\s*metadata:\s*\n((?:^[ \t]+[^\n]*\n?)*)", doc)
        if not meta_m:
            continue
        meta_text = meta_m.group(1)
        name_m = re.search(r"(?m)^\s+name:\s*[\"']?([^\"'\s]+)", meta_text)
        if not name_m:
            continue
        name = name_m.group(1).strip()
        ns_m = re.search(r"(?m)^\s+namespace:\s*[\"']?([^\"'\s]+)", meta_text)
        namespace = ns_m.group(1).strip() if ns_m else "default"

        spec_m = re.search(r"(?m)^\s*spec:\s*\n((?:^[ \t]+[^\n]*\n?)*)", doc)
        if not spec_m:
            continue
        spec_text = spec_m.group(1)
        cluster_m = re.search(r"(?m)^\s+cluster:\s*\n(?:^\s+[^\n]*\n)*?\s+name:\s*[\"']?([^\"'\s]+)", spec_text)
        if not cluster_m:
            continue
        cluster_name = cluster_m.group(1).strip()
        resources[(kind, namespace, name)] = cluster_name
    return resources

for yaml_path in yaml_files:
    basename = os.path.basename(yaml_path)
    rel_path = os.path.relpath(yaml_path, repo_root)
    with open(yaml_path, "r", encoding="utf-8") as f:
        content = f.read()

    current_cnpg = parse_cnpg_resources(content)
    if current_cnpg:
        git_show = subprocess.run(["git", "-C", repo_root, "show", f"origin/main:k8s/{basename}"],
                                  stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        if git_show.returncode == 0:
            base_cnpg = parse_cnpg_resources(git_show.stdout)
            for (kind, ns, name), current_cluster in current_cnpg.items():
                if (kind, ns, name) in base_cnpg:
                    base_cluster = base_cnpg[(kind, ns, name)]
                    if current_cluster != base_cluster:
                        errors.append(
                            f"{rel_path}: {kind} '{ns}/{name}' changes spec.cluster.name from '{base_cluster}' to '{current_cluster}'. "
                            f"spec.cluster.name is immutable on CloudNativePG Database/ScheduledBackup CRDs and a new resource name must be used."
                        )

if errors:
    print("🚨 [Aerial Verify] CloudNativePG immutability violation detected:", file=sys.stderr)
    for err in errors:
        print(f"   • {err}", file=sys.stderr)
    sys.exit(1)
EOF
