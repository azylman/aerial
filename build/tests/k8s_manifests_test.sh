#!/bin/sh
set -eu

if [ -n "${TEST_SRCDIR:-}" ]; then
    cd "${TEST_SRCDIR}/${TEST_WORKSPACE:-_main}"
fi

# Step A: Validate Kustomize rendering (required)
if ! command -v kubectl >/dev/null 2>&1; then
    echo "🚨 Error: kubectl is required for Kubernetes manifest validation" >&2
    exit 1
fi
kubectl kustomize --load-restrictor=LoadRestrictionsNone k8s >/dev/null

# Step B: Standard-library Python validation of Flux substitutions (required)
if ! command -v python3 >/dev/null 2>&1; then
    echo "🚨 Error: python3 is required for Kubernetes manifest validation" >&2
    exit 1
fi

python3 - k8s << 'EOF'
import glob
import os
import re
import sys

k8s_dir = sys.argv[1]
yaml_files = sorted(glob.glob(os.path.join(k8s_dir, "*.yaml")))

errors = []

for yaml_path in yaml_files:
    basename = os.path.basename(yaml_path)
    rel_path = yaml_path
    with open(yaml_path, "r", encoding="utf-8") as f:
        content = f.read()

    lines = content.splitlines()
    docs = []
    current_doc = []
    doc_start = 1
    for idx, line in enumerate(lines, start=1):
        if re.match(r"^---\s*$", line):
            if current_doc:
                docs.append((doc_start, current_doc))
            current_doc = []
            doc_start = idx + 1
        else:
            current_doc.append((idx, line))
    if current_doc:
        docs.append((doc_start, current_doc))

    for start_line, doc in docs:
        doc_text = "\n".join(l for _, l in doc)
        if basename == "kustomization.yaml" or "kustomize.toolkit.fluxcd.io/substitute: disabled" in doc_text:
            continue
        for lineno, line in doc:
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            for m in re.finditer(r"(?<!\$)\$\{([^}]+)\}", line):
                expr = m.group(1)
                if ":=" not in expr and ":-" not in expr:
                    errors.append(f"{rel_path}:{lineno}: unescaped ${{{expr}}} without Flux default (:= or :-) or kustomize.toolkit.fluxcd.io/substitute: disabled")

if errors:
    print("🚨 Kubernetes manifest validation errors detected:", file=sys.stderr)
    for err in errors:
        print(f"   • {err}", file=sys.stderr)
    sys.exit(1)
EOF
