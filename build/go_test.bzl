load("@rules_go//go:def.bzl", _go_test = "go_test")

def go_test(name, data = [], **kwargs):
    pkg = native.package_name()
    extra_data = []

    # Root testdata (prlifecycle fixtures)
    if (
        pkg == "pkg/prwire" or
        pkg.startswith("services/vanadis-restate/internal/prstate") or
        pkg == "services/synapse/internal/router"
    ):
        extra_data.append("//:testdata")

    # Canonical hooks fixture for eri env tests
    if pkg == "services/eri/pkg/env":
        extra_data.append("//:canonical_hooks")

    # Console static assets for console server tests
    if pkg.startswith("services/console"):
        extra_data.append("//services/console:static")

    # Synapse testdata for synapse internal subpackages
    if pkg.startswith("services/synapse") and pkg != "services/synapse":
        extra_data.append("//services/synapse:testdata")

    # Schema.sql only for packages running database tests with pgtest
    if (
        pkg.startswith("services/eri") or
        pkg.startswith("services/coven") or
        pkg.startswith("services/haro") or
        pkg.startswith("services/prospera") or
        pkg.startswith("services/transcripts") or
        pkg.startswith("services/vanadis-restate") or
        pkg == "pkg/pgtest" or
        pkg == "pkg/hermetictest"
    ):
        extra_data.append("//services/eri/pkg/db:schema.sql")

    all_data = list(data)
    for d in extra_data:
        if d not in all_data:
            all_data.append(d)

    _go_test(
        name = name,
        data = all_data,
        **kwargs
    )
