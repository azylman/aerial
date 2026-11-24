"""Macro wrapping rules_go go_test to inject standard Aerial testdata fixtures."""

load("@rules_go//go:def.bzl", _go_test = "go_test")

def go_test(name, data = [], **kwargs):
    """Defines a Go test target with standard repository fixtures and testdata.

    Args:
        name: Name of the test target.
        data: Additional runtime data files or targets.
        **kwargs: Additional arguments forwarded to rules_go go_test.
    """
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
        extra_data.append("//services/console/static:static")

    # Synapse testdata for synapse internal subpackages
    if pkg.startswith("services/synapse") and pkg != "services/synapse":
        extra_data.append("//services/synapse/testdata:testdata")

    restate_data = []
    restate_env = None

    # Restate server binary for integration tests
    if pkg in ("services/eri/pkg/restatetest", "services/eri/pkg/queue"):
        restate_data = select({
            "@rules_go//go/platform:linux_arm64": ["@restate_server_linux_arm64//:restate-server"],
            "//conditions:default": ["@restate_server_linux_amd64//:restate-server"],
        })
        restate_env = select({
            "@rules_go//go/platform:linux_arm64": {"RESTATE_SERVER_BIN": "$(rootpath @restate_server_linux_arm64//:restate-server)"},
            "//conditions:default": {"RESTATE_SERVER_BIN": "$(rootpath @restate_server_linux_amd64//:restate-server)"},
        })

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

    if restate_data:
        all_data = all_data + restate_data

    if restate_env != None and "env" not in kwargs:
        kwargs["env"] = restate_env

    _go_test(
        name = name,
        data = all_data,
        **kwargs
    )
