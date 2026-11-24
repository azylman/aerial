"""Rules and macros for building and transitioning Aerial OCI container images."""

load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load", "oci_push")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

def _go_prod_cover_transition_impl(_settings, _attr):
    return {
        "//command_line_option:collect_code_coverage": True,
        "//command_line_option:instrumentation_filter": "^//(pkg|services|mcp|images)[/:]",
        "//command_line_option:strip": "always",
        "@rules_go//go/config:cover_format": "go_cover",
        "@rules_go//go/config:pure": True,
        "@rules_go//go/config:tags": ["timetzdata"],
    }

_go_prod_cover_transition = transition(
    implementation = _go_prod_cover_transition_impl,
    inputs = [],
    outputs = [
        "//command_line_option:collect_code_coverage",
        "//command_line_option:instrumentation_filter",
        "//command_line_option:strip",
        "@rules_go//go/config:cover_format",
        "@rules_go//go/config:pure",
        "@rules_go//go/config:tags",
    ],
)

def _go_cover_binary_impl(ctx):
    actual = ctx.attr.actual[0]
    orig_exe = actual[DefaultInfo].files.to_list()[0]
    out_exe = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.symlink(
        output = out_exe,
        target_file = orig_exe,
        is_executable = True,
    )
    return [
        DefaultInfo(
            files = depset([out_exe]),
            runfiles = actual[DefaultInfo].default_runfiles,
            executable = out_exe,
        ),
    ]

go_cover_binary = rule(
    implementation = _go_cover_binary_impl,
    attrs = {
        "actual": attr.label(
            cfg = _go_prod_cover_transition,
            mandatory = True,
        ),
    },
    executable = True,
)

def _go_pure_binary_transition_impl(_settings, _attr):
    return {
        "//command_line_option:strip": "always",
        "@rules_go//go/config:pure": True,
        "@rules_go//go/config:tags": ["timetzdata"],
    }

_go_pure_binary_transition = transition(
    implementation = _go_pure_binary_transition_impl,
    inputs = [],
    outputs = [
        "//command_line_option:strip",
        "@rules_go//go/config:pure",
        "@rules_go//go/config:tags",
    ],
)

def _go_pure_binary_impl(ctx):
    actual = ctx.attr.actual[0]
    return [
        DefaultInfo(
            files = actual[DefaultInfo].files,
            runfiles = actual[DefaultInfo].default_runfiles,
        ),
    ]

go_pure_binary = rule(
    implementation = _go_pure_binary_impl,
    attrs = {
        "actual": attr.label(
            cfg = _go_pure_binary_transition,
            mandatory = True,
        ),
    },
)

def _build_covdata_impl(ctx):
    go = ctx.toolchains["@rules_go//go:toolchain"]
    out = ctx.actions.declare_file("covdata")
    goroot = go.sdk.root_file.dirname
    ctx.actions.run_shell(
        outputs = [out],
        inputs = depset([go.sdk.go, go.sdk.package_list, go.sdk.root_file], transitive = [go.sdk.srcs, go.sdk.tools, go.sdk.libs, go.sdk.headers]),
        command = """
            set -e
            export GOROOT="$PWD/{goroot}"
            export GOCACHE="$PWD/.gocache"
            export CGO_ENABLED=0
            "{go_bin}" build -trimpath -ldflags='-s -w' -o "{out}" cmd/covdata
        """.format(
            goroot = goroot,
            go_bin = go.sdk.go.path,
            out = out.path,
        ),
        mnemonic = "BuildCovdata",
    )
    return [DefaultInfo(files = depset([out]))]

build_covdata = rule(
    implementation = _build_covdata_impl,
    toolchains = ["@rules_go//go:toolchain"],
)

def aerial_go_image(
        name,
        binary = None,
        bin_path = None,
        workdir = "/app",
        base = "@distroless_static",
        user = "65532:65532",
        cover = True,
        extra_tars = [],
        entrypoint = None,
        cmd = None,
        env = None,
        exposed_ports = ["8080/tcp"],
        image_prefix = "aerial",
        **kwargs):
    """Builds an OCI container image for an Aerial Go service binary.

    Args:
        name: Target image name.
        binary: Build target of the binary to package.
        bin_path: Absolute destination path inside the container.
        workdir: Working directory inside the container.
        base: Base OCI image reference.
        user: UID:GID pair for container user.
        cover: Whether to build with coverage instrumentation.
        extra_tars: Additional tar layer targets.
        entrypoint: Container entrypoint list.
        cmd: Container default command list.
        env: Environment variables map.
        exposed_ports: List of exposed network ports.
        image_prefix: Image repository prefix.
        **kwargs: Additional arguments passed to oci_image.
    """
    if binary == None:
        if name in ["docket-mcp", "kernel-mcp"]:
            binary = "//mcp/" + name + ":" + name
        else:
            binary = "//services/" + name + ":" + name

    if bin_path == None:
        bin_path = "/app/" + name

    if cover:
        cover_bin_name = name + "_cover_bin"
        go_cover_binary(
            name = cover_bin_name,
            actual = binary,
        )
        bin_target = ":" + cover_bin_name
    else:
        pure_bin_name = name + "_pure_bin"
        go_pure_binary(
            name = pure_bin_name,
            actual = binary,
        )
        bin_target = ":" + pure_bin_name

    layer_target = name + "_bin_tar"
    pkg_tar(
        name = layer_target,
        files = {bin_target: bin_path.lstrip("/")},
        mode = "0755",
    )
    native.alias(
        name = name + "_layer",
        actual = ":" + layer_target,
    )

    tars = [":" + layer_target] + extra_tars

    if entrypoint == None:
        entrypoint = [bin_path]

    # Distroless rules_oci images omit shells/curl/wget and Docker-level HEALTHCHECK directives;
    # Kubernetes livenessProbe and readinessProbe HTTP checks in k8s/*.yaml supervise service health.
    oci_image(
        name = name,
        base = base,
        tars = tars,
        entrypoint = entrypoint,
        cmd = cmd,
        env = env,
        exposed_ports = exposed_ports,
        user = user,
        workdir = workdir,
        **kwargs
    )

    oci_load(
        name = "load_" + name,
        image = ":" + name,
        repo_tags = ["ghcr.io/azylman/" + image_prefix + "-" + name + ":local"],
    )

    oci_push(
        name = "push_" + name,
        image = ":" + name,
        repository = "ghcr.io/azylman/" + image_prefix + "-" + name,
    )
