load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load", "oci_push")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

def _go_prod_cover_transition_impl(settings, attr):
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
    out_name = ctx.attr.out_name if ctx.attr.out_name else orig_exe.basename
    out_exe = ctx.actions.declare_file(ctx.label.name + "/" + out_name)
    ctx.actions.run_shell(
        inputs = [orig_exe],
        outputs = [out_exe],
        command = "cp -L \"$1\" \"$2\"",
        arguments = [orig_exe.path, out_exe.path],
        mnemonic = "CopyCoverBin",
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
        "out_name": attr.string(),
    },
    executable = True,
)

def _go_pure_binary_transition_impl(settings, attr):
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
    if binary == None:
        if name in ["docket-mcp", "kernel-mcp"]:
            binary = "//mcp/" + name + ":" + name
        else:
            binary = "//services/" + name + ":" + name

    if bin_path == None:
        bin_path = "/app/" + name

    parts = bin_path.strip("/").split("/")
    bin_name = parts[-1]
    if len(parts) > 1:
        bin_dir = "/" + "/".join(parts[:-1])
    else:
        bin_dir = "/app"

    bin_target = binary
    strip_prefix = None
    if cover:
        cover_bin_name = name + "_cover_bin"
        go_cover_binary(
            name = cover_bin_name,
            actual = binary,
            out_name = bin_name,
        )
        bin_target = ":" + cover_bin_name
        strip_prefix = cover_bin_name
    else:
        pure_bin_name = name + "_pure_bin"
        go_pure_binary(
            name = pure_bin_name,
            actual = binary,
        )
        named_bin_target = name + "_bin_named"
        native.genrule(
            name = named_bin_target,
            srcs = [":" + pure_bin_name],
            outs = [name + "_bin/" + bin_name],
            cmd = "cp -L $< $@",
            executable = True,
        )
        bin_target = ":" + named_bin_target
        strip_prefix = name + "_bin"

    layer_target = name + "_bin_tar"
    pkg_tar(
        name = layer_target,
        srcs = [bin_target],
        package_dir = bin_dir,
        strip_prefix = strip_prefix,
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
