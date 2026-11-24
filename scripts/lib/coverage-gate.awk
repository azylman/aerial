BEGIN {
    split(filter, fa, " ")
    for (i in fa) {
        k = fa[i]
        sub(/^github\.com\/azylman\/aerial\/?/, "", k)
        sub(/^\.\/?/, "", k)
        sub(/\/$/, "", k)
        if (k != "") want[k] = 1
    }
}
/^mode:/ { next }
NF >= 3 {
    b = $1
    s = $(NF-1) + 0
    c = $NF + 0
    if (!(b in stmts)) {
        stmts[b] = s
        f = b
        sub(/:.*$/, "", f)
        sub(/\/[^/]+$/, "", f)
        sub(/^github\.com\/azylman\/aerial\/?/, "", f)
        sub(/^\.\/?/, "", f)
        sub(/\/$/, "", f)
        pkg[b] = f
    }
    if (c > 0) cov[b] = 1
}
END {
    for (b in stmts) {
        p = pkg[b]
        p_tot[p] += stmts[b]
        if (cov[b]) p_cov[p] += stmts[b]
    }
    fail = 0
    for (p in p_tot) {
        if (mode == "staged" && filter != "" && !(p in want)) continue
        if (p_tot[p] > 0 && ((p_cov[p] / p_tot[p]) * 100.0) < 95.0) {
            printf "🚨 [Aerial Verify] %s: %.1f%% below 95.0%% floor\n", p, (p_cov[p]/p_tot[p])*100.0 > "/dev/stderr"
            fail = 1
        }
    }
    if (fail) exit 1
}
