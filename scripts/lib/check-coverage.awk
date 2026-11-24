BEGIN {
    num_pkgs = 0
    num_svcs = 0
    while ((getline line < svc_pkg_file) > 0) {
        if (line == "") continue
        split(line, parts, "|")
        s = parts[1]
        p = parts[2]
        pkg_list[++num_pkgs] = p
        pkg_svc[p] = s
        pkg_to_svc[p] = s
        if (!(s in known_svcs)) {
            known_svcs[s] = 1
            svc_list[++num_svcs] = s
        }
    }
    close(svc_pkg_file)

    for (i = 1; i <= num_svcs; i++) {
        for (j = i + 1; j <= num_svcs; j++) {
            if (length(svc_list[j]) > length(svc_list[i])) {
                tmp = svc_list[i]
                svc_list[i] = svc_list[j]
                svc_list[j] = tmp
            }
        }
    }

    mode_line = "mode: set"

    if (cov_file_list != "") {
        while ((getline cov_file < cov_file_list) > 0) {
            if (cov_file == "") continue
            bt_idx = index(cov_file, "bazel-testlogs/")
            if (bt_idx == 0) continue
            sub_path = substr(cov_file, bt_idx + length("bazel-testlogs/"))
            file_svc = ""
            for (i = 1; i <= num_svcs; i++) {
                s = svc_list[i]
                if (index(sub_path, s "/") == 1) {
                    file_svc = s
                    break
                }
            }
            if (file_svc == "") continue

            while ((getline line < cov_file) > 0) {
                if (line ~ /^mode:/) {
                    mode_line = line
                    continue
                }
                nf = split(line, fields, " ")
                if (nf < 3) continue
                first_f = fields[1]
                stmts = fields[nf - 1] + 0
                cnt = fields[nf] + 0

                col_idx = index(first_f, ":")
                if (col_idx <= 0) continue
                fpath = substr(first_f, 1, col_idx - 1)
                slash_idx = 0
                for (k = length(fpath); k >= 1; k--) {
                    if (substr(fpath, k, 1) == "/") {
                        slash_idx = k
                        break
                    }
                }
                if (slash_idx <= 1) continue
                pkg = substr(fpath, 1, slash_idx - 1)

                if (pkg in pkg_to_svc && pkg_to_svc[pkg] == file_svc) {
                    block_id = first_f " " stmts
                    if (!((pkg, block_id) in pkg_block_seen)) {
                        pkg_block_seen[pkg, block_id] = 1
                        pkg_blocks[pkg, ++pkg_num_blocks[pkg]] = block_id
                        pkg_block_stmts[pkg, block_id] = stmts
                    }
                    pkg_block_count[pkg, block_id] += cnt
                }
            }
            close(cov_file)
        }
        close(cov_file_list)
    }

    for (i = 1; i <= num_pkgs; i++) {
        p = pkg_list[i]
        s = pkg_svc[p]
        thresh = default_thresh + 0.0

        nb = pkg_num_blocks[p] + 0
        tot = 0
        cov = 0
        if (nb > 0) {
            for (b = 1; b <= nb; b++) {
                blk = pkg_blocks[p, b]
                st = pkg_block_stmts[p, blk]
                tot += st
                if (pkg_block_count[p, blk] > 0) {
                    cov += st
                }
            }
        }

        if (tot > 0) {
            pct_val = sprintf("%.1f", (cov / tot) * 100.0)
            if (cov * 100 < tot * thresh) {
                status = "FAIL"
            } else {
                status = "PASS"
            }
        } else {
            pct_val = "-1"
            status = "N/A"
        }

        thresh_str = sprintf("%.1f", thresh)

        if (gaps_mode || custom_prof_dir != "" || status == "FAIL") {
            safe_pkg = s "_" p
            gsub(/[\/\.]/, "_", safe_pkg)
            out_file = prof_dir "/" safe_pkg ".out"
            print mode_line > out_file
            if (nb > 0) {
                for (b = 1; b <= nb; b++) {
                    blk = pkg_blocks[p, b]
                    print blk " " pkg_block_count[p, blk] > out_file
                }
            }
            close(out_file)
        }

        print s "|" p "|" tot "|" cov "|" pct_val "|" thresh_str "|" status > pkg_data_file
    }
    close(pkg_data_file)
}
