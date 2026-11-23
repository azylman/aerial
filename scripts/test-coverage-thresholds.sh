#!/bin/sh
# Exercise production gates with synthetic Go profiles, without running Go tests.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT HUP INT TERM
mkdir -p "$fixture/scripts/lib" "$fixture/pkg" "$fixture/bin"
cp "$root/scripts/check-coverage.sh" "$fixture/scripts/"
cp "$root/scripts/lib/hermetic-go-env.sh" "$fixture/scripts/lib/"
: > "$fixture/pkg/go.mod"
# Extract the actual function, leaving verify's top-level orchestration unexecuted.
sed -n '/^run_go_pkg_coverage() {$/,/^}$/p' "$root/scripts/verify.sh" > "$fixture/staged.sh"
cat > "$fixture/bin/go" <<STUB
#!/bin/sh
case "\$1" in
    list) echo example/pkg ;;
    test) for arg do case "\$arg" in -coverprofile=*) cp '$fixture/profile' "\${arg#*=}" ;; esac; done ;;
esac
STUB
chmod +x "$fixture/bin/go"
export PATH="$fixture/bin:$PATH"
failures=0
while read -r covered total expected; do
    printf 'mode: set\nexample/pkg/file.go:1.1,2.1 %s 1\nexample/pkg/file.go:3.1,4.1 %s 0\n' "$covered" "$((total - covered))" > "$fixture/profile"
    for gate in staged ci; do
        actual=0
        if [ "$gate" = staged ]; then
            sh -c 'has_cmd() { command -v "$1" >/dev/null; }; . "$1/scripts/lib/hermetic-go-env.sh"; . "$1/staged.sh"; run_go_pkg_coverage "$1/pkg" .' sh "$fixture" > "$fixture/log" 2>&1 || actual=$?
        else
            sh "$fixture/scripts/check-coverage.sh" --check --summary --profile-dir "$fixture/results" > "$fixture/log" 2>&1 || actual=$?
        fi
        if [ "$gate" = ci ] && [ "$total" -gt 0 ]; then
            status=PASS; [ "$expected" -eq 0 ] || status=FAIL
            grep -q "|$status\$" "$fixture/results/pkg_summary.txt" || failures=$((failures + 1))
        fi
        if [ "$actual" -ne "$expected" ]; then
            echo "FAIL $gate $covered/$total: expected $expected, got $actual"
            failures=$((failures + 1))
        fi
        if [ "$gate" = ci ] && [ "$covered" = 94999 ]; then
            grep -q 'Global Go Backend statement coverage (95.00%) is below threshold' "$fixture/log" || failures=$((failures + 1))
        fi
    done
done <<'CASES'
1902 2003 1
1903 2003 0
19 20 0
94999 100000 1
0 0 0
CASES

# Verify merge_coverage_profiles handles conflicting modes and output cleanup.
. "$fixture/scripts/lib/hermetic-go-env.sh"
prof_a="$fixture/prof_a.out"
prof_b="$fixture/prof_b.out"
prof_merged="$fixture/prof_merged.out"
printf 'mode: set\nexample/pkg/a.go:1.1,2.1 1 1\n' > "$prof_a"
printf 'mode: count\nexample/pkg/b.go:1.1,2.1 1 2\n' > "$prof_b"

merge_rc=0
merge_coverage_profiles "$prof_merged" "$prof_a" "$prof_b" 2>/dev/null || merge_rc=$?
if [ "$merge_rc" -eq 0 ]; then
    echo "FAIL: merge_coverage_profiles should return non-zero on conflicting modes"
    failures=$((failures + 1))
fi
if [ -e "$prof_merged" ]; then
    echo "FAIL: merge_coverage_profiles left output file on conflicting modes failure"
    failures=$((failures + 1))
fi

# Ensure existing output file is removed on conflicting modes failure.
touch "$prof_merged"
merge_rc=0
merge_coverage_profiles "$prof_merged" "$prof_a" "$prof_b" 2>/dev/null || merge_rc=$?
if [ "$merge_rc" -eq 0 ] || [ -e "$prof_merged" ]; then
    echo "FAIL: merge_coverage_profiles should fail and clean up existing output file on conflicting modes"
    failures=$((failures + 1))
fi

# Verify matching modes merge successfully into a non-empty profile.
printf 'mode: set\nexample/pkg/b.go:1.1,2.1 1 1\n' > "$prof_b"
if ! merge_coverage_profiles "$prof_merged" "$prof_a" "$prof_b" || [ ! -s "$prof_merged" ]; then
    echo "FAIL: merge_coverage_profiles failed on valid matching profiles"
    failures=$((failures + 1))
fi

[ "$failures" -eq 0 ] && echo 'Coverage boundary regressions passed.'
