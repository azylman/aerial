package guard

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/sanitizer"
)

const zeroSHA = "0000000000000000000000000000000000000000"

// fixtureObj is one pack entry; baseIdx >= 0 makes it an OFS delta against
// that earlier entry and baseSHA makes it a REF delta.
type fixtureObj struct {
	kind    int
	data    []byte
	baseIdx int
	baseSHA string
	rawZ    []byte // overrides the zlib stream written for data
}

func lit(kind int, data string) fixtureObj {
	return fixtureObj{kind: kind, data: []byte(data), baseIdx: -1}
}

func refDelta(base string, delta []byte) fixtureObj {
	return fixtureObj{kind: packRefDelta, data: delta, baseIdx: -1, baseSHA: base}
}

func ofsDelta(baseIdx int, delta []byte) fixtureObj {
	return fixtureObj{kind: packOfsDelta, data: delta, baseIdx: baseIdx}
}

func fixtureSHA(objType, data string) string {
	h := sha1.New()
	fmt.Fprintf(h, "%s %d\x00%s", objType, len(data), data)
	return hex.EncodeToString(h.Sum(nil))
}

func treeOf(entries ...string) string {
	var b strings.Builder
	for i := 0; i+2 < len(entries); i += 3 {
		raw, err := hex.DecodeString(entries[i+2])
		if err != nil {
			panic(err)
		}
		b.WriteString(entries[i] + " " + entries[i+1] + "\x00" + string(raw))
	}
	return b.String()
}

func commitOf(tree, msg string) string {
	return "tree " + tree + "\nauthor T <t@example.com> 0 +0000\ncommitter T <t@example.com> 0 +0000\n\n" + msg + "\n"
}

func varint(n int) []byte {
	var out []byte
	for {
		c := byte(n & 0x7f)
		n >>= 7
		if n == 0 {
			return append(out, c)
		}
		out = append(out, c|0x80)
	}
}

func deltaOf(baseSize, resultSize int, ops ...[]byte) []byte {
	out := append(varint(baseSize), varint(resultSize)...)
	for _, op := range ops {
		out = append(out, op...)
	}
	return out
}

func insertOp(s string) []byte { return append([]byte{byte(len(s))}, s...) }
func copyOp(off, size int) []byte {
	return []byte{0x80 | 0x01 | 0x10, byte(off), byte(size)}
}

func zlibBytes(data []byte) []byte {
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	_, _ = zw.Write(data)
	_ = zw.Close()
	return buf.Bytes()
}

func packOf(objs ...fixtureObj) []byte {
	var buf bytes.Buffer
	buf.WriteString("PACK")
	buf.Write([]byte{0, 0, 0, 2, 0, 0, 0, byte(len(objs))})
	offsets := make([]int, len(objs))
	for i, o := range objs {
		offsets[i] = buf.Len()
		size := len(o.data)
		c := byte(o.kind<<4) | byte(size&0x0f)
		size >>= 4
		for size > 0 {
			buf.WriteByte(c | 0x80)
			c = byte(size & 0x7f)
			size >>= 7
		}
		buf.WriteByte(c)
		switch {
		case o.baseIdx >= 0:
			buf.Write(ofsBytes(offsets[i] - offsets[o.baseIdx]))
		case o.baseSHA != "":
			raw, err := hex.DecodeString(o.baseSHA)
			if err != nil {
				panic(err)
			}
			buf.Write(raw)
		}
		if o.rawZ != nil {
			buf.Write(o.rawZ)
		} else {
			buf.Write(zlibBytes(o.data))
		}
	}
	buf.Write(make([]byte, 20))
	return buf.Bytes()
}

func ofsBytes(off int) []byte {
	out := []byte{byte(off & 0x7f)}
	for off >>= 7; off > 0; off >>= 7 {
		off--
		out = append([]byte{0x80 | byte(off&0x7f)}, out...)
	}
	return out
}

func pkt(s string) string { return fmt.Sprintf("%04x%s", len(s)+4, s) }

func pushBodyRef(ref string, objs ...fixtureObj) []byte {
	head := pkt(zeroSHA+" "+strings.Repeat("a", 40)+" "+ref+"\x00report-status side-band-64k ofs-delta\n") + "0000"
	return append([]byte(head), packOf(objs...)...)
}

func pushBody(objs ...fixtureObj) []byte {
	return pushBodyRef("refs/heads/main", objs...)
}

type fakeFetcher map[string]string

func (f fakeFetcher) Fetch(_ context.Context, owner, repo, sha string) (string, []byte, error) {
	if owner != "acme" || repo != "widgets" {
		return "", nil, errors.New("wrong repo")
	}
	data, ok := f[sha]
	if !ok {
		return "", nil, errors.New("not found")
	}
	return "blob", []byte(data), nil
}

// literalFilePush returns blob + nested trees + commit for one file at path.
func literalFilePush(path, content, msg string) []fixtureObj {
	blobSHA := fixtureSHA("blob", content)
	objs := []fixtureObj{lit(packBlob, content)}
	parts := strings.Split(path, "/")
	tree := treeOf("100644", parts[len(parts)-1], blobSHA)
	objs = append(objs, lit(packTree, tree))
	for i := len(parts) - 2; i >= 0; i-- {
		tree = treeOf("40000", parts[i], fixtureSHA("tree", tree))
		objs = append(objs, lit(packTree, tree))
	}
	return append(objs, lit(packCommit, commitOf(fixtureSHA("tree", tree), msg)))
}

// sharedTreePush pushes one blob as hosts.yaml inside an identical subtree
// mounted at two directories (each a single path component under a parent).
func sharedTreePush(content, dirA, dirB string) []fixtureObj {
	blobSHA := fixtureSHA("blob", content)
	sub := treeOf("100644", "hosts.yaml", blobSHA)
	subSHA := fixtureSHA("tree", sub)
	objs := []fixtureObj{lit(packBlob, content), lit(packTree, sub)}
	parentA, nameA, okA := strings.Cut(dirA, "/")
	parentB, nameB, okB := strings.Cut(dirB, "/")
	if okA != okB {
		panic("sharedTreePush: mismatched dir depths")
	}
	var root string
	if nameA == "" {
		root = treeOf("40000", parentA, subSHA, "40000", parentB, subSHA)
	} else {
		if !okA || !okB {
			panic("sharedTreePush: expected okA and okB")
		}
		mid := treeOf("40000", nameA, subSHA, "40000", nameB, subSHA)
		objs = append(objs, lit(packTree, mid))
		root = treeOf("40000", parentA, fixtureSHA("tree", mid))
	}
	objs = append(objs, lit(packTree, root))
	return append(objs, lit(packCommit, commitOf(fixtureSHA("tree", root), "add")))
}

func deltaFilePush(name, result string, delta fixtureObj, extra ...fixtureObj) []fixtureObj {
	tree := treeOf("100644", name, fixtureSHA("blob", result))
	objs := make([]fixtureObj, 0, len(extra)+3)
	objs = append(objs, extra...)
	objs = append(objs, delta, lit(packTree, tree))
	return append(objs, lit(packCommit, commitOf(fixtureSHA("tree", tree), "update")))
}

const ghpFixture = "ghp_" + "abcdefghijklmnopqrstuvwxyz0123456789"

type scanReceivePackCase struct {
	name    string
	body    []byte
	policy  PushScanPolicy
	fetch   ObjectFetcher
	want    []Violation
	wantErr string
}

func TestScanReceivePack(t *testing.T) {
	sanitizer.ResetSensitiveTokensForTest()
	sanitizer.RegisterSensitiveTokens("dummy-push-secret-value")
	t.Cleanup(sanitizer.ResetSensitiveTokensForTest)

	public := PushScanPolicy{PublicRepo: true, Owner: "acme", Repo: "widgets"}
	private := PushScanPolicy{Owner: "acme", Repo: "widgets"}
	allowCfg := PushScanPolicy{PublicRepo: true, Owner: "acme", Repo: "widgets", RegexAllowlist: []string{"cfg/"}}
	allowAll := PushScanPolicy{PublicRepo: true, Owner: "acme", Repo: "widgets", RegexAllowlist: []string{"secret"}}
	fetcher := fakeFetcher{fixtureSHA("blob", "alpha\n"): "alpha\n"}

	spanDelta := refDelta(fixtureSHA("blob", "alpha\n"), deltaOf(6, 18, copyOp(0, 6), insertOp("db=10.9."), insertOp("8.7\n")))
	ofsBase := lit(packBlob, "base\n")
	ofsResult := "base\nk=192.168.4.4\n"

	submoduleTree := treeOf("160000", "mod", strings.Repeat("1", 40), "40000", "old", strings.Repeat("2", 40), "100644", "a.txt", fixtureSHA("blob", "ip 10.5.5.5\n"))

	tests := append(
		scanReceivePackSecretCases(public, private, allowCfg, allowAll, fetcher, spanDelta, ofsBase, ofsResult, submoduleTree),
		scanReceivePackHygieneCases(private)...,
	)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ScanReceivePackContext(context.Background(), bytes.NewReader(tc.body), tc.policy, tc.fetch)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for i := range got {
				got[i].Pattern = ""
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("violations = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func scanReceivePackSecretCases(public, private, allowCfg, allowAll PushScanPolicy, fetcher fakeFetcher, spanDelta, ofsBase fixtureObj, ofsResult, submoduleTree string) []scanReceivePackCase {
	rawPack := func(tail string) []byte {
		return []byte(pkt(zeroSHA+" "+zeroSHA+" refs/heads/x\n") + "0000PACK\x00\x00\x00\x02" + tail)
	}

	return []scanReceivePackCase{
		{
			name: "literal blob private ip", body: pushBody(literalFilePush("cfg/hosts.yaml", "hosts:\n  db: 10.20.30.40\n", "add hosts")...), policy: public,
			want: []Violation{{Category: "PrivateIPv4", File: "cfg/hosts.yaml", LineNum: 2, Sample: "10.20.30.40"}},
		},
		{
			name: "delta insert spanning two insert ops", body: pushBody(deltaFilePush("app.env", "alpha\ndb=10.9.8.7\n", spanDelta)...), policy: public, fetch: fetcher,
			want: []Violation{{Category: "PrivateIPv4", File: "app.env", LineNum: 2, Sample: "10.9.8.7"}},
		},
		{
			name: "ofs delta against in-pack base", body: pushBody(deltaFilePush("n.txt", ofsResult, ofsDelta(0, deltaOf(5, len(ofsResult), copyOp(0, 5), insertOp("k=192.168.4.4\n"))), ofsBase)...), policy: public,
			want: []Violation{{Category: "PrivateIPv4", File: "n.txt", LineNum: 2, Sample: "192.168.4.4"}},
		},
		{
			name: "commit message hit", body: pushBody(literalFilePush("README.md", "hi\n", "rotate "+ghpFixture)...), policy: public,
			want: []Violation{{Category: "GitHubPAT", File: "commit 050b8d2253f6fe4d510d8d6c88431500374086ee", LineNum: 5, Sample: "gh***89"}},
		},
		{
			name: "tree filename hit", body: pushBody(literalFilePush("10.1.1.1.txt", "ok\n", "add")...), policy: public,
			want: []Violation{{Category: "PrivateIPv4", File: "10.1.1.1.txt", Sample: "10.1.1.1"}},
		},
		{
			name: "value hit in private repo", body: pushBody(literalFilePush("secret.txt", "a\ntoken: dummy-push-secret-value\n", "add")...), policy: private,
			want: []Violation{{Category: "InfisicalSecret", File: "secret.txt", LineNum: 2, Sample: "du***ue"}},
		},
		{
			name: "value hit under allowlisted path kept", body: pushBody(literalFilePush("secret.txt", "dummy-push-secret-value\n", "add")...), policy: allowAll,
			want: []Violation{{Category: "InfisicalSecret", File: "secret.txt", LineNum: 1, Sample: "du***ue"}},
		},
		{name: "ip hit ignored for private repo", body: pushBody(literalFilePush("cfg/hosts.yaml", "db: 10.20.30.40\n", "add")...), policy: private},
		{name: "test-net address ignored", body: pushBody(literalFilePush("docs/net.md", "host: 192.0.2.10\n", "add")...), policy: public},
		{name: "allowlisted path dropped", body: pushBody(literalFilePush("cfg/hosts.yaml", "db: 10.20.30.40\n", "add")...), policy: allowCfg},
		{
			name: "shared tree at allowlisted and other path kept", body: pushBody(sharedTreePush("db: 10.20.30.40\n", "cfg", "docs")...), policy: allowCfg,
			want: []Violation{{Category: "PrivateIPv4", File: "docs/hosts.yaml", LineNum: 1, Sample: "10.20.30.40"}},
		},
		{name: "shared tree only at allowlisted paths dropped", body: pushBody(sharedTreePush("db: 10.20.30.40\n", "cfg/a", "cfg/b")...), policy: allowCfg},
		{
			name: "unresolvable path kept", body: pushBody(lit(packBlob, "gw 172.16.5.4\n")), policy: allowCfg,
			want: []Violation{{Category: "PrivateIPv4", File: "blob a862ef04af87a98b9bbde4087bf4b1463952d013", LineNum: 1, Sample: "172.16.5.4"}},
		},
		{
			name: "missing external base keeps violation", body: pushBody(refDelta(strings.Repeat("b", 40), deltaOf(6, 18, copyOp(0, 6), insertOp("db=10.9.8.7\n")))), policy: allowCfg, fetch: fetcher,
			want: []Violation{{Category: "PrivateIPv4", File: "unresolved object at pack offset 12", Sample: "10.9.8.7"}},
		},
		{name: "binary blob skips regex", body: pushBody(lit(packBlob, "\x00\x01 10.2.3.4")), policy: public},
		{
			name: "ref delta against in-pack delta", body: pushBody(deltaFilePush("c.txt", "base\nmore\nip 10.7.7.7\n",
				refDelta(fixtureSHA("blob", "base\nmore\n"), deltaOf(10, 22, copyOp(0, 10), insertOp("ip 10.7.7.7\n"))),
				ofsBase, ofsDelta(0, deltaOf(5, 10, copyOp(0, 5), insertOp("more\n"))))...), policy: public,
			want: []Violation{{Category: "PrivateIPv4", File: "c.txt", LineNum: 3, Sample: "10.7.7.7"}},
		},
		{
			name: "secret filename masked in tree hit", body: pushBody(literalFilePush("dummy-push-secret-value.txt", "x\n", "add")...), policy: private,
			want: []Violation{{Category: "InfisicalSecret", File: "[REDACTED].txt", Sample: "du***ue"}},
		},
		{
			name: "delta base size mismatch stays unresolved", body: pushBody(refDelta(fixtureSHA("blob", "alpha\n"), deltaOf(99, 12, insertOp("db=10.9.8.7\n")))), policy: allowCfg,
			want: []Violation{{Category: "PrivateIPv4", File: "unresolved object at pack offset 12", Sample: "10.9.8.7"}},
		},
		{name: "unknown object type", body: pushBody(fixtureObj{kind: 5, data: []byte("x"), baseIdx: -1}), policy: public, wantErr: "unknown pack object type 5"},
		{name: "unsupported pack version", body: []byte(pkt(zeroSHA+" "+zeroSHA+" refs/heads/x\n") + "0000PACK\x00\x00\x00\x09\x00\x00\x00\x00"), policy: public, wantErr: "unsupported pack version 9"},
		{name: "corrupt zlib stream", body: pushBody(fixtureObj{kind: packBlob, data: []byte("hello"), baseIdx: -1, rawZ: []byte("nope")}), policy: public, wantErr: "inflate object at offset 12"},
		{name: "inflated size mismatch", body: pushBody(fixtureObj{kind: packBlob, data: []byte("hello"), baseIdx: -1, rawZ: zlibBytes([]byte("hi"))}), policy: public, wantErr: "header declared 5"},
		{name: "truncated delta insert", body: pushBody(ofsBase, ofsDelta(0, deltaOf(5, 5, []byte{10, 'a'}))), policy: public, wantErr: "truncated delta insert"},
		{name: "reserved delta opcode", body: pushBody(ofsBase, ofsDelta(0, deltaOf(5, 5, []byte{0}))), policy: public, wantErr: "reserved delta opcode 0"},
		{name: "ofs delta pointing at itself", body: pushBody(fixtureObj{kind: packOfsDelta, data: deltaOf(1, 1), baseIdx: 0}), policy: public, wantErr: "invalid delta base offset"},
		{
			name: "tag message hit", body: pushBody(lit(packTag, "object "+zeroSHA+"\ntype commit\ntag v1\ntagger T <t@example.com> 0 +0000\n\nrelease at 10.3.3.3\n")), policy: public,
			want: []Violation{{Category: "PrivateIPv4", File: "tag a27f2b6fd97b436ac3bcf040013c5532763b5484", LineNum: 6, Sample: "10.3.3.3"}},
		},
		{
			name: "submodule and unchanged subtree skipped in path walk", body: pushBody(lit(packBlob, "ip 10.5.5.5\n"), lit(packTree, submoduleTree), lit(packCommit, commitOf(fixtureSHA("tree", submoduleTree), "add"))), policy: public,
			want: []Violation{{Category: "PrivateIPv4", File: "a.txt", LineNum: 1, Sample: "10.5.5.5"}},
		},
		{name: "pack object count limit", body: rawPack("\xff\xff\xff\xff"), policy: public, wantErr: "pack declares"},
		{name: "truncated object header", body: rawPack("\x00\x00\x00\x01"), policy: public, wantErr: "read object header"},
		{name: "truncated object size", body: rawPack("\x00\x00\x00\x01\xb0"), policy: public, wantErr: "read object size"},
		{name: "truncated ofs delta offset", body: rawPack("\x00\x00\x00\x01\x61"), policy: public, wantErr: "read delta offset"},
		{name: "truncated multi-byte ofs offset", body: rawPack("\x00\x00\x00\x01\x61\x81"), policy: public, wantErr: "read delta offset"},
		{name: "non-hex pkt-line length", body: []byte("zzzz"), policy: public, wantErr: "invalid pkt-line length"},
		{name: "pkt-line length out of range", body: []byte("0002"), policy: public, wantErr: "out of range"},
		{name: "truncated ref delta base", body: rawPack("\x00\x00\x00\x01\x71abc"), policy: public, wantErr: "read delta base id"},
		{name: "delete-only push has no pack", body: []byte(pkt(strings.Repeat("a", 40)+" "+zeroSHA+" refs/heads/old\x00report-status\n") + "0000"), policy: public},
		{name: "malformed pack header", body: []byte(pkt(zeroSHA+" "+zeroSHA+" refs/heads/x\n") + "0000JUNKJUNKJUNK"), policy: public, wantErr: "missing PACK header"},
		{name: "truncated pkt-line", body: []byte("00"), policy: public, wantErr: "truncated pkt-line header"},
		{name: "malformed command", body: []byte(pkt("garbage\n") + "0000"), policy: public, wantErr: "malformed receive-pack command"},
	}
}

func scanReceivePackHygieneCases(private PushScanPolicy) []scanReceivePackCase {
	return []scanReceivePackCase{
		{
			name:   "direct push to refs/heads/main rejected",
			body:   pushBody(literalFilePush("clean.txt", "clean\n", "add")...),
			policy: PushScanPolicy{BlockMainPush: true, Owner: "acme", Repo: "widgets"},
			want:   []Violation{{Category: "DirectMainPush", File: "refs/heads/main", Sample: "direct push to refs/heads/main is prohibited"}},
		},
		{
			name:   "committed PR_DESCRIPTION.md rejected without secrets",
			body:   pushBody(literalFilePush("PR_DESCRIPTION.md", "title\n\nbody\n", "add pr desc")...),
			policy: private,
			want:   []Violation{{Category: "TransientPRDescriptionFile", File: "PR_DESCRIPTION.md", Sample: "PR_DESCRIPTION.md"}},
		},
		{
			name:   "committed .pr_description.md rejected without secrets",
			body:   pushBody(literalFilePush(".pr_description.md", "title\n\nbody\n", "add pr desc")...),
			policy: private,
			want:   []Violation{{Category: "TransientPRDescriptionFile", File: ".pr_description.md", Sample: ".pr_description.md"}},
		},
		{
			name:   "oversized GEMINI.md rejected",
			body:   pushBody(literalFilePush("GEMINI.md", strings.Repeat("x", 23041), "add gemini")...),
			policy: private,
			want:   []Violation{{Category: "RuleFileSizeCeiling", File: "GEMINI.md", Sample: "23041 bytes"}},
		},
		{
			name:   "oversized AGENTS.md rejected",
			body:   pushBody(literalFilePush("AGENTS.md", strings.Repeat("x", 23041), "add agents")...),
			policy: private,
			want:   []Violation{{Category: "RuleFileSizeCeiling", File: "AGENTS.md", Sample: "23041 bytes"}},
		},
		{
			name:   "GEMINI.md within limit allowed",
			body:   pushBody(literalFilePush("GEMINI.md", strings.Repeat("x", 23040), "add gemini")...),
			policy: private,
		},
		{
			name:   "nested GEMINI.md oversized allowed",
			body:   pushBody(literalFilePush("docs/GEMINI.md", strings.Repeat("x", 23041), "add nested gemini")...),
			policy: private,
		},
		{
			name:   "feature branch push with BlockMainPush allowed",
			body:   pushBodyRef("refs/heads/feat/patch", literalFilePush("clean.txt", "clean\n", "add")...),
			policy: PushScanPolicy{BlockMainPush: true, Owner: "acme", Repo: "widgets"},
		},
	}
}

func TestParseReceivePackRequest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		body     string
		wantRefs string
		wantCaps string
		wantPack string
	}{
		{
			name: "commands and caps", body: pkt(zeroSHA+" "+zeroSHA+" refs/heads/a\x00report-status side-band-64k\n") + pkt(zeroSHA+" "+zeroSHA+" refs/heads/b\n") + "0000PACK",
			wantRefs: "refs/heads/a,refs/heads/b", wantCaps: "report-status,side-band-64k", wantPack: "PACK",
		},
		{
			name: "shallow lines and push options skipped", body: pkt("shallow "+zeroSHA+"\n") + pkt(zeroSHA+" "+zeroSHA+" refs/heads/a\x00push-options\n") + "0000" + pkt("ci.skip\n") + "0000PACK",
			wantRefs: "refs/heads/a", wantCaps: "push-options", wantPack: "PACK",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req, err := ParseReceivePackRequest([]byte(tc.body))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			refs := make([]string, len(req.Commands))
			for i, c := range req.Commands {
				refs[i] = c.Ref
			}
			if strings.Join(refs, ",") != tc.wantRefs || strings.Join(req.Capabilities, ",") != tc.wantCaps || string(req.Pack) != tc.wantPack {
				t.Fatalf("got refs=%v caps=%v pack=%q", refs, req.Capabilities, req.Pack)
			}
		})
	}
}

func TestScanPayload(t *testing.T) {
	sanitizer.ResetSensitiveTokensForTest()
	sanitizer.RegisterSensitiveTokens("dummy-payload-secret")
	t.Cleanup(sanitizer.ResetSensitiveTokensForTest)

	public := PushScanPolicy{PublicRepo: true, RegexAllowlist: []string{"testdata/"}}
	tests := []struct {
		name     string
		data     string
		filePath string
		policy   PushScanPolicy
		want     []Violation
	}{
		{name: "value hit on private repo", data: `{"body":"dummy-payload-secret"}`, want: []Violation{{Category: "InfisicalSecret", File: "request body", LineNum: 1, Sample: "du***et"}}},
		{name: "regex hit on public repo", data: "line\nssh 10.4.4.4\n", filePath: "docs/a.md", policy: public, want: []Violation{{Category: "PrivateIPv4", File: "docs/a.md", LineNum: 2, Sample: "10.4.4.4"}}},
		{name: "regex ignored on private repo", data: "ssh 10.4.4.4", filePath: "docs/a.md"},
		{name: "allowlisted file path drops regex", data: "ssh 10.4.4.4", filePath: "testdata/x.txt", policy: public},
		{name: "allowlist never drops value hit", data: "dummy-payload-secret", filePath: "testdata/x.txt", policy: public, want: []Violation{{Category: "InfisicalSecret", File: "testdata/x.txt", LineNum: 1, Sample: "du***et"}}},
		{name: "allowed secret sample drops value hit on request body", data: `{"body":"dummy-payload-secret"}`, policy: PushScanPolicy{AllowedSecretSamples: []string{"du***et"}}},
		{name: "allowed secret sample never drops value hit for file payload", data: "dummy-payload-secret", filePath: "docs/key.md", policy: PushScanPolicy{AllowedSecretSamples: []string{"du***et"}}, want: []Violation{{Category: "InfisicalSecret", File: "docs/key.md", LineNum: 1, Sample: "du***et"}}},
		{name: "non-matching allowed secret sample retains value hit on request body", data: `{"body":"dummy-payload-secret"}`, policy: PushScanPolicy{AllowedSecretSamples: []string{"ot***er"}}, want: []Violation{{Category: "InfisicalSecret", File: "request body", LineNum: 1, Sample: "du***et"}}},
		{name: "binary payload skips regex", data: "\x00 10.4.4.4", filePath: "img.bin", policy: public},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ScanPayload([]byte(tc.data), tc.filePath, tc.policy)
			for i := range got {
				got[i].Pattern = ""
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("violations = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestApplyDelta(t *testing.T) {
	t.Parallel()
	big := strings.Repeat("a", 0x10000)
	tests := []struct {
		name    string
		base    string
		delta   []byte
		want    string
		wantErr string
	}{
		{name: "copy and insert", base: "hello", delta: deltaOf(5, 8, copyOp(0, 5), insertOp("!!!")), want: "hello!!!"},
		{name: "zero copy size means 64KiB", base: big, delta: append(deltaOf(0x10000, 0x10000), 0x80), want: big},
		{name: "empty delta", base: "x", wantErr: "truncated delta size"},
		{name: "missing result size", base: "x", delta: varint(1), wantErr: "truncated delta size"},
		{name: "base size mismatch", base: "x", delta: deltaOf(2, 1), wantErr: "delta base size 2"},
		{name: "reserved insert opcode", base: "x", delta: deltaOf(1, 1, []byte{0}), wantErr: "invalid delta insert"},
		{name: "copy out of range", base: "x", delta: deltaOf(1, 4, copyOp(0, 4)), wantErr: "delta copy out of range"},
		{name: "truncated copy", base: "x", delta: deltaOf(1, 1, []byte{0x91, 0}), wantErr: "truncated delta copy"},
		{name: "result size mismatch", base: "x", delta: deltaOf(1, 5, copyOp(0, 1)), wantErr: "delta produced 1 bytes, want 5"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := applyDelta([]byte(tc.base), tc.delta)
			if (err == nil) != (tc.wantErr == "") || (err != nil && !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
			if string(got) != tc.want {
				t.Fatalf("got %d bytes, want %d", len(got), len(tc.want))
			}
		})
	}
}

func TestDeltaInsertRuns(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		delta   []byte
		want    string
		wantErr string
	}{
		{name: "copy splits runs", delta: deltaOf(3, 6, insertOp("ab"), insertOp("c"), copyOp(0, 2), insertOp("d")), want: "[abc d]"},
		{name: "empty delta", wantErr: "truncated delta size"},
		{name: "truncated copy", delta: deltaOf(1, 1, []byte{0x91}), wantErr: "truncated delta copy"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runs, err := deltaInsertRuns(tc.delta)
			if (err == nil) != (tc.wantErr == "") || (err != nil && !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
			if tc.wantErr == "" && fmt.Sprintf("%s", runs) != tc.want {
				t.Fatalf("runs = %s, want %s", runs, tc.want)
			}
		})
	}
}

func TestTreeNames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		data string
		want string
	}{
		{name: "entry names joined", data: treeOf("100644", "a.txt", strings.Repeat("1", 40), "40000", "dir", strings.Repeat("2", 40)), want: "a.txt\ndir"},
		{name: "malformed tree falls back to raw text", data: "abc\x00def", want: "abc\ndef"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := treeNames([]byte(tc.data)); got != tc.want {
				t.Fatalf("treeNames = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestIsZeroOID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		oid  string
		want bool
	}{
		{name: "all zeros", oid: "0000000000000000000000000000000000000000", want: true},
		{name: "non-zero sha", oid: "050b8d2253f6fe4d510d8d6c88431500374086ee", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isZeroOID(tc.oid); got != tc.want {
				t.Fatalf("isZeroOID(%q) = %v, want %v", tc.oid, got, tc.want)
			}
		})
	}
}
