package guard

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1" //nolint:gosec // git object IDs are SHA-1 by protocol, not a security primitive here.
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/azylman/aerial/pkg/sanitizer"
)

// Push scan categories and limits.
const (
	categoryRegisteredSecret       = "InfisicalSecret"
	patternRegisteredSecret        = "registered_secret"
	categoryPrivateIPv4            = "PrivateIPv4"
	categoryRFC1918                = "RFC1918_192_168"
	categoryDirectMainPush         = "DirectMainPush"
	categoryTransientPRDescription = "TransientPRDescriptionFile"
	categoryRuleFileSizeCeiling    = "RuleFileSizeCeiling"
	maxRuleFileBytes               = 23040
	refMain                        = "refs/heads/main"

	objBlob   = "blob"
	objTree   = "tree"
	objCommit = "commit"

	maxPackObjects       = 1 << 20
	maxInflatedPackBytes = 1 << 30
	maxDeltaDepth        = 64
	binarySniffBytes     = 8000
)

// Git pack object type codes.
const (
	packCommit   = 1
	packTree     = 2
	packBlob     = 3
	packTag      = 4
	packOfsDelta = 6
	packRefDelta = 7
)

// PrivateIPv4Pattern matches RFC1918 IPv4 literals (10/8, 172.16/12, 192.168/16).
var PrivateIPv4Pattern = PatternRule{
	Category: categoryPrivateIPv4,
	Regex:    regexp.MustCompile(`\b(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3})\b`),
}

// PushScanPolicy controls which rules apply to a push or GitHub API write.
// Registered secret values are matched on every repository; regex rules run
// only when PublicRepo is set, minus hits under a RegexAllowlist path prefix.
type PushScanPolicy struct {
	PublicRepo     bool
	RegexAllowlist []string
	BlockMainPush  bool
	// Owner and Repo identify the destination repository for ObjectFetcher lookups.
	Owner string
	Repo  string
}

func isZeroOID(oid string) bool {
	return strings.Trim(strings.TrimSpace(oid), "0") == ""
}

// ObjectFetcher loads an existing git object from the destination repository.
// objType is one of "blob", "tree", "commit" or "tag".
type ObjectFetcher interface {
	Fetch(ctx context.Context, owner, repo, sha string) (objType string, data []byte, err error)
}

// RefCommand is one ref update line of a git-receive-pack request.
type RefCommand struct {
	Old string
	New string
	Ref string
}

// ReceivePackRequest is the parsed command section of a git-receive-pack body.
type ReceivePackRequest struct {
	Commands     []RefCommand
	Capabilities []string
	Pack         []byte
}

// HasCapability reports whether the client requested capability name.
func (r *ReceivePackRequest) HasCapability(name string) bool {
	for _, c := range r.Capabilities {
		if c == name || strings.HasPrefix(c, name+"=") {
			return true
		}
	}
	return false
}

// PushScanRules returns the regex rules applied to public repositories:
// BuiltinPatterns without the hostname and 192.168-only rules, plus PrivateIPv4.
func PushScanRules() []PatternRule {
	rules := make([]PatternRule, 0, len(BuiltinPatterns))
	for _, p := range BuiltinPatterns {
		if p.Category == categoryInternalClusterDomain || p.Category == categoryRFC1918 {
			continue
		}
		rules = append(rules, p)
	}
	return append(rules, PrivateIPv4Pattern)
}

// ParseReceivePackRequest splits a git-receive-pack body into ref commands,
// requested capabilities and the trailing packfile (empty for delete-only pushes).
func ParseReceivePackRequest(body []byte) (*ReceivePackRequest, error) {
	req := &ReceivePackRequest{}
	pos := 0
	first := true
	for {
		line, next, flush, err := readPktLine(body, pos)
		if err != nil {
			return nil, err
		}
		pos = next
		if flush {
			break
		}
		line = strings.TrimSuffix(line, "\n")
		if strings.HasPrefix(line, "shallow ") {
			continue
		}
		if first {
			if cmd, caps, ok := strings.Cut(line, "\x00"); ok {
				line = cmd
				req.Capabilities = strings.Fields(caps)
			}
			first = false
		}
		parts := strings.SplitN(line, " ", 3)
		if len(parts) != 3 {
			return nil, fmt.Errorf("malformed receive-pack command %q", line)
		}
		req.Commands = append(req.Commands, RefCommand{Old: parts[0], New: parts[1], Ref: parts[2]})
	}
	if req.HasCapability("push-options") {
		for {
			_, next, flush, err := readPktLine(body, pos)
			if err != nil {
				return nil, err
			}
			pos = next
			if flush {
				break
			}
		}
	}
	req.Pack = body[pos:]
	return req, nil
}

func readPktLine(body []byte, pos int) (line string, next int, flush bool, err error) {
	if pos+4 > len(body) {
		return "", pos, false, errors.New("truncated pkt-line header")
	}
	n, err := strconv.ParseUint(string(body[pos:pos+4]), 16, 16)
	if err != nil {
		return "", pos, false, fmt.Errorf("invalid pkt-line length: %w", err)
	}
	if n == 0 {
		return "", pos + 4, true, nil
	}
	if n < 4 || pos+int(n) > len(body) {
		return "", pos, false, fmt.Errorf("pkt-line length %d out of range", n)
	}
	return string(body[pos+4 : pos+int(n)]), pos + int(n), false, nil
}

// packObject is one raw entry of a packfile.
type packObject struct {
	offset     int64
	kind       int
	data       []byte // literal content, or delta instructions
	baseOffset int64  // packOfsDelta
	baseSHA    string // packRefDelta
	sha        string // literal objects only (deltas fill it on resolve)
}

func parsePack(pack []byte) ([]packObject, error) {
	if len(pack) == 0 {
		return nil, nil
	}
	if len(pack) < 12 || string(pack[:4]) != "PACK" {
		return nil, errors.New("missing PACK header")
	}
	if v := binary.BigEndian.Uint32(pack[4:8]); v != 2 && v != 3 {
		return nil, fmt.Errorf("unsupported pack version %d", v)
	}
	count := binary.BigEndian.Uint32(pack[8:12])
	if count > maxPackObjects {
		return nil, fmt.Errorf("pack declares %d objects (limit %d)", count, maxPackObjects)
	}
	r := bytes.NewReader(pack)
	if _, err := r.Seek(12, io.SeekStart); err != nil {
		return nil, err
	}
	objs := make([]packObject, 0, count)
	var inflated int64
	for range count {
		obj, err := readPackObject(r, int64(len(pack)))
		if err != nil {
			return nil, err
		}
		inflated += int64(len(obj.data))
		if inflated > maxInflatedPackBytes {
			return nil, errors.New("pack inflates past the scan limit")
		}
		objs = append(objs, obj)
	}
	return objs, nil
}

func readPackObject(r *bytes.Reader, packLen int64) (packObject, error) {
	offset := packLen - int64(r.Len())
	c, err := r.ReadByte()
	if err != nil {
		return packObject{}, fmt.Errorf("read object header: %w", err)
	}
	obj := packObject{offset: offset, kind: int(c>>4) & 7}
	size := uint64(c & 0x0f)
	shift := uint(4)
	for c&0x80 != 0 {
		if c, err = r.ReadByte(); err != nil {
			return packObject{}, fmt.Errorf("read object size: %w", err)
		}
		size |= uint64(c&0x7f) << shift
		shift += 7
		if shift > 63 {
			return packObject{}, errors.New("object size overflow")
		}
	}
	if baseErr := readDeltaBase(r, &obj); baseErr != nil {
		return packObject{}, baseErr
	}
	if size > maxInflatedPackBytes {
		return packObject{}, fmt.Errorf("object at offset %d declares %d bytes", offset, size)
	}
	data, err := inflateObject(r, size)
	if err != nil {
		return packObject{}, fmt.Errorf("inflate object at offset %d: %w", offset, err)
	}
	obj.data = data
	if name := literalTypeName(obj.kind); name != "" {
		obj.sha = gitObjectSHA(name, data)
	}
	return obj, nil
}

func readDeltaBase(r *bytes.Reader, obj *packObject) error {
	switch obj.kind {
	case packOfsDelta:
		c, err := r.ReadByte()
		if err != nil {
			return fmt.Errorf("read delta offset: %w", err)
		}
		off := int64(c & 0x7f)
		for c&0x80 != 0 {
			if c, err = r.ReadByte(); err != nil {
				return fmt.Errorf("read delta offset: %w", err)
			}
			off = ((off + 1) << 7) | int64(c&0x7f)
		}
		obj.baseOffset = obj.offset - off
		if off <= 0 || obj.baseOffset < 12 {
			return fmt.Errorf("invalid delta base offset at %d", obj.offset)
		}
	case packRefDelta:
		var sha [20]byte
		if _, err := io.ReadFull(r, sha[:]); err != nil {
			return fmt.Errorf("read delta base id: %w", err)
		}
		obj.baseSHA = hex.EncodeToString(sha[:])
	case packCommit, packTree, packBlob, packTag:
	default:
		return fmt.Errorf("unknown pack object type %d at offset %d", obj.kind, obj.offset)
	}
	return nil
}

// inflateObject reads one zlib stream. bytes.Reader is an io.ByteReader, so the
// decompressor stops exactly at the end of the stream (checksum included).
func inflateObject(r *bytes.Reader, size uint64) ([]byte, error) {
	zr, err := zlib.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	data, err := io.ReadAll(io.LimitReader(zr, int64(size)+1))
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) != size {
		return nil, fmt.Errorf("inflated %d bytes, header declared %d", len(data), size)
	}
	return data, nil
}

func literalTypeName(kind int) string {
	switch kind {
	case packCommit:
		return objCommit
	case packTree:
		return objTree
	case packBlob:
		return objBlob
	case packTag:
		return "tag"
	}
	return ""
}

func gitObjectSHA(objType string, data []byte) string {
	h := sha1.New() //nolint:gosec // git object ID.
	h.Write([]byte(objType + " " + strconv.Itoa(len(data)) + "\x00"))
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

// deltaInsertRuns returns the concatenated bytes of each run of consecutive
// insert instructions; copy instructions end a run.
func deltaInsertRuns(delta []byte) ([][]byte, error) {
	pos, err := skipDeltaSizes(delta)
	if err != nil {
		return nil, err
	}
	var runs [][]byte
	var cur []byte
	for pos < len(delta) {
		op := delta[pos]
		pos++
		switch {
		case op&0x80 != 0:
			if len(cur) > 0 {
				runs = append(runs, cur)
				cur = nil
			}
			pos += bitsSet(op & 0x7f)
		case op == 0:
			return nil, errors.New("reserved delta opcode 0")
		default:
			if pos+int(op) > len(delta) {
				return nil, errors.New("truncated delta insert")
			}
			cur = append(cur, delta[pos:pos+int(op)]...)
			pos += int(op)
		}
	}
	if pos > len(delta) {
		return nil, errors.New("truncated delta copy")
	}
	if len(cur) > 0 {
		runs = append(runs, cur)
	}
	return runs, nil
}

func bitsSet(b byte) int {
	n := 0
	for ; b != 0; b &= b - 1 {
		n++
	}
	return n
}

func readDeltaVarint(delta []byte, pos int) (uint64, int, error) {
	var v uint64
	shift := uint(0)
	for {
		if pos >= len(delta) || shift > 63 {
			return 0, pos, errors.New("truncated delta size")
		}
		c := delta[pos]
		pos++
		v |= uint64(c&0x7f) << shift
		shift += 7
		if c&0x80 == 0 {
			return v, pos, nil
		}
	}
}

func skipDeltaSizes(delta []byte) (int, error) {
	_, pos, err := readDeltaVarint(delta, 0)
	if err != nil {
		return 0, err
	}
	_, pos, err = readDeltaVarint(delta, pos)
	return pos, err
}

// applyDelta rebuilds the target object from base and delta instructions.
func applyDelta(base, delta []byte) ([]byte, error) {
	baseSize, pos, err := readDeltaVarint(delta, 0)
	if err != nil {
		return nil, err
	}
	if baseSize != uint64(len(base)) {
		return nil, fmt.Errorf("delta base size %d, have %d", baseSize, len(base))
	}
	resultSize, pos, err := readDeltaVarint(delta, pos)
	if err != nil {
		return nil, err
	}
	if resultSize > maxInflatedPackBytes {
		return nil, errors.New("delta result too large")
	}
	out := make([]byte, 0, resultSize)
	for pos < len(delta) {
		op := delta[pos]
		pos++
		if op&0x80 == 0 {
			if op == 0 || pos+int(op) > len(delta) {
				return nil, errors.New("invalid delta insert")
			}
			out = append(out, delta[pos:pos+int(op)]...)
			pos += int(op)
			continue
		}
		var off, size uint64
		if off, size, pos, err = readCopyOp(delta, pos, op); err != nil {
			return nil, err
		}
		if off+size > uint64(len(base)) {
			return nil, errors.New("delta copy out of range")
		}
		out = append(out, base[off:off+size]...)
	}
	if uint64(len(out)) != resultSize {
		return nil, fmt.Errorf("delta produced %d bytes, want %d", len(out), resultSize)
	}
	return out, nil
}

func readCopyOp(delta []byte, pos int, op byte) (off, size uint64, next int, err error) {
	for i := range 7 {
		if op&(1<<i) == 0 {
			continue
		}
		if pos >= len(delta) {
			return 0, 0, pos, errors.New("truncated delta copy")
		}
		if i < 4 {
			off |= uint64(delta[pos]) << (8 * i)
		} else {
			size |= uint64(delta[pos]) << (8 * (i - 4))
		}
		pos++
	}
	if size == 0 {
		size = 0x10000
	}
	return off, size, pos, nil
}

// pushHit is one raw finding before location resolution.
type pushHit struct {
	obj      int
	regex    bool
	category string
	pattern  string
	raw      string // regex match text; never reported for secrets
	sample   string // masked sample reported to callers
}

func isBinary(data []byte) bool {
	return bytes.IndexByte(data[:min(len(data), binarySniffBytes)], 0) >= 0
}

func maskRegexSample(category, raw string) string {
	if category == categoryPrivateIPv4 {
		return raw
	}
	return sanitizer.MaskTokenSample(raw)
}

func valueHits(obj int, data []byte) []pushHit {
	samples := sanitizer.MatchSensitiveTokens(string(data))
	hits := make([]pushHit, 0, len(samples))
	for _, sample := range samples {
		hits = append(hits, pushHit{obj: obj, category: categoryRegisteredSecret, pattern: patternRegisteredSecret, sample: sample})
	}
	return hits
}

func regexHits(obj int, text string, rules []PatternRule) []pushHit {
	var hits []pushHit
	for _, p := range rules {
		seen := map[string]bool{}
		for _, m := range p.Regex.FindAllString(text, -1) {
			if seen[m] {
				continue
			}
			seen[m] = true
			hits = append(hits, pushHit{obj: obj, regex: true, category: p.Category, pattern: p.Regex.String(), raw: m, sample: maskRegexSample(p.Category, m)})
		}
	}
	return hits
}

// treeEntry is one entry of a raw git tree object.
type treeEntry struct {
	mode string
	name string
	sha  string
}

func parseTree(data []byte) ([]treeEntry, error) {
	var entries []treeEntry
	for len(data) > 0 {
		sp := bytes.IndexByte(data, ' ')
		nul := bytes.IndexByte(data, 0)
		if sp < 0 || nul < sp || nul+21 > len(data) {
			return nil, errors.New("malformed tree object")
		}
		entries = append(entries, treeEntry{
			mode: string(data[:sp]),
			name: string(data[sp+1 : nul]),
			sha:  hex.EncodeToString(data[nul+1 : nul+21]),
		})
		data = data[nul+21:]
	}
	return entries, nil
}

func treeNames(data []byte) string {
	entries, err := parseTree(data)
	if err != nil {
		return string(bytes.ReplaceAll(data, []byte{0}, []byte{'\n'}))
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.name
	}
	return strings.Join(names, "\n")
}

// scanObjectRaw scans one pack entry without resolving deltas.
func scanObjectRaw(i int, obj packObject, rules []PatternRule) ([]pushHit, error) {
	if obj.kind == packOfsDelta || obj.kind == packRefDelta {
		runs, err := deltaInsertRuns(obj.data)
		if err != nil {
			return nil, fmt.Errorf("delta at offset %d: %w", obj.offset, err)
		}
		var hits []pushHit
		for _, run := range runs {
			hits = append(hits, valueHits(i, run)...)
			for seg := range bytes.SplitSeq(run, []byte{0}) {
				hits = append(hits, regexHits(i, string(seg), rules)...)
			}
		}
		return hits, nil
	}
	hits := valueHits(i, obj.data)
	switch {
	case len(rules) == 0:
	case obj.kind == packTree:
		hits = append(hits, regexHits(i, treeNames(obj.data), rules)...)
	case obj.kind == packBlob && isBinary(obj.data):
	default:
		hits = append(hits, regexHits(i, string(obj.data), rules)...)
	}
	return hits, nil
}

func checkCommandHygiene(commands []RefCommand, policy PushScanPolicy) []Violation {
	var out []Violation
	for _, cmd := range commands {
		if !isZeroOID(cmd.New) && policy.BlockMainPush && cmd.Ref == refMain {
			out = append(out, Violation{
				Category: categoryDirectMainPush,
				Pattern:  refMain,
				Sample:   "direct push to refs/heads/main is prohibited",
				File:     refMain,
			})
		}
	}
	return out
}

func checkResolvedHygiene(res *packResolver, objs []packObject) []Violation {
	var out []Violation
	for i := range objs {
		ro, ok := res.resolved[i]
		if !ok || ro.typ != objBlob {
			continue
		}
		for _, p := range res.paths[i] {
			base := path.Base(p)
			if p == "PR_DESCRIPTION.md" || p == ".pr_description.md" || base == "PR_DESCRIPTION.md" || base == ".pr_description.md" {
				out = append(out, Violation{
					Category: categoryTransientPRDescription,
					Pattern:  p,
					Sample:   p,
					File:     p,
				})
			}
			if (p == "GEMINI.md" || p == "AGENTS.md") && len(ro.data) > maxRuleFileBytes {
				out = append(out, Violation{
					Category: categoryRuleFileSizeCeiling,
					Pattern:  "max_bytes=23040",
					Sample:   fmt.Sprintf("%d bytes", len(ro.data)),
					File:     p,
				})
			}
		}
	}
	return out
}

var hygieneTargetBytes = [][]byte{
	[]byte("PR_DESCRIPTION.md"),
	[]byte(".pr_description.md"),
	[]byte("GEMINI.md"),
	[]byte("AGENTS.md"),
}

func hasHygieneCandidates(objs []packObject) bool {
	for _, obj := range objs {
		switch obj.kind {
		case packTree, packOfsDelta, packRefDelta:
			for _, target := range hygieneTargetBytes {
				if bytes.Contains(obj.data, target) {
					return true
				}
			}
		}
	}
	return false
}

// ScanReceivePackContext scans a git-receive-pack request body (gzip already removed)
// and returns violations with file:line locations where resolvable; ctx bounds object fetches.
func ScanReceivePackContext(ctx context.Context, body io.Reader, policy PushScanPolicy, fetch ObjectFetcher) ([]Violation, error) {
	raw, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("read receive-pack body: %w", err)
	}
	req, err := ParseReceivePackRequest(raw)
	if err != nil {
		return nil, err
	}
	out := checkCommandHygiene(req.Commands, policy)
	objs, err := parsePack(req.Pack)
	if err != nil {
		return nil, err
	}
	if len(objs) == 0 {
		return dedupeViolations(out), nil
	}
	var rules []PatternRule
	if policy.PublicRepo {
		rules = PushScanRules()
	}
	var hits []pushHit
	for i, obj := range objs {
		h, err := scanObjectRaw(i, obj, rules)
		if err != nil {
			return nil, err
		}
		hits = append(hits, h...)
	}
	if len(hits) == 0 && !hasHygieneCandidates(objs) {
		return dedupeViolations(out), nil
	}
	res := newPackResolver(ctx, objs, policy, fetch)
	res.resolveAll()
	res.buildPaths()
	for _, h := range hits {
		if v, keep := res.locate(h, policy.RegexAllowlist); keep {
			// Paths and filenames can embed the secret itself.
			v.File = sanitizer.SanitizeString(v.File)
			out = append(out, v)
		}
	}
	out = append(out, checkResolvedHygiene(res, objs)...)
	return dedupeViolations(out), nil
}

func dedupeViolations(in []Violation) []Violation {
	seen := map[Violation]bool{}
	out := make([]Violation, 0, len(in))
	for _, v := range in {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

type resolvedObject struct {
	typ  string
	data []byte
	sha  string
}

// packResolver rebuilds delta objects (fetching external bases) and maps
// pushed blobs and trees back to repository paths.
type packResolver struct {
	ctx        context.Context //nolint:containedctx // scoped to one ScanReceivePack call.
	objs       []packObject
	policy     PushScanPolicy
	fetch      ObjectFetcher
	byOffset   map[int64]int
	bySHA      map[string]int
	resolved   map[int]resolvedObject
	inProgress map[int]bool
	external   map[string]resolvedObject
	blobPaths  map[string][]string // every pushed path referencing a blob
	treePaths  map[string][]string // every directory a pushed tree appears at
	paths      map[int][]string    // resolved blob paths by object index
	walked     map[string]bool     // tree sha + "\x00" + dir already walked
}

func newPackResolver(ctx context.Context, objs []packObject, policy PushScanPolicy, fetch ObjectFetcher) *packResolver {
	r := &packResolver{
		ctx: ctx, objs: objs, policy: policy, fetch: fetch,
		byOffset:   make(map[int64]int, len(objs)),
		bySHA:      make(map[string]int, len(objs)),
		resolved:   make(map[int]resolvedObject, len(objs)),
		inProgress: map[int]bool{},
		external:   map[string]resolvedObject{},
		blobPaths:  map[string][]string{},
		treePaths:  map[string][]string{},
		paths:      make(map[int][]string, len(objs)),
		walked:     map[string]bool{},
	}
	for i, o := range objs {
		r.byOffset[o.offset] = i
		if o.sha != "" {
			r.bySHA[o.sha] = i
			r.resolved[i] = resolvedObject{typ: literalTypeName(o.kind), data: o.data, sha: o.sha}
		}
	}
	return r
}

func (r *packResolver) resolveAll() {
	// Unresolvable objects simply stay out of r.resolved; locate reports
	// their hits by pack offset (fail closed).
	for i := range r.objs {
		if _, err := r.resolve(i, 0); err != nil {
			continue
		}
	}
}

func (r *packResolver) resolve(i, depth int) (resolvedObject, error) {
	if ro, ok := r.resolved[i]; ok {
		return ro, nil
	}
	if r.inProgress[i] || depth > maxDeltaDepth {
		return resolvedObject{}, errors.New("delta chain too deep or cyclic")
	}
	r.inProgress[i] = true
	defer delete(r.inProgress, i)
	obj := r.objs[i]
	base, err := r.deltaBase(obj, depth)
	if err != nil {
		return resolvedObject{}, err
	}
	data, err := applyDelta(base.data, obj.data)
	if err != nil {
		return resolvedObject{}, err
	}
	ro := resolvedObject{typ: base.typ, data: data, sha: gitObjectSHA(base.typ, data)}
	r.resolved[i] = ro
	r.bySHA[ro.sha] = i
	return ro, nil
}

func (r *packResolver) deltaBase(obj packObject, depth int) (resolvedObject, error) {
	if obj.kind == packOfsDelta {
		j, ok := r.byOffset[obj.baseOffset]
		if !ok {
			return resolvedObject{}, fmt.Errorf("no object at delta base offset %d", obj.baseOffset)
		}
		return r.resolve(j, depth+1)
	}
	if j, ok := r.bySHA[obj.baseSHA]; ok {
		return r.resolve(j, depth+1)
	}
	for j, o := range r.objs {
		if o.kind != packOfsDelta && o.kind != packRefDelta {
			continue
		}
		if _, err := r.resolve(j, depth+1); err != nil {
			continue
		}
		if k, ok := r.bySHA[obj.baseSHA]; ok {
			return r.resolve(k, depth+1)
		}
	}
	return r.fetchExternal(obj.baseSHA)
}

func (r *packResolver) fetchExternal(sha string) (resolvedObject, error) {
	if ro, ok := r.external[sha]; ok {
		return ro, nil
	}
	if r.fetch == nil {
		return resolvedObject{}, errors.New("no object fetcher for external delta base")
	}
	typ, data, err := r.fetch.Fetch(r.ctx, r.policy.Owner, r.policy.Repo, sha)
	if err != nil {
		return resolvedObject{}, fmt.Errorf("fetch delta base %s: %w", sha, err)
	}
	ro := resolvedObject{typ: typ, data: data, sha: sha}
	r.external[sha] = ro
	return ro, nil
}

// buildPaths walks every pushed commit's root tree through trees present in
// the pack. New blobs can only be reachable through new trees, so trees the
// server already has are not fetched.
func (r *packResolver) buildPaths() {
	idxs := make([]int, 0, len(r.resolved))
	for i := range r.resolved {
		idxs = append(idxs, i)
	}
	sort.Ints(idxs)
	for _, i := range idxs {
		ro := r.resolved[i]
		if ro.typ != objCommit {
			continue
		}
		if tree, ok := commitTree(ro.data); ok {
			r.walkTree(tree, "", 0)
		}
	}
	for i, ro := range r.resolved {
		if ro.typ == objBlob {
			r.paths[i] = r.blobPaths[ro.sha]
		}
	}
}

func commitTree(data []byte) (string, bool) {
	for line := range strings.SplitSeq(string(data), "\n") {
		if line == "" {
			break
		}
		if sha, ok := strings.CutPrefix(line, "tree "); ok {
			return strings.TrimSpace(sha), true
		}
	}
	return "", false
}

func (r *packResolver) walkTree(sha, dir string, depth int) {
	key := sha + "\x00" + dir
	if r.walked[key] || depth > 256 {
		return
	}
	r.walked[key] = true
	i, ok := r.bySHA[sha]
	if !ok {
		return
	}
	ro, ok := r.resolved[i]
	if !ok || ro.typ != objTree {
		return
	}
	r.treePaths[sha] = append(r.treePaths[sha], dir)
	entries, err := parseTree(ro.data)
	if err != nil {
		return
	}
	for _, e := range entries {
		p := path.Join(dir, e.name)
		switch e.mode {
		case "40000", "040000":
			r.walkTree(e.sha, p, depth+1)
		case "160000":
		default:
			if !slices.Contains(r.blobPaths[e.sha], p) {
				r.blobPaths[e.sha] = append(r.blobPaths[e.sha], p)
			}
		}
	}
}

// locate turns a raw hit into a Violation. keep is false only for a regex hit
// whose content is reachable solely through allowlisted paths: a blob or tree
// pushed at several paths stays a violation if any one of them is not
// allowlisted, and that path is the one reported.
func (r *packResolver) locate(h pushHit, allowlist []string) (Violation, bool) {
	v := Violation{Category: h.category, Pattern: h.pattern, Sample: h.sample}
	ro, ok := r.resolved[h.obj]
	if !ok {
		v.File = fmt.Sprintf("unresolved object at pack offset %d", r.objs[h.obj].offset)
		return v, true
	}
	var paths []string
	switch ro.typ {
	case objBlob:
		paths = r.blobPaths[ro.sha]
		v.LineNum = locateLine(ro.data, h)
	case objTree:
		if name := treeEntryContaining(ro.data, h); name != "" {
			for _, dir := range r.treePaths[ro.sha] {
				paths = append(paths, path.Join(dir, name))
			}
		}
	default:
		v.LineNum = locateLine(ro.data, h)
	}
	if len(paths) == 0 {
		v.File = ro.typ + " " + ro.sha
		return v, true
	}
	v.File = paths[0]
	if !h.regex {
		return v, true
	}
	for _, p := range paths {
		if !pathAllowlisted(p, allowlist) {
			v.File = p
			return v, true
		}
	}
	return v, false
}

func treeEntryContaining(data []byte, h pushHit) string {
	entries, err := parseTree(data)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if h.regex && strings.Contains(e.name, h.raw) {
			return e.name
		}
		if !h.regex && containsSample(e.name, h.sample) {
			return e.name
		}
	}
	return ""
}

func containsSample(text, sample string) bool {
	return sample != "" && slices.Contains(sanitizer.MatchSensitiveTokens(text), sample)
}

// locateLine returns the 1-based line of the hit within data, or 0 when the
// hit cannot be pinned to one line.
func locateLine(data []byte, h pushHit) int {
	for n, line := range strings.Split(string(data), "\n") {
		if h.regex && strings.Contains(line, h.raw) {
			return n + 1
		}
		if !h.regex && containsSample(line, h.sample) {
			return n + 1
		}
	}
	return 0
}

func pathAllowlisted(p string, allowlist []string) bool {
	for _, prefix := range allowlist {
		prefix = strings.TrimPrefix(strings.TrimSpace(prefix), "/")
		if prefix != "" && strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}

// ScanPayload scans a GitHub API write payload. Registered values always
// match; regex rules apply when policy.PublicRepo, except under an allowlisted
// filePath. filePath is empty when the payload is not a repository file.
func ScanPayload(data []byte, filePath string, policy PushScanPolicy) []Violation {
	label := sanitizer.SanitizeString(filePath)
	if label == "" {
		label = "request body"
	}
	hits := valueHits(0, data)
	allowlisted := filePath != "" && pathAllowlisted(filePath, policy.RegexAllowlist)
	if policy.PublicRepo && !allowlisted && !isBinary(data) {
		hits = append(hits, regexHits(0, string(data), PushScanRules())...)
	}
	out := make([]Violation, 0, len(hits))
	for _, h := range hits {
		out = append(out, Violation{
			Category: h.category, Pattern: h.pattern, Sample: h.sample,
			File: label, LineNum: locateLine(data, h),
		})
	}
	return out
}
