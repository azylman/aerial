package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	// DefaultSkillMatchMinSim is the minimum cosine similarity threshold for routing unloaded candidates to existing skills (Case B).
	DefaultSkillMatchMinSim = 0.72
	// DefaultSkillCliqueMinSim is the minimum pairwise similarity threshold for forming new skill candidate cliques (Case C).
	DefaultSkillCliqueMinSim = 0.70
	// DefaultSkillCliqueMinSize is the minimum clique size required to emit a CREATE batch.
	DefaultSkillCliqueMinSize = 2
	// DefaultSkillCliqueMaxSize is the maximum clique size clustered together.
	DefaultSkillCliqueMaxSize = 5
	// DefaultSkillCreateMinSteps is the minimum number of procedural steps required for a candidate to join a Case C (CREATE) skill clique.
	DefaultSkillCreateMinSteps = 3
	// DefaultShortFrictionMinRepeats is the minimum repeat clique size required to emit an ALERT batch for short (< 3 steps) friction candidates.
	DefaultShortFrictionMinRepeats = 5
)

var (
	validSkillNameRegex = regexp.MustCompile(`^[a-z0-9-]+$`)
	_                   = []any{
		cleanOuterCodeFences,
		ParseSkillFrontmatter,
		ScanCustomSkills,
		PlanSkillConsolidation,
		findCandidateCliques,
		ValidateCompiledSkillMarkdown,
		buildSkillDesignerPrompt,
	}
)

// DiskSkill represents a custom skill scanned from the disk repository.
type DiskSkill struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Domain      string    `json:"domain"`
	RelPath     string    `json:"rel_path"`
	RawContent  string    `json:"raw_content"`
	Embedding   []float32 `json:"-"`
}

// SkillConsolidationBatch groups procedural candidates scheduled for compilation into an EDIT or CREATE action.
type SkillConsolidationBatch struct {
	Action        string                `json:"action"` // "EDIT" or "CREATE"
	TargetSkill   *DiskSkill            `json:"target_skill,omitempty"`
	Domain        string                `json:"domain"`
	Candidates    []ProceduralCandidate `json:"candidates"`
	RequireReview bool                  `json:"require_review,omitempty"`
}

// SkillFileChange represents a compiled skill file modification ready for Gitless commit/PR publishing.
type SkillFileChange struct {
	RelPath       string                `json:"rel_path"`
	Content       string                `json:"content"`
	Action        string                `json:"action"`
	SkillName     string                `json:"skill_name"`
	CommitSummary string                `json:"commit_summary"`
	CandidateIDs  []int64               `json:"candidate_ids"`
	Candidates    []ProceduralCandidate `json:"candidates,omitempty"`
	RequireReview bool                  `json:"require_review,omitempty"`
}

func batchRequiresReview(cands []ProceduralCandidate) bool {
	for _, c := range cands {
		if _, blocked := BlockedCoreSkills[strings.TrimSpace(c.LoadedSkill)]; blocked {
			return true
		}
	}
	return false
}

// DesignerSkillResponse represents the structured LLM output from the Skill Designer.
type DesignerSkillResponse struct {
	Action        string `json:"action"`
	Name          string `json:"name"`
	Domain        string `json:"domain"`
	CommitSummary string `json:"commit_summary"`
	Markdown      string `json:"markdown"`
}

// cleanOuterCodeFences strips optional outer markdown code fences (e.g. ```markdown ... ```) wrapped around content.
func cleanOuterCodeFences(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if strings.HasPrefix(trimmed, "```") {
		firstNL := strings.Index(trimmed, "\n")
		lastFence := strings.LastIndex(trimmed, "```")
		if firstNL != -1 && lastFence > firstNL {
			return strings.TrimSpace(trimmed[firstNL+1 : lastFence])
		}
	}
	return trimmed
}

// ParseSkillFrontmatter strips optional outer code fences, extracts and unmarshals the YAML frontmatter block,
// and returns the parsed name, description, markdown body, or an error.
func ParseSkillFrontmatter(raw string) (name, description, body string, err error) {
	cleaned := cleanOuterCodeFences(raw)
	if !strings.HasPrefix(cleaned, "---") {
		return "", "", "", fmt.Errorf("missing frontmatter opening delimiter")
	}

	rest := cleaned[3:]
	switch {
	case strings.HasPrefix(rest, "\r\n"):
		rest = rest[2:]
	case strings.HasPrefix(rest, "\n"):
		rest = rest[1:]
	default:
		return "", "", "", fmt.Errorf("invalid frontmatter opening delimiter")
	}

	var fmContent string
	closingIdx := -1
	searchOffset := 0
	for {
		sub := rest[searchOffset:]
		idx := strings.Index(sub, "\n---")
		if idx == -1 {
			break
		}
		absIdx := searchOffset + idx
		after := rest[absIdx+4:]
		if after == "" || strings.HasPrefix(after, "\n") || strings.HasPrefix(after, "\r\n") {
			closingIdx = absIdx
			fmContent = rest[:closingIdx]
			switch {
			case strings.HasPrefix(after, "\r\n"):
				body = strings.TrimSpace(after[2:])
			case strings.HasPrefix(after, "\n"):
				body = strings.TrimSpace(after[1:])
			default:
				body = ""
			}
			break
		}
		searchOffset = absIdx + 1
	}

	if closingIdx == -1 {
		return "", "", "", fmt.Errorf("missing frontmatter closing delimiter")
	}

	var fm struct {
		Name        string `yaml:"name"`
		Description string `yaml:"description"`
	}
	if err := yaml.Unmarshal([]byte(fmContent), &fm); err != nil {
		return "", "", "", fmt.Errorf("invalid frontmatter yaml: %w", err)
	}

	name = strings.TrimSpace(fm.Name)
	description = strings.TrimSpace(fm.Description)
	if name == "" || description == "" {
		return "", "", "", fmt.Errorf("frontmatter name and description must be non-empty")
	}

	return name, description, body, nil
}

// ScanCustomSkills scans common/*/SKILL.md and discord/*/SKILL.md under customSkillsDir, parses frontmatter,
// sets RelPath to custom-skills/<domain>/<name>/SKILL.md, and generates embeddings over name + ": " + description.
func ScanCustomSkills(ctx context.Context, customSkillsDir string, ollama OllamaClient) ([]DiskSkill, error) {
	var skills []DiskSkill
	domains := []string{"common", "discord"}

	for _, domain := range domains {
		domainDir := filepath.Join(customSkillsDir, domain)
		entries, err := os.ReadDir(domainDir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("failed to read custom skills domain %s: %w", domain, err)
		}

		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			skillPath := filepath.Join(domainDir, entry.Name(), "SKILL.md")
			data, err := os.ReadFile(skillPath)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return nil, fmt.Errorf("failed to read skill file %s: %w", skillPath, err)
			}

			name, desc, _, err := ParseSkillFrontmatter(string(data))
			if err != nil {
				return nil, fmt.Errorf("failed to parse frontmatter in %s: %w", skillPath, err)
			}

			var emb []float32
			if ollama != nil {
				vec, err := ollama.GenerateEmbedding(ctx, name+": "+desc, false, 1)
				if err != nil {
					return nil, fmt.Errorf("failed to generate embedding for skill %s: %w", name, err)
				}
				emb = vec
			}

			skills = append(skills, DiskSkill{
				Name:        name,
				Description: desc,
				Domain:      domain,
				RelPath:     fmt.Sprintf("custom-skills/%s/%s/SKILL.md", domain, name),
				RawContent:  string(data),
				Embedding:   emb,
			})
		}
	}

	sort.Slice(skills, func(i, j int) bool {
		if skills[i].Domain != skills[j].Domain {
			return skills[i].Domain < skills[j].Domain
		}
		return skills[i].Name < skills[j].Name
	})

	return skills, nil
}

func hasCandidateWithAvoid(group []ProceduralCandidate) bool {
	for _, c := range group {
		if len(c.Avoid) > 0 {
			return true
		}
	}
	return false
}

func planLoadedSkillBatches(pending []ProceduralCandidate, diskSkillsByName map[string]*DiskSkill) ([]SkillConsolidationBatch, []ProceduralCandidate) {
	caseAGroups := make(map[string][]ProceduralCandidate)
	var remaining []ProceduralCandidate

	for _, c := range pending {
		if c.LoadedSkill != "" && diskSkillsByName[c.LoadedSkill] != nil {
			caseAGroups[c.LoadedSkill] = append(caseAGroups[c.LoadedSkill], c)
		} else {
			remaining = append(remaining, c)
		}
	}

	var batches []SkillConsolidationBatch
	caseASkillNames := make([]string, 0, len(caseAGroups))
	for name := range caseAGroups {
		caseASkillNames = append(caseASkillNames, name)
	}
	sort.Strings(caseASkillNames)

	for _, name := range caseASkillNames {
		group := caseAGroups[name]
		if len(group) >= 2 || hasCandidateWithAvoid(group) {
			target := diskSkillsByName[name]
			batches = append(batches, SkillConsolidationBatch{
				Action:        "EDIT",
				TargetSkill:   target,
				Domain:        target.Domain,
				Candidates:    group,
				RequireReview: batchRequiresReview(group),
			})
		}
	}
	return batches, remaining
}

func findBestMatchingDiskSkill(emb []float32, diskSkills []DiskSkill) (*DiskSkill, float64) {
	var bestSkill *DiskSkill
	bestSim := -1.0
	for i := range diskSkills {
		if len(diskSkills[i].Embedding) == 0 {
			continue
		}
		sim := CosineSimilarity(emb, diskSkills[i].Embedding)
		if sim > bestSim {
			bestSim = sim
			bestSkill = &diskSkills[i]
		}
	}
	return bestSkill, bestSim
}

func planUnloadedSkillBatches(remaining []ProceduralCandidate, diskSkills []DiskSkill, diskSkillsByName map[string]*DiskSkill) ([]SkillConsolidationBatch, []ProceduralCandidate) {
	caseBGroups := make(map[string][]ProceduralCandidate)
	var caseBUnmatched []ProceduralCandidate

	for _, c := range remaining {
		if len(c.Embedding) == 0 || len(diskSkills) == 0 {
			caseBUnmatched = append(caseBUnmatched, c)
			continue
		}

		bestSkill, bestSim := findBestMatchingDiskSkill(c.Embedding, diskSkills)
		if bestSkill != nil && bestSim >= DefaultSkillMatchMinSim {
			caseBGroups[bestSkill.Name] = append(caseBGroups[bestSkill.Name], c)
		} else {
			caseBUnmatched = append(caseBUnmatched, c)
		}
	}

	caseBSkillNames := make([]string, 0, len(caseBGroups))
	for name := range caseBGroups {
		caseBSkillNames = append(caseBSkillNames, name)
	}
	sort.Strings(caseBSkillNames)
	batches := make([]SkillConsolidationBatch, 0, len(caseBSkillNames))

	for _, name := range caseBSkillNames {
		group := caseBGroups[name]
		target := diskSkillsByName[name]
		batches = append(batches, SkillConsolidationBatch{
			Action:        "EDIT",
			TargetSkill:   target,
			Domain:        target.Domain,
			Candidates:    group,
			RequireReview: batchRequiresReview(group),
		})
	}

	return batches, caseBUnmatched
}

// PlanSkillConsolidation groups pending procedural candidates into consolidation batches across Case A (loaded custom skill match),
// Case B (unloaded routing miss matching existing skill), and Case C (novel workflow clique).
// Batches containing candidates extracted while a BlockedCoreSkills skill was loaded are flagged with RequireReview: true.
func PlanSkillConsolidation(pending []ProceduralCandidate, diskSkills []DiskSkill) ([]SkillConsolidationBatch, []int64) {
	diskSkillsByName := make(map[string]*DiskSkill, len(diskSkills))
	for i := range diskSkills {
		diskSkillsByName[diskSkills[i].Name] = &diskSkills[i]
	}

	batches, remaining := planLoadedSkillBatches(pending, diskSkillsByName)
	caseBBatches, caseBUnmatched := planUnloadedSkillBatches(remaining, diskSkills, diskSkillsByName)
	batches = append(batches, caseBBatches...)

	// Case C: Novel Workflow Clique (>= 3 steps, minSize = 2)
	// Case D: Short / 1-Liner Friction Alert Clique (< 3 steps, minSize = 5)
	batches = appendCliqueBatches(batches, findCandidateCliques(caseBUnmatched, DefaultSkillCliqueMinSim, DefaultSkillCliqueMinSize, DefaultSkillCliqueMaxSize), "CREATE")
	batches = appendCliqueBatches(batches, findCandidateCliques(filterShortFrictionCandidates(caseBUnmatched), DefaultSkillCliqueMinSim, DefaultShortFrictionMinRepeats, DefaultShortFrictionMinRepeats), "ALERT")

	return batches, nil
}

func appendCliqueBatches(batches []SkillConsolidationBatch, cliques [][]ProceduralCandidate, action string) []SkillConsolidationBatch {
	for _, clique := range cliques {
		domain := "discord"
		if len(clique) > 0 && clique[0].Domain != "" {
			domain = clique[0].Domain
		}
		batches = append(batches, SkillConsolidationBatch{
			Action:        action,
			Domain:        domain,
			Candidates:    clique,
			RequireReview: action != "ALERT" && batchRequiresReview(clique),
		})
	}
	return batches
}

// filterCreateCliqueCandidates retains candidates with non-empty embeddings and at least DefaultSkillCreateMinSteps steps.
func filterCreateCliqueCandidates(cands []ProceduralCandidate) []ProceduralCandidate {
	var group []ProceduralCandidate
	for _, c := range cands {
		if len(c.Embedding) > 0 && len(c.Steps) >= DefaultSkillCreateMinSteps {
			group = append(group, c)
		}
	}
	return group
}

// filterShortFrictionCandidates retains candidates with non-empty embeddings and fewer than DefaultSkillCreateMinSteps steps.
func filterShortFrictionCandidates(cands []ProceduralCandidate) []ProceduralCandidate {
	var group []ProceduralCandidate
	for _, c := range cands {
		if len(c.Embedding) > 0 && len(c.Steps) < DefaultSkillCreateMinSteps {
			group = append(group, c)
		}
	}
	return group
}

// findCandidateCliques clusters procedural candidates into complete-linkage cliques of size [minSize, maxSize].
func findCandidateCliques(cands []ProceduralCandidate, minSim float64, minSize, maxSize int) [][]ProceduralCandidate {
	if minSim <= 0 {
		minSim = DefaultSkillCliqueMinSim
	}
	if minSize <= 0 {
		minSize = DefaultSkillCliqueMinSize
	}
	if maxSize < minSize {
		maxSize = DefaultSkillCliqueMaxSize
	}

	group := cands
	if minSize < DefaultShortFrictionMinRepeats {
		group = filterCreateCliqueCandidates(cands)
	}

	return findCompleteLinkageCliques(
		group, minSim, 1.0, minSize, maxSize,
		func(c ProceduralCandidate) int64 { return c.ID },
		func(c ProceduralCandidate) []float32 { return c.Embedding },
	)
}

// ValidateCompiledSkillMarkdown validates compiled skill markdown against structural, length, and scope constraints.
func ValidateCompiledSkillMarkdown(action, domain, rawMarkdown string, target *DiskSkill, existingSkills []DiskSkill) (name, relPath, cleanedMarkdown string, err error) {
	cleanedMarkdown = cleanOuterCodeFences(rawMarkdown)

	parsedName, desc, _, err := ParseSkillFrontmatter(cleanedMarkdown)
	if err != nil {
		return "", "", "", fmt.Errorf("invalid skill markdown frontmatter: %w", err)
	}
	name = parsedName

	if !validSkillNameRegex.MatchString(name) {
		return "", "", "", fmt.Errorf("invalid skill name %q: must match ^[a-z0-9-]+$", name)
	}

	if len(desc) < 20 || len(desc) > 500 {
		return "", "", "", fmt.Errorf("skill description length %d out of bounds [20, 500]", len(desc))
	}

	if len(cleanedMarkdown) < 100 || len(cleanedMarkdown) > 16384 {
		return "", "", "", fmt.Errorf("skill markdown length %d out of bounds [100, 16384]", len(cleanedMarkdown))
	}

	if _, blocked := BlockedCoreSkills[name]; blocked {
		return "", "", "", fmt.Errorf("skill name %q is a blocked core skill", name)
	}

	act := strings.ToUpper(strings.TrimSpace(action))
	switch act {
	case "EDIT":
		if target == nil {
			return "", "", "", fmt.Errorf("target skill required for EDIT action")
		}
		if name != target.Name {
			return "", "", "", fmt.Errorf("edited skill name %q does not match target %q", name, target.Name)
		}
		minLen := int(float64(len(target.RawContent)) * 0.70)
		if len(cleanedMarkdown) < minLen {
			return "", "", "", fmt.Errorf("edited skill markdown shrunk by >30%% (%d bytes < %d bytes minimum)", len(cleanedMarkdown), minLen)
		}
		relPath = target.RelPath

	case "CREATE":
		for _, s := range existingSkills {
			if s.Name == name {
				return "", "", "", fmt.Errorf("skill name %q collides with existing skill", name)
			}
		}
		normDomain := strings.ToLower(strings.TrimSpace(domain))
		if normDomain != "common" {
			normDomain = "discord"
		}
		relPath = fmt.Sprintf("custom-skills/%s/%s/SKILL.md", normDomain, name)

	default:
		return "", "", "", fmt.Errorf("unsupported action %q: must be EDIT or CREATE", action)
	}

	return name, relPath, cleanedMarkdown, nil
}

// buildSkillDesignerPrompt formats the LLM prompt for the skill designer compiler.
func buildSkillDesignerPrompt(batch SkillConsolidationBatch) string {
	var sb strings.Builder
	sb.WriteString("You are an expert AI skill designer for the Aerial assistant system.\n")
	sb.WriteString("Your task is to synthesize procedural knowledge extracted from actual runtime trajectory friction into clean, robust SKILL.md guidelines.\n\n")

	fmt.Fprintf(&sb, "Action: %s\n", batch.Action)
	fmt.Fprintf(&sb, "Target Domain: %s\n", batch.Domain)

	if batch.Action == "EDIT" && batch.TargetSkill != nil {
		fmt.Fprintf(&sb, "Target Skill Name: %s\n", batch.TargetSkill.Name)
		fmt.Fprintf(&sb, "Target Skill RelPath: %s\n", batch.TargetSkill.RelPath)
		fmt.Fprintf(&sb, "Target Skill Description: %s\n\n", batch.TargetSkill.Description)
		sb.WriteString("Existing SKILL.md Content:\n```markdown\n")
		sb.WriteString(batch.TargetSkill.RawContent)
		sb.WriteString("\n```\n\n")
	}

	sb.WriteString("Procedural Friction Candidates to Consolidate:\n")
	for i, c := range batch.Candidates {
		fmt.Fprintf(&sb, "Candidate #%d (ID: %d):\n", i+1, c.ID)
		if c.LoadedSkill != "" {
			fmt.Fprintf(&sb, "  Loaded Skill: %s\n", c.LoadedSkill)
		}
		fmt.Fprintf(&sb, "  Trigger: %s\n", c.Trigger)
		if len(c.Steps) > 0 {
			sb.WriteString("  Observed Steps:\n")
			for _, step := range c.Steps {
				fmt.Fprintf(&sb, "    - %s\n", step)
			}
		}
		if len(c.Avoid) > 0 {
			sb.WriteString("  Avoid / Friction:\n")
			for _, avoid := range c.Avoid {
				fmt.Fprintf(&sb, "    - %s\n", avoid)
			}
		}
	}

	sb.WriteString("\nInstructions:\n")
	sb.WriteString("1. If Action is EDIT, update the existing SKILL.md content by integrating the new triggers, steps, and avoid guidance without deleting unrelated workflows. Keep frontmatter name matching the target.\n")
	sb.WriteString("2. If Action is CREATE, design a new SKILL.md with valid YAML frontmatter (name in ^[a-z0-9-]+$, description between 20 and 500 chars; never reuse a reserved core skill name such as self-improvement or incident-triage), clear usage guidelines, step-by-step procedures, and avoid sections ONLY when the candidates represent a cohesive multi-step user-facing domain workflow (>= 3 sequential steps); return action \"NOOP\" for single-tool CLI flags, metric lookups, or standalone shell guardrails that belong in semantic facts or rules.\n")
	sb.WriteString("3. Enforce a 30+ days durability bar: write generalizable procedures and imperative avoid rules (+ one clause of why), stripping PR/issue numbers, commit SHAs, dates, negative tool claims (\"tool X is broken\"), transient environment errors, or unresolved dead-ends. If the candidates are noisy, transient, invalid, or do not warrant a durable skill change, return action \"NOOP\".\n")
	sb.WriteString("4. Respond STRICTLY with a valid JSON object matching this schema:\n")
	sb.WriteString("{\n")
	sb.WriteString("  \"action\": \"EDIT\" | \"CREATE\" | \"NOOP\",\n")
	sb.WriteString("  \"name\": \"<skill-name>\",\n")
	sb.WriteString("  \"domain\": \"<domain>\",\n")
	sb.WriteString("  \"commit_summary\": \"<commit summary under 72 chars>\",\n")
	sb.WriteString("  \"markdown\": \"<full SKILL.md with frontmatter>\"\n")
	sb.WriteString("}\n")

	return sb.String()
}
