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
	DefaultSkillCliqueMinSim = 0.75
	// DefaultSkillCliqueMinSize is the minimum clique size required to emit a CREATE batch.
	DefaultSkillCliqueMinSize = 2
	// DefaultSkillCliqueMaxSize is the maximum clique size clustered together.
	DefaultSkillCliqueMaxSize = 5
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
	Action      string                `json:"action"` // "EDIT" or "CREATE"
	TargetSkill *DiskSkill            `json:"target_skill,omitempty"`
	Domain      string                `json:"domain"`
	Candidates  []ProceduralCandidate `json:"candidates"`
}

// SkillFileChange represents a compiled skill file modification ready for Gitless commit/PR publishing.
type SkillFileChange struct {
	RelPath       string  `json:"rel_path"`
	Content       string  `json:"content"`
	Action        string  `json:"action"`
	SkillName     string  `json:"skill_name"`
	CommitSummary string  `json:"commit_summary"`
	CandidateIDs  []int64 `json:"candidate_ids"`
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
	if strings.HasPrefix(rest, "\r\n") {
		rest = rest[2:]
	} else if strings.HasPrefix(rest, "\n") {
		rest = rest[1:]
	} else {
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
			if strings.HasPrefix(after, "\r\n") {
				body = strings.TrimSpace(after[2:])
			} else if strings.HasPrefix(after, "\n") {
				body = strings.TrimSpace(after[1:])
			} else {
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

// PlanSkillConsolidation groups pending procedural candidates into consolidation batches across Case A (loaded custom skill match),
// Case B (unloaded routing miss matching existing skill), and Case C (novel workflow clique). Blocked core skill candidate IDs are skipped.
func PlanSkillConsolidation(pending []ProceduralCandidate, diskSkills []DiskSkill) ([]SkillConsolidationBatch, []int64) {
	var skippedIDs []int64
	var active []ProceduralCandidate

	for _, c := range pending {
		if _, blocked := BlockedCoreSkills[c.LoadedSkill]; blocked {
			skippedIDs = append(skippedIDs, c.ID)
			continue
		}
		active = append(active, c)
	}

	diskSkillsByName := make(map[string]*DiskSkill, len(diskSkills))
	for i := range diskSkills {
		diskSkillsByName[diskSkills[i].Name] = &diskSkills[i]
	}

	// Case A: Loaded Custom Skill Match
	caseAGroups := make(map[string][]ProceduralCandidate)
	var remaining []ProceduralCandidate

	for _, c := range active {
		if c.LoadedSkill != "" && diskSkillsByName[c.LoadedSkill] != nil {
			caseAGroups[c.LoadedSkill] = append(caseAGroups[c.LoadedSkill], c)
		} else {
			remaining = append(remaining, c)
		}
	}

	var batches []SkillConsolidationBatch

	var caseASkillNames []string
	for name := range caseAGroups {
		caseASkillNames = append(caseASkillNames, name)
	}
	sort.Strings(caseASkillNames)

	for _, name := range caseASkillNames {
		group := caseAGroups[name]
		hasAvoid := false
		for _, c := range group {
			if len(c.Avoid) > 0 {
				hasAvoid = true
				break
			}
		}
		if len(group) >= 2 || hasAvoid {
			target := diskSkillsByName[name]
			batches = append(batches, SkillConsolidationBatch{
				Action:      "EDIT",
				TargetSkill: target,
				Domain:      target.Domain,
				Candidates:  group,
			})
		}
	}

	// Case B: Unloaded Routing Miss
	// Candidates with LoadedSkill == "" (or not in diskSkills) whose embedding has
	// CosineSimilarity >= 0.72 against a DiskSkill.Embedding are grouped by highest-similarity DiskSkill.
	caseBGroups := make(map[string][]ProceduralCandidate)
	var caseBUnmatched []ProceduralCandidate

	for _, c := range remaining {
		if len(c.Embedding) == 0 || len(diskSkills) == 0 {
			caseBUnmatched = append(caseBUnmatched, c)
			continue
		}

		var bestSkill *DiskSkill
		bestSim := -1.0
		for i := range diskSkills {
			if len(diskSkills[i].Embedding) == 0 {
				continue
			}
			sim := CosineSimilarity(c.Embedding, diskSkills[i].Embedding)
			if sim > bestSim {
				bestSim = sim
				bestSkill = &diskSkills[i]
			}
		}

		if bestSkill != nil && bestSim >= DefaultSkillMatchMinSim {
			caseBGroups[bestSkill.Name] = append(caseBGroups[bestSkill.Name], c)
		} else {
			caseBUnmatched = append(caseBUnmatched, c)
		}
	}

	var caseBSkillNames []string
	for name := range caseBGroups {
		caseBSkillNames = append(caseBSkillNames, name)
	}
	sort.Strings(caseBSkillNames)

	for _, name := range caseBSkillNames {
		group := caseBGroups[name]
		target := diskSkillsByName[name]
		batches = append(batches, SkillConsolidationBatch{
			Action:      "EDIT",
			TargetSkill: target,
			Domain:      target.Domain,
			Candidates:  group,
		})
	}

	// Case C: Novel Workflow Clique
	// Remaining unmatched candidates clustered via complete-linkage cosine similarity
	// (minSim = 0.75, minSize = 2, maxSize = 5); each clique of size >= 2 emits a CREATE batch.
	cliques := findCandidateCliques(caseBUnmatched, DefaultSkillCliqueMinSim, DefaultSkillCliqueMinSize, DefaultSkillCliqueMaxSize)
	for _, clique := range cliques {
		domain := "discord"
		if len(clique) > 0 && clique[0].Domain != "" {
			domain = clique[0].Domain
		}
		batches = append(batches, SkillConsolidationBatch{
			Action:     "CREATE",
			Domain:     domain,
			Candidates: clique,
		})
	}

	sort.Slice(skippedIDs, func(i, j int) bool {
		return skippedIDs[i] < skippedIDs[j]
	})

	return batches, skippedIDs
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

	var group []ProceduralCandidate
	for _, c := range cands {
		if len(c.Embedding) > 0 {
			group = append(group, c)
		}
	}

	sort.Slice(group, func(i, j int) bool {
		return group[i].ID < group[j].ID
	})

	type edge struct {
		i, j int
		sim  float64
	}
	var edges []edge
	for i := 0; i < len(group); i++ {
		for j := i + 1; j < len(group); j++ {
			sim := CosineSimilarity(group[i].Embedding, group[j].Embedding)
			if sim >= minSim {
				edges = append(edges, edge{i: i, j: j, sim: sim})
			}
		}
	}

	sort.SliceStable(edges, func(a, b int) bool {
		if edges[a].sim != edges[b].sim {
			return edges[a].sim > edges[b].sim
		}
		if group[edges[a].i].ID != group[edges[b].i].ID {
			return group[edges[a].i].ID < group[edges[b].i].ID
		}
		return group[edges[a].j].ID < group[edges[b].j].ID
	})

	type candidateMatch struct {
		cand ProceduralCandidate
		sim  float64
	}

	var cliques [][]ProceduralCandidate
	used := make(map[int64]bool, len(group))
	for _, e := range edges {
		u, v := group[e.i], group[e.j]
		if used[u.ID] || used[v.ID] {
			continue
		}
		clique := []ProceduralCandidate{u, v}
		if maxSize > 2 {
			var matchCands []candidateMatch
			for k := 0; k < len(group); k++ {
				if k == e.i || k == e.j || used[group[k].ID] {
					continue
				}
				simU := CosineSimilarity(u.Embedding, group[k].Embedding)
				simV := CosineSimilarity(v.Embedding, group[k].Embedding)
				if simU >= minSim && simV >= minSim {
					matchCands = append(matchCands, candidateMatch{cand: group[k], sim: min(simU, simV)})
				}
			}
			sort.SliceStable(matchCands, func(a, b int) bool {
				if matchCands[a].sim != matchCands[b].sim {
					return matchCands[a].sim > matchCands[b].sim
				}
				return matchCands[a].cand.ID < matchCands[b].cand.ID
			})

			for _, c := range matchCands {
				if len(clique) >= maxSize {
					break
				}
				matchesAll := true
				for _, member := range clique[2:] {
					sim := CosineSimilarity(member.Embedding, c.cand.Embedding)
					if sim < minSim {
						matchesAll = false
						break
					}
				}
				if matchesAll {
					clique = append(clique, c.cand)
				}
			}
		}

		if len(clique) >= minSize {
			sort.Slice(clique, func(a, b int) bool { return clique[a].ID < clique[b].ID })
			for _, m := range clique {
				used[m.ID] = true
			}
			cliques = append(cliques, clique)
		}
	}
	return cliques
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

	sb.WriteString(fmt.Sprintf("Action: %s\n", batch.Action))
	sb.WriteString(fmt.Sprintf("Target Domain: %s\n", batch.Domain))

	if batch.Action == "EDIT" && batch.TargetSkill != nil {
		sb.WriteString(fmt.Sprintf("Target Skill Name: %s\n", batch.TargetSkill.Name))
		sb.WriteString(fmt.Sprintf("Target Skill RelPath: %s\n", batch.TargetSkill.RelPath))
		sb.WriteString(fmt.Sprintf("Target Skill Description: %s\n\n", batch.TargetSkill.Description))
		sb.WriteString("Existing SKILL.md Content:\n```markdown\n")
		sb.WriteString(batch.TargetSkill.RawContent)
		sb.WriteString("\n```\n\n")
	}

	sb.WriteString("Procedural Friction Candidates to Consolidate:\n")
	for i, c := range batch.Candidates {
		sb.WriteString(fmt.Sprintf("Candidate #%d (ID: %d):\n", i+1, c.ID))
		sb.WriteString(fmt.Sprintf("  Trigger: %s\n", c.Trigger))
		if len(c.Steps) > 0 {
			sb.WriteString("  Observed Steps:\n")
			for _, step := range c.Steps {
				sb.WriteString(fmt.Sprintf("    - %s\n", step))
			}
		}
		if len(c.Avoid) > 0 {
			sb.WriteString("  Avoid / Friction:\n")
			for _, avoid := range c.Avoid {
				sb.WriteString(fmt.Sprintf("    - %s\n", avoid))
			}
		}
	}

	sb.WriteString("\nInstructions:\n")
	sb.WriteString("1. If Action is EDIT, update the existing SKILL.md content by integrating the new triggers, steps, and avoid guidance without deleting unrelated workflows. Keep frontmatter name matching the target.\n")
	sb.WriteString("2. If Action is CREATE, design a new SKILL.md with valid YAML frontmatter (name in ^[a-z0-9-]+$, description between 20 and 500 chars), clear usage guidelines, step-by-step procedures, and avoid sections.\n")
	sb.WriteString("3. If the candidates are noisy, invalid, or do not warrant a skill change, return action \"NOOP\".\n")
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
