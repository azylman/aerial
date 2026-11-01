package delivery

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"
)

var (
	reCodeSpans   = regexp.MustCompile("(?s)```.*?```|~~~.*?~~~|`[^`\n]+`")
	reDisplayMath = regexp.MustCompile(`(\n?)[ \t]*(?:\$\$([\s\S]+?)\$\$|\\\[([\s\S]+?)\\\])[ \t]*(\n?)`)
	reInlineParen = regexp.MustCompile(`\\\(([^\n]+?)\\\)`)
	reLatexText   = regexp.MustCompile(`\\(?:text(?:bf|it|rm|sf|tt)?|math(?:rm|bf|it|cal|bb|sf|tt|scr|frak)|operatorname|boldsymbol|bm)\{([^{}]*)\}`)
	reLatexFrac   = regexp.MustCompile(`\\frac\{([^{}]*)\}\{([^{}]*)\}`)
	reLatexSqrt   = regexp.MustCompile(`\\sqrt\{([^{}]*)\}`)
	reLatexCmd    = regexp.MustCompile(`\\[a-zA-Z]+`)
	reSuperSub    = regexp.MustCompile(`([\^_])(?:\{([^{}]+)\}|([0-9a-zA-Z+\-]))`)
)

var latexCommandMap = map[string]string{
	`\alpha`: "α", `\beta`: "β", `\gamma`: "γ", `\delta`: "δ", `\epsilon`: "ε", `\varepsilon`: "ε",
	`\zeta`: "ζ", `\eta`: "η", `\theta`: "θ", `\iota`: "ι", `\kappa`: "κ", `\lambda`: "λ",
	`\mu`: "μ", `\nu`: "ν", `\xi`: "ξ", `\pi`: "π", `\rho`: "ρ", `\sigma`: "σ", `\tau`: "τ",
	`\upsilon`: "υ", `\phi`: "φ", `\varphi`: "φ", `\chi`: "χ", `\psi`: "ψ", `\omega`: "ω",
	`\Gamma`: "Γ", `\Delta`: "Δ", `\Theta`: "Θ", `\Lambda`: "Λ", `\Xi`: "Ξ", `\Pi`: "Π",
	`\Sigma`: "Σ", `\Phi`: "Φ", `\Psi`: "Ψ", `\Omega`: "Ω",
	`\cdot`: "·", `\times`: "×", `\div`: "÷", `\pm`: "±", `\mp`: "∓",
	`\le`: "≤", `\leq`: "≤", `\ge`: "≥", `\geq`: "≥", `\ne`: "≠", `\neq`: "≠",
	`\approx`: "≈", `\equiv`: "≡", `\sim`: "∼", `\propto`: "∝",
	`\in`: "∈", `\notin`: "∉", `\subset`: "⊂", `\subseteq`: "⊆", `\cup`: "∪", `\cap`: "∩", `\emptyset`: "∅",
	`\infty`: "∞", `\nabla`: "∇", `\partial`: "∂", `\forall`: "∀", `\exists`: "∃",
	`\to`: "→", `\rightarrow`: "→", `\leftarrow`: "←", `\Rightarrow`: "⇒", `\Leftrightarrow`: "⇔",
	`\sum`: "∑", `\prod`: "∏", `\int`: "∫",
	`\max`: "max", `\min`: "min", `\log`: "log", `\ln`: "ln", `\exp`: "exp", `\lim`: "lim", `\sin`: "sin", `\cos`: "cos",
	`\left`: "", `\right`: "", `\quad`: " ", `\qquad`: "  ",
}

var superscriptRunes = map[rune]rune{
	'0': '⁰', '1': '¹', '2': '²', '3': '³', '4': '⁴', '5': '⁵', '6': '⁶', '7': '⁷', '8': '⁸', '9': '⁹',
	'+': '⁺', '-': '⁻', '=': '⁼', '(': '⁽', ')': '⁾', 'n': 'ⁿ', 'i': 'ⁱ',
}

var subscriptRunes = map[rune]rune{
	'0': '₀', '1': '₁', '2': '₂', '3': '₃', '4': '₄', '5': '₅', '6': '₆', '7': '₇', '8': '₈', '9': '₉',
	'+': '₊', '-': '₋', '=': '₌', '(': '₍', ')': '₎',
	'a': 'ₐ', 'e': 'ₑ', 'h': 'ₕ', 'i': 'ᵢ', 'j': 'ⱼ', 'k': 'ₖ', 'l': 'ₗ', 'm': 'ₘ', 'n': 'ₙ',
	'o': 'ₒ', 'p': 'ₚ', 'r': 'ᵣ', 's': 'ₛ', 't': 'ₜ', 'u': 'ᵤ', 'v': 'ᵥ', 'x': 'ₓ',
}

// FormatDiscordLatex converts display and inline LaTeX math outside code blocks into Discord-friendly
// fenced blocks and Unicode mathematical notation while preserving currency ($200), tickers ($SOL), and env vars ($PATH).
func FormatDiscordLatex(text string) string {
	if !strings.ContainsAny(text, "$\\") {
		return text
	}

	var masked []string
	mask := func(s string) string {
		idx := len(masked)
		masked = append(masked, s)
		return fmt.Sprintf("\x00CODE%d\x00", idx)
	}

	out := reCodeSpans.ReplaceAllStringFunc(text, mask)
	out = reDisplayMath.ReplaceAllStringFunc(out, func(m string) string {
		sub := reDisplayMath.FindStringSubmatch(m)
		inner := strings.TrimSpace(translateLatexMath(sub[2] + sub[3]))
		return "\n" + mask("```\n"+inner+"\n```") + "\n"
	})
	out = strings.Trim(out, "\n")

	out = reInlineParen.ReplaceAllStringFunc(out, func(m string) string {
		sub := reInlineParen.FindStringSubmatch(m)
		return translateLatexMath(sub[1])
	})

	lines := strings.Split(out, "\n")
	for i, line := range lines {
		if strings.Count(line, "$") >= 2 {
			lines[i] = replaceInlineDollarMath(line)
		}
	}
	out = strings.Join(lines, "\n")

	for i, m := range slices.Backward(masked) {
		out = strings.ReplaceAll(out, fmt.Sprintf("\x00CODE%d\x00", i), m)
	}
	return out
}

func replaceInlineDollarMath(line string) string {
	var sb strings.Builder
	for i := 0; i < len(line); {
		if line[i] != '$' || (i > 0 && line[i-1] == '\\') || i+1 >= len(line) || line[i+1] == ' ' || line[i+1] == '\t' {
			sb.WriteByte(line[i])
			i++
			continue
		}
		closeIdx := -1
		for j := i + 1; j < len(line); j++ {
			if line[j] == '$' && line[j-1] != '\\' {
				closeIdx = j
				break
			}
		}
		if closeIdx > i+1 && line[closeIdx-1] != ' ' && line[closeIdx-1] != '\t' && line[closeIdx-1] != '(' && line[closeIdx-1] != '[' && (closeIdx+1 == len(line) || (!isWordOrDigit(line[closeIdx+1]) && line[closeIdx+1] != '\\')) && isLikelyLatexMath(line[i+1:closeIdx]) {
			sb.WriteString(translateLatexMath(line[i+1 : closeIdx]))
			i = closeIdx + 1
			continue
		}
		sb.WriteByte(line[i])
		i++
	}
	return sb.String()
}

func isWordOrDigit(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_'
}

func isLikelyLatexMath(s string) bool {
	if s[0] >= '0' && s[0] <= '9' {
		k := 0
		for k < len(s) && ((s[k] >= '0' && s[k] <= '9') || s[k] == '.' || s[k] == ',') {
			k++
		}
		for k < len(s) && (s[k] == ' ' || s[k] == '\t') {
			k++
		}
		r, _ := utf8.DecodeRuneInString(s[k:])
		return k < len(s) && strings.ContainsRune(`\=^_<>±×·+-*/`, r)
	}
	if len(s) == 1 && ((s[0] >= 'a' && s[0] <= 'z') || (s[0] >= 'A' && s[0] <= 'Z')) {
		return true
	}
	return strings.ContainsAny(s, `\=^_<>±×·+-*/()[]{}|`)
}

func translateLatexMath(s string) string {
	out := reLatexText.ReplaceAllString(s, "$1")
	out = strings.ReplaceAll(out, `\,`, " ")
	out = strings.ReplaceAll(out, `\;`, " ")
	out = reLatexCmd.ReplaceAllStringFunc(out, func(cmd string) string {
		if repl, ok := latexCommandMap[cmd]; ok {
			return repl
		}
		return cmd
	})
	out = reSuperSub.ReplaceAllStringFunc(out, func(m string) string {
		sub := reSuperSub.FindStringSubmatch(m)
		op, grp := sub[1], sub[2]
		if grp == "" {
			grp = sub[3]
		}
		table, openP, closeP := subscriptRunes, "₍", "₎"
		if op == "^" {
			table, openP, closeP = superscriptRunes, "⁽", "⁾"
		}
		var mapped strings.Builder
		for _, r := range grp {
			mr, ok := table[r]
			if !ok {
				return openP + grp + closeP
			}
			mapped.WriteRune(mr)
		}
		return mapped.String()
	})
	out = reLatexSqrt.ReplaceAllString(out, "√($1)")
	return reLatexFrac.ReplaceAllString(out, "($1)/($2)")
}
