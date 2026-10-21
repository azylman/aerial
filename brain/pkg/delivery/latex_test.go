package delivery

import "testing"

func TestFormatDiscordLatex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, in, want string
	}{
		{"PlainTextFastPath", "Hello world, no math or dollars here!", "Hello world, no math or dollars here!"},
		{"CurrencyAndTickersUntouched", "From $100 to $200 while $SOL and $BTC rallied, $PATH_VAR stayed set, and $SOL when $x = 2$ held.", "From $100 to $200 while $SOL and $BTC rallied, $PATH_VAR stayed set, and $SOL when x = 2 held."},
		{"InlineGreekAndComparison", "Using $\\lambda = 0.7$ with $\\Delta x \\le \\epsilon$ and $s \\in S$, where $S$ is the set.", "Using λ = 0.7 with Δ x ≤ ε and s ∈ S, where S is the set."},
		{"DisplayMathDoubleDollarToCodeBlock", "Formula:\n$$\\text{MMR}(c) = 0.7 \\cdot \\text{Relevance}(c, Q) - 0.3 \\cdot \\max_{s \\in S} \\text{CosineSim}(c, s)$$\nDone.", "Formula:\n```\nMMR(c) = 0.7 · Relevance(c, Q) - 0.3 · max₍s ∈ S₎ CosineSim(c, s)\n```\nDone."},
		{"DisplayMathBracketsAndInlineParens", "Block: \\[\\frac{\\text{cost}_{10}}{c} \\approx \\sqrt{x^2 + y_0}\\] and inline \\(\\alpha \\times \\beta \\ne \\infty \\, \\unknown\\).", "Block:\n```\n(cost₁₀)/(c) ≈ √(x² + y₀)\n```\nand inline α × β ≠ ∞   \\unknown."},
		{"CodeSpansAndFencesMasked", "Keep `$\\lambda = 0.7$` and:\n```tex\n$$\\frac{1}{2}$$\n```\nbut convert $\\theta^2 + x_{10} \\ge 0$.", "Keep `$\\lambda = 0.7$` and:\n```tex\n$$\\frac{1}{2}$$\n```\nbut convert θ² + x₁₀ ≥ 0."},
		{"FontWrappersFunctionCallsAndLeadingDigits", "$$\n\\mathcal{S}(m) = w_T T(m) + w_N N(m)\n$$\n* **$T(m)$ — Prior**: boosts $T(m)$ and $1 - \\max_{f \\in \\text{DB}} \\text{CosineSim}(e_m, e_f)$.\n* Mid novelty ($0.12 \\le N \\le 0.40$) with $-0.24$ penalty while $100 stays.", "```\nS(m) = w₍T₎ T(m) + w₍N₎ N(m)\n```\n* **T(m) — Prior**: boosts T(m) and 1 - max₍f ∈ DB₎ CosineSim(eₘ, e₍f₎).\n* Mid novelty (0.12 ≤ N ≤ 0.40) with -0.24 penalty while $100 stays."},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := FormatDiscordLatex(tc.in); got != tc.want {
				t.Errorf("FormatDiscordLatex(%q)\ngot:  %q\nwant: %q", tc.in, got, tc.want)
			}
		})
	}
}
