package delivery

import "testing"

func TestFormatDiscordLatex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "PlainTextFastPath",
			in:   "Hello world, no math or dollars here!",
			want: "Hello world, no math or dollars here!",
		},
		{
			name: "CurrencyTickersAndEnvVarsPreserved",
			in:   "From $100 to $200 while $SOL and $BTC rallied and $PATH_VAR stayed set.",
			want: "From $100 to $200 while $SOL and $BTC rallied and $PATH_VAR stayed set.",
		},
		{
			name: "EscapedAndUnclosedDollarsPreserved",
			in:   "Keep \\$100 and \\$200 or a trailing $100 with bare $.",
			want: "Keep \\$100 and \\$200 or a trailing $100 with bare $.",
		},
		{
			name: "CurrencyAdjacentToParenthesizedMath",
			in:   "Saved $100 ($\\lambda = 0.7$) and $200 ($0.12 \\le N \\le 0.40$).",
			want: "Saved $100 (λ = 0.7) and $200 (0.12 ≤ N ≤ 0.40).",
		},
		{
			name: "SingleLetterInlineMath",
			in:   "Candidate memory $m$ in set $S$.",
			want: "Candidate memory m in set S.",
		},
		{
			name: "FunctionCallInlineMath",
			in:   "* **$T(m)$ — Prior**: boosts $T(m)$ and $N(m)$.",
			want: "* **T(m) — Prior**: boosts T(m) and N(m).",
		},
		{
			name: "InlineGreekAndComparisonOperators",
			in:   "Using $\\lambda = 0.7$ with $\\Delta x \\le \\epsilon$ and $s \\in S$.",
			want: "Using λ = 0.7 with Δ x ≤ ε and s ∈ S.",
		},
		{
			name: "DigitLeadingInlineMathWithLatexCommands",
			in:   "Mid novelty ($0.12 \\le N \\le 0.40$) and $1 - \\max_{f \\in \\text{DB}} \\text{CosineSim}(e_m, e_f)$.",
			want: "Mid novelty (0.12 ≤ N ≤ 0.40) and 1 - max₍f ∈ DB₎ CosineSim(eₘ, e₍f₎).",
		},
		{
			name: "DigitLeadingInlineMathWithUnicodeOperators",
			in:   "Margin of $10 ± 1$ and $2 × 3$.",
			want: "Margin of 10 ± 1 and 2 × 3.",
		},
		{
			name: "SignedNumericInlineMath",
			in:   "Takes a $-0.24$ diversity penalty.",
			want: "Takes a -0.24 diversity penalty.",
		},
		{
			name: "SuperscriptsAndSubscriptsMappedAndFallback",
			in:   "Convert $\\theta^2 + x_{10} + e^{-\\lambda t} + w_T$.",
			want: "Convert θ² + x₁₀ + e⁽-λ t⁾ + w₍T₎.",
		},
		{
			name: "FontAndTextWrappersUnwrapped",
			in:   "Score $\\mathcal{S}(m) + \\mathbb{E}[x] + \\mathbf{v} + \\text{MMR}(c)$.",
			want: "Score S(m) + E[x] + v + MMR(c).",
		},
		{
			name: "FractionsAndSquareRoots",
			in:   "Ratio $\\frac{\\text{cost}_{10}}{c} \\approx \\sqrt{x^2 + y_0}$.",
			want: "Ratio (cost₁₀)/(c) ≈ √(x² + y₀).",
		},
		{
			name: "InlineParensAndSpacingCommands",
			in:   "Inline \\(\\alpha \\times \\beta \\ne \\infty \\, \\; \\unknown\\).",
			want: "Inline α × β ≠ ∞     \\unknown.",
		},
		{
			name: "DisplayMathDoubleDollarToCodeBlock",
			in:   "Formula:\n$$\\mathcal{S}(m) = w_T T(m) + w_N N(m)$$\nDone.",
			want: "Formula:\n```\nS(m) = w₍T₎ T(m) + w₍N₎ N(m)\n```\nDone.",
		},
		{
			name: "DisplayMathBracketsToCodeBlock",
			in:   "Block:\n\\[\\text{MMR}(c) = 0.7 \\cdot \\text{Relevance}(c, Q)\\]\nDone.",
			want: "Block:\n```\nMMR(c) = 0.7 · Relevance(c, Q)\n```\nDone.",
		},
		{
			name: "InlineCodeAndFencedCodeBlocksMasked",
			in:   "Keep `$\\lambda = 0.7$` and:\n```tex\n$$\\frac{1}{2}$$\n```\nbut convert $x = 1$.",
			want: "Keep `$\\lambda = 0.7$` and:\n```tex\n$$\\frac{1}{2}$$\n```\nbut convert x = 1.",
		},
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
