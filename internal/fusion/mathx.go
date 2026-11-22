package fusion

import (
	"math"
	"math/bits"
)

// Integer helpers. All fusion arithmetic is integer so results are exact
// and identical on every platform; products that could exceed 64 bits use
// 128-bit intermediates.

// mulDiv returns a*b/c rounded to nearest, for a, b >= 0 and c > 0,
// saturating at math.MaxInt64.
func mulDiv(a, b, c int64) int64 {
	if a <= 0 || b <= 0 {
		return 0
	}
	hi, lo := bits.Mul64(uint64(a), uint64(b))
	var carry uint64
	lo, carry = bits.Add64(lo, uint64(c)/2, 0)
	hi += carry
	if hi >= uint64(c) {
		return math.MaxInt64
	}
	q, _ := bits.Div64(hi, lo, uint64(c))
	if q > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(q)
}

// combine merges two independent estimates (x1, v1) and (x2, v2) by inverse
// variance weighting. x values must be >= 0, variances > 0.
func combine(x1, v1, x2, v2 int64) (x, v int64) {
	den := uint64(v1) + uint64(v2)
	h1, l1 := bits.Mul64(uint64(x1), uint64(v2))
	h2, l2 := bits.Mul64(uint64(x2), uint64(v1))
	lo, c := bits.Add64(l1, l2, 0)
	hi, _ := bits.Add64(h1, h2, c)
	lo, c = bits.Add64(lo, den/2, 0)
	hi += c
	q, _ := bits.Div64(hi, lo, den) // q <= max(x1, x2), so hi < den
	return int64(q), max(1, mulDiv(v1, v2, int64(den)))
}

// isqrt returns floor(sqrt(n)) for n >= 0.
func isqrt(n int64) int64 {
	if n <= 0 {
		return 0
	}
	x := int64(math.Sqrt(float64(n))) // initial guess, corrected below
	for x*x > n {
		x--
	}
	for (x+1)*(x+1) <= n {
		x++
	}
	return x
}

// sigmaVariance returns sigma squared, saturating so sums stay in range.
func sigmaVariance(sigma int64) int64 {
	if sigma > 1<<30 {
		sigma = 1 << 30
	}
	return max(1, sigma*sigma)
}

func absDiff(a, b int64) int64 {
	if a > b {
		return a - b
	}
	return b - a
}
