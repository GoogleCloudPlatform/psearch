package services

import (
	"unicode"

	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// FoldAccents removes diacritics ("Tênis" -> "Tenis") so queries match the
// accent-folded tokenlists created by migration 014b. It mirrors the
// Spanner expression REGEXP_REPLACE(NORMALIZE(x, NFD), r'\p{M}', ”).
func FoldAccents(s string) string {
	t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	out, _, err := transform.String(t, s)
	if err != nil {
		return s
	}
	return out
}
