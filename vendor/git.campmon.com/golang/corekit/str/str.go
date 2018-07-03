package str // import "git.campmon.com/golang/corekit/str"

import (
	"strings"
	"unicode/utf8"
)

// IsEmpty returns true if the length of trimmed input is greater than zero
func IsEmpty(input string) bool {
	return len(strings.TrimSpace(input)) == 0
}

// SanitizeUTF8 replace invalid UTF-8 code point in the given string to utf8.RuneError, which is a replacement character
// refer to https://www.fileformat.info/info/unicode/char/fffd/index.htm
func SanitizeUTF8(s string) string {
	return ReplaceInvalidUTF8With(s, utf8.RuneError)
}

// ReplaceInvalidUTF8With replace invalid UTF-8 code point in the given string to the given rune
func ReplaceInvalidUTF8With(s string, r rune) string {
	if utf8.ValidString(s) {
		return s
	}
	v := make([]rune, 0, len(s)+utf8.UTFMax)
	i := 0
	for {
		ru, size := utf8.DecodeRuneInString(s[i:])
		if size == 0 {
			break
		}
		i += size
		if ru == utf8.RuneError {
			v = append(v, r)
			continue
		}
		v = append(v, ru)
	}
	return string(v)
}

// RemoveInvalidUTF8 remove invalid utf8 code point from the string
func RemoveInvalidUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	return strings.Map(func(r rune) rune {
		if r == utf8.RuneError {
			return -1
		}
		return r
	}, s)
}
