package common

// B2S converts boolean to string
func B2S(val bool) string {
	if val {
		return "YES"
	}
	return "NO"
}
