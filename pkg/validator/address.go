package validator

import (
	"regexp"
	"strings"
)

var addressRegex = regexp.MustCompile("^0x[a-fA-F0-9]{40}$")

// IsValidAddress checks if the given string is a valid Ethereum address
func IsValidAddress(address string) bool {
	return addressRegex.MatchString(address)
}

// NormalizeAddress converts an address to lowercase
func NormalizeAddress(address string) string {
	return strings.ToLower(address)
}

// IsZeroAddress checks if the address is the zero address
func IsZeroAddress(address string) bool {
	return NormalizeAddress(address) == "0x0000000000000000000000000000000000000000"
}

// TruncateAddress returns a truncated address for display (0x1234...abcd)
func TruncateAddress(address string, prefixLen, suffixLen int) string {
	if len(address) < prefixLen+suffixLen+3 {
		return address
	}
	return address[:prefixLen] + "..." + address[len(address)-suffixLen:]
}
