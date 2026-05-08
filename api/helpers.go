package api

import (
	"strings"
)

// isMinisterType returns true for any minister subtype value.
func isMinisterType(t string) bool {
	switch t {
	case "cabinetMinister", "stateMinister":
		return true
	}
	return false
}

// ministerTypeFromName derives the correct minister subtype from the minister's name.
func ministerTypeFromName(name string) string {
	lowerName := strings.ToLower(name)
	if strings.HasPrefix(lowerName, "state minister") || strings.HasPrefix(lowerName, "non cabinet minister") {
		return "stateMinister"
	}
	return "cabinetMinister"
}
