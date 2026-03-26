package utils

import "orgchart_nexoan/models"

// FilterByExactName returns only the results whose Name exactly matches name.
func FilterByExactName(results []models.SearchResult, name string) []models.SearchResult {
	var exact []models.SearchResult
	for _, r := range results {
		if r.Name == name {
			exact = append(exact, r)
		}
	}
	return exact
}
