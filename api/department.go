package api

import (
	"fmt"
	"strings"
	"time"

	"orgchart_nexoan/internal/utils"
	"orgchart_nexoan/models"

	"github.com/google/uuid"
)

// MoveDepartment moves a department from one minister to another
// MoveDepartment moves a department to a new minister
func (c *Client) MoveDepartment(transaction map[string]interface{}) error {
	// Extract details from the transaction
	newParent := transaction["new_parent"].(string)
	child := transaction["child"].(string)
	dateStr := transaction["date"].(string)

	// Parse the date
	date, err := time.Parse("2006-01-02", strings.TrimSpace(dateStr))
	if err != nil {
		return fmt.Errorf("failed to parse date: %w", err)
	}
	dateISO := date.Format(time.RFC3339)

	// Search for the department by name
	departmentResults, err := c.SearchEntities(&models.SearchCriteria{
		Kind: &models.Kind{
			Major: "Organisation",
			Minor: "department",
		},
		Name: child,
	})
	if err != nil {
		return fmt.Errorf("failed to search for department: %w", err)
	}
	// Filter for exact name match
	departmentResults = utils.FilterByExactName(departmentResults, child)
	if len(departmentResults) == 0 {
		return fmt.Errorf("department '%s' not found", child)
	}
	if len(departmentResults) > 1 {
		return fmt.Errorf("multiple departments found with name '%s'", child)
	}
	departmentID := departmentResults[0].ID

	// Check for active incoming relationships to this department
	// Get all relationships where this department is the target
	departmentRelations, err := c.GetRelatedEntities(departmentID, &models.Relationship{
		Name: "AS_DEPARTMENT",
	})
	if err != nil {
		return fmt.Errorf("failed to get department relationships: %w", err)
	}

	// Look for active AS_DEPARTMENT relationships coming into this department
	for _, rel := range departmentRelations {
		if rel.EndTime == "" {
			// Found an active relationship - terminate it directly
			// We have the minister ID (rel.RelatedEntityID) and can terminate the relationship directly
			terminateRelationship := &models.Entity{
				ID: rel.RelatedEntityID, // This is the minister ID
				Relationships: []models.RelationshipEntry{
					{
						Key: rel.ID,
						Value: models.Relationship{
							EndTime: dateISO,
							ID:      rel.ID,
						},
					},
				},
			}

			_, err = c.UpdateEntity(rel.RelatedEntityID, terminateRelationship)
			if err != nil {
				return fmt.Errorf("failed to terminate old relationship: %w", err)
			}
			// Continue to terminate all active relationships, don't break
		}
	}

	// Get the new minister entity ID by president
	// We need the president name to get the correct minister
	newPresidentName, ok := transaction["new_president_name"].(string)
	if !ok || newPresidentName == "" {
		return fmt.Errorf("new_president_name is required and must be a non-empty string")
	}

	newMinisterEntity, err := c.GetActiveMinisterByPresident(newPresidentName, newParent, dateISO)
	if err != nil {
		return fmt.Errorf("failed to get new minister '%s' under president '%s': %w", newParent, newPresidentName, err)
	}
	newMinisterID := newMinisterEntity.ID

	// Create new AS_DEPARTMENT relationship from new minister to department
	// Use transaction ID and timestamp to ensure unique relationship ID
	currentTimestamp := fmt.Sprintf("%s_%s", strings.ReplaceAll(time.Now().Format(time.RFC3339), ":", "-"), uuid.New().String()[:8])
	uniqueRelationshipID := fmt.Sprintf("%s_%s_%s", newMinisterID, departmentID, currentTimestamp)

	newRelationship := &models.Entity{
		ID: newMinisterID,
		Relationships: []models.RelationshipEntry{
			{
				Key: uniqueRelationshipID,
				Value: models.Relationship{
					RelatedEntityID: departmentID,
					StartTime:       dateISO,
					EndTime:         "",
					ID:              uniqueRelationshipID,
					Name:            "AS_DEPARTMENT",
				},
			},
		},
	}

	_, err = c.UpdateEntity(newMinisterID, newRelationship)
	if err != nil {
		return fmt.Errorf("failed to create new relationship: %w", err)
	}

	return nil
}

// RenameDepartment renames a department and transfers all its people relationships to the new department
func (c *Client) RenameDepartment(transaction map[string]interface{}, entityCounters map[string]int) (int, error) {
	// Extract details from the transaction
	oldName := transaction["old"].(string)
	newName := transaction["new"].(string)
	dateStr := transaction["date"].(string)
	relType := "AS_DEPARTMENT"
	transactionID := transaction["transaction_id"].(string)
	presidentName, ok := transaction["president"].(string)
	if !ok || presidentName == "" {
		return 0, fmt.Errorf("president name is required and must be a non-empty string when renaming a department")
	}

	// Parse the date
	date, err := time.Parse("2006-01-02", strings.TrimSpace(dateStr))
	if err != nil {
		return 0, fmt.Errorf("failed to parse date: %w", err)
	}
	dateISO := date.Format(time.RFC3339)

	// Get the old department's ID
	oldDepartmentResults, err := c.SearchEntities(&models.SearchCriteria{
		Kind: &models.Kind{
			Major: "Organisation",
			Minor: "department",
		},
		Name: oldName,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to search for old department: %w", err)
	}
	// Filter for exact name match
	oldDepartmentResults = utils.FilterByExactName(oldDepartmentResults, oldName)
	if len(oldDepartmentResults) == 0 {
		return 0, fmt.Errorf("old department not found: %s", oldName)
	}
	if len(oldDepartmentResults) > 1 {
		return 0, fmt.Errorf("multiple departments found with name '%s'", oldName)
	}
	oldDepartmentID := oldDepartmentResults[0].ID

	// Check if the new department name already exists
	existingDepartmentResults, err := c.SearchEntities(&models.SearchCriteria{
		Kind: &models.Kind{
			Major: "Organisation",
			Minor: "department",
		},
		Name: newName,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to search for new department name: %w", err)
	}
	// Filter for exact name match
	existingDepartmentResults = utils.FilterByExactName(existingDepartmentResults, newName)

	var newDepartmentID string
	var newDepartmentCounter int

	if len(existingDepartmentResults) > 0 {
		if len(existingDepartmentResults) > 1 {
			return 0, fmt.Errorf("multiple departments found with name '%s'", newName)
		}
		// Check if the existing department has any active AS_DEPARTMENT relationships
		existingDepartment := existingDepartmentResults[0]
		existingDepartmentID := existingDepartment.ID

		// Get all AS_DEPARTMENT relationships for this department
		existingDepartmentRelations, err := c.GetRelatedEntities(existingDepartmentID, &models.Relationship{
			Name: "AS_DEPARTMENT",
		})
		if err != nil {
			return 0, fmt.Errorf("failed to get existing department relationships: %w", err)
		}

		// Check if any relationships are still active (EndTime == "")
		hasActiveRelationships := false
		for _, rel := range existingDepartmentRelations {
			if rel.EndTime == "" {
				hasActiveRelationships = true
				break
			}
		}

		if hasActiveRelationships {
			// Department exists and has active relationships, cannot proceed
			return 0, fmt.Errorf("department with name '%s' already exists and has active relationships", newName)
		} else {
			// Department exists but all relationships are terminated, we can reuse it
			newDepartmentID = existingDepartment.ID
			//newDepartmentCounter = 0 // No new entity created, just reusing existing
		}
	} else {
		// Department doesn't exist, we'll create a new one
		newDepartmentID = ""
		//newDepartmentCounter = 0
	}

	// Get all active relationships coming into this department
	// The department can have multiple active relationships to different ministers from different presidents
	departmentRelations, err := c.GetRelatedEntities(oldDepartmentID, &models.Relationship{
		Name: "AS_DEPARTMENT",
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get department relationships: %w", err)
	}

	// Find the minister that has an active relationship to this department under the specified president
	var ministerID string
	var ministerName string
	for _, rel := range departmentRelations {
		if rel.EndTime == "" {
			// This is an active relationship, check if the minister is under the correct president
			ministerResults, err := c.SearchEntities(&models.SearchCriteria{ID: rel.RelatedEntityID})
			if err != nil || len(ministerResults) == 0 {
				continue
			}
			minister := ministerResults[0]

			// Check if this minister is under the specified president
			_, err = c.GetActiveMinisterByPresident(presidentName, minister.Name, dateISO)
			if err == nil {
				// Found the minister under the correct president
				ministerID = minister.ID
				ministerName = minister.Name
				break
			}
		}
	}

	if ministerID == "" {
		return 0, fmt.Errorf("no active minister relationship found for department '%s' under president '%s'", oldName, presidentName)
	}

	// Verify that this minister is under the correct president
	// _, err = c.GetMinisterByPresident(presidentName, ministerName, dateISO)
	// if err != nil {
	// 	return 0, fmt.Errorf("minister '%s' not found under president '%s'", ministerName, presidentName)
	// }

	// Create new department or reuse existing inactive department
	if newDepartmentID == "" {
		// Create new department under the same minister
		addEntityTransaction := map[string]interface{}{
			"parent":         ministerName,
			"child":          newName,
			"date":           dateStr,
			"parent_type":    ministerTypeFromName(ministerName),
			"child_type":     "department",
			"rel_type":       relType,
			"transaction_id": transactionID,
			"president":      presidentName,
		}

		// Create the new department
		newDepartmentCounter, err = c.AddOrgEntity(addEntityTransaction, entityCounters)
		if err != nil {
			return 0, fmt.Errorf("failed to create new department: %w", err)
		}

		// Get the new department's ID
		newDepartmentResults, err := c.SearchEntities(&models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Organisation",
				Minor: "department",
			},
			Name: newName,
		})
		if err != nil {
			return 0, fmt.Errorf("failed to search for new department: %w", err)
		}
		// Filter for exact name match
		newDepartmentResults = utils.FilterByExactName(newDepartmentResults, newName)
		if len(newDepartmentResults) == 0 {
			return 0, fmt.Errorf("new department not found: %s", newName)
		}
		if len(newDepartmentResults) > 1 {
			return 0, fmt.Errorf("multiple departments found with name '%s'", newName)
		}
		newDepartmentID = newDepartmentResults[0].ID
	} else {
		// Reusing existing inactive department - create the relationship with the minister
		// Generate a unique relationship ID
		newDepartmentCounter = entityCounters["department"]
		currentTimestamp := fmt.Sprintf("%s_%s", strings.ReplaceAll(time.Now().Format(time.RFC3339), ":", "-"), uuid.New().String()[:8])
		uniqueRelationshipID := fmt.Sprintf("%s_%s_%s", ministerID, newDepartmentID, currentTimestamp)

		// Create the relationship between minister and the reactivated department
		reactivateRelationship := &models.Entity{
			ID: ministerID,
			Relationships: []models.RelationshipEntry{
				{
					Key: uniqueRelationshipID,
					Value: models.Relationship{
						RelatedEntityID: newDepartmentID,
						StartTime:       dateISO,
						EndTime:         "",
						ID:              uniqueRelationshipID,
						Name:            relType,
					},
				},
			},
		}

		_, err = c.UpdateEntity(ministerID, reactivateRelationship)
		if err != nil {
			return 0, fmt.Errorf("failed to create relationship with reactivated department: %w", err)
		}
	}

	// Terminate the old department's relationship with minister directly
	// Get the specific existing relationship to this department
	existingRelations, err := c.GetRelatedEntities(ministerID, &models.Relationship{
		Name:            "AS_DEPARTMENT",
		RelatedEntityID: oldDepartmentID,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get existing relationship: %w", err)
	}

	// Find the active relationship (no end time)
	var existingRel *models.Relationship
	for _, rel := range existingRelations {
		if rel.EndTime == "" {
			existingRel = &rel
			break
		}
	}

	if existingRel == nil {
		return 0, fmt.Errorf("no active relationship found between minister '%s' and department '%s'", ministerID, oldDepartmentID)
	}

	// Terminate the relationship by updating it with the end time
	terminateRelationship := &models.Entity{
		ID: ministerID,
		Relationships: []models.RelationshipEntry{
			{
				Key: existingRel.ID,
				Value: models.Relationship{
					EndTime: dateISO,
					ID:      existingRel.ID,
				},
			},
		},
	}

	_, err = c.UpdateEntity(ministerID, terminateRelationship)
	if err != nil {
		return 0, fmt.Errorf("failed to terminate old department's minister relationship: %w", err)
	}

	// Create RENAMED_TO relationship
	// Use transaction ID and current timestamp to ensure unique relationship ID
	currentTimestamp := fmt.Sprintf("%s_%s", strings.ReplaceAll(time.Now().Format(time.RFC3339), ":", "-"), uuid.New().String()[:8])
	uniqueRelationshipID := fmt.Sprintf("%s_%s_%s", oldDepartmentID, newDepartmentID, currentTimestamp)

	renameRelationship := &models.Entity{
		ID: oldDepartmentID,
		Relationships: []models.RelationshipEntry{
			{
				Key: uniqueRelationshipID,
				Value: models.Relationship{
					RelatedEntityID: newDepartmentID,
					StartTime:       dateISO,
					EndTime:         "",
					ID:              uniqueRelationshipID,
					Name:            "RENAMED_TO",
				},
			},
		},
	}

	_, err = c.UpdateEntity(oldDepartmentID, renameRelationship)
	if err != nil {
		return 0, fmt.Errorf("failed to create RENAMED_TO relationship: %w", err)
	}

	return newDepartmentCounter, nil
}
