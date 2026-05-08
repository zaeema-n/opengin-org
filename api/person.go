package api

import (
	"fmt"
	"strings"
	"time"

	"orgchart_nexoan/internal/utils"
	"orgchart_nexoan/models"

	"github.com/google/uuid"
)

// AddPersonEntity creates a new person entity and establishes its relationship with a parent entity.
// Assumes the parent entity already exists.
func (c *Client) AddPersonEntity(transaction map[string]interface{}, entityCounters map[string]int) (int, error) {
	// Extract details from the transaction
	parent := transaction["parent"].(string)
	child := transaction["child"].(string)
	dateStr := transaction["date"].(string)
	parentType := transaction["parent_type"].(string)
	childType := transaction["child_type"].(string)
	relType := transaction["rel_type"].(string)
	transactionID := transaction["transaction_id"].(string)

	// Get president name if parent is a minister -> currently only supports adding people to ministers
	var presidentName string
	if isMinisterType(parentType) {
		var ok bool
		presidentName, ok = transaction["president"].(string)
		if !ok || presidentName == "" {
			return 0, fmt.Errorf("president name is required and must be a non-empty string when adding a person to a minister")
		}
	}

	// Parse the date
	date, err := time.Parse("2006-01-02", strings.TrimSpace(dateStr))
	if err != nil {
		return 0, fmt.Errorf("failed to parse date: %w", err)
	}
	dateISO := date.Format(time.RFC3339)

	// Get the parent entity ID
	var parentID string

	if isMinisterType(parentType) {
		// Parent is a minister, need president context to get the correct minister
		ministerEntity, err := c.GetActiveMinisterByPresident(presidentName, parent, dateISO)
		if err != nil {
			return 0, fmt.Errorf("failed to get parent minister entity: %w", err)
		}
		parentID = ministerEntity.ID
	} else {
		// For other parent types, use the original logic
		searchCriteria := &models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Organisation",
				Minor: parentType,
			},
			Name: parent,
		}

		searchResults, err := c.SearchEntities(searchCriteria)
		if err != nil {
			return 0, fmt.Errorf("failed to search for parent entity: %w", err)
		}
		// Filter for exact name match
		searchResults = utils.FilterByExactName(searchResults, parent)
		if len(searchResults) == 0 {
			return 0, fmt.Errorf("parent entity not found: %s", parent)
		}
		if len(searchResults) > 1 {
			return 0, fmt.Errorf("multiple parent entities found with name '%s'", parent)
		}
		parentID = searchResults[0].ID
	}

	// Check if person already exists (search across all person types)
	personSearchCriteria := &models.SearchCriteria{
		Kind: &models.Kind{
			Major: "Person",
		},
		Name: child,
	}

	personResults, err := c.SearchEntities(personSearchCriteria)
	if err != nil {
		return 0, fmt.Errorf("failed to search for person entity: %w", err)
	}
	// Filter for exact name match
	personResults = utils.FilterByExactName(personResults, child)
	if len(personResults) > 1 {
		return 0, fmt.Errorf("multiple entities found for person: %s", child)
	}

	var childID string
	if len(personResults) == 1 {
		// Person exists, use existing ID
		childID = personResults[0].ID
	} else {
		// Generate new entity ID
		if _, exists := entityCounters[childType]; !exists {
			return 0, fmt.Errorf("unknown child type: %s", childType)
		}

		// Get the part before the first underscore for the prefix
		prefixPart := strings.Split(transactionID, "_")[0]
		prefix := fmt.Sprintf("%s_%s", prefixPart, strings.ToLower(childType[:3]))
		entityCounters[childType]++ // Increment the counter
		newEntityID := fmt.Sprintf("%s_%d", prefix, entityCounters[childType])

		// Create the new child entity
		childEntity := &models.Entity{
			ID: newEntityID,
			Kind: models.Kind{
				Major: "Person",
				Minor: childType,
			},
			Created:    dateISO,
			Terminated: "",
			Name: models.TimeBasedValue{
				StartTime: dateISO,
				Value:     child,
			},
			Metadata:      []models.MetadataEntry{},
			Attributes:    []models.AttributeEntry{},
			Relationships: []models.RelationshipEntry{},
		}

		// Create the child entity
		createdChild, err := c.CreateEntity(childEntity)
		if err != nil {
			return 0, fmt.Errorf("failed to create child entity: %w", err)
		}
		childID = createdChild.ID
	}

	// Update the parent entity to add the relationship to the child
	// Use transaction ID and current timestamp to ensure unique relationship ID
	currentTimestamp := fmt.Sprintf("%s_%s", strings.ReplaceAll(time.Now().Format(time.RFC3339), ":", "-"), uuid.New().String()[:8])
	uniqueRelationshipID := fmt.Sprintf("%s_%s_%s", parentID, childID, currentTimestamp)

	parentEntity := &models.Entity{
		ID:         parentID,
		Kind:       models.Kind{},
		Created:    "",
		Terminated: "",
		Name:       models.TimeBasedValue{},
		Metadata:   []models.MetadataEntry{},
		Attributes: []models.AttributeEntry{},
		Relationships: []models.RelationshipEntry{
			{
				Key: uniqueRelationshipID,
				Value: models.Relationship{
					RelatedEntityID: childID,
					StartTime:       dateISO,
					EndTime:         "",
					ID:              uniqueRelationshipID,
					Name:            relType,
				},
			},
		},
	}

	_, err = c.UpdateEntity(parentID, parentEntity)
	if err != nil {
		return 0, fmt.Errorf("failed to update parent entity: %w", err)
	}

	return entityCounters[childType], nil
}

// TerminatePersonEntity terminates a specific relationship between Person type entity and another entity at a given date
func (c *Client) TerminatePersonEntity(transaction map[string]interface{}) error {
	// Extract details from the transaction
	parent := transaction["parent"].(string)
	child := transaction["child"].(string)
	dateStr := transaction["date"].(string)
	parentType := transaction["parent_type"].(string)
	childType := transaction["child_type"].(string)
	relType := transaction["rel_type"].(string)

	// Get president name if parent is a minister -> currently only supports terminating relationships with ministers
	var presidentName string
	if isMinisterType(parentType) {
		var ok bool
		presidentName, ok = transaction["president"].(string)
		if !ok || presidentName == "" {
			return fmt.Errorf("president name is required and must be a non-empty string when terminating relationships with ministers")
		}
	}

	// Parse the date
	date, err := time.Parse("2006-01-02", strings.TrimSpace(dateStr))
	if err != nil {
		return fmt.Errorf("failed to parse date: %w", err)
	}
	dateISO := date.Format(time.RFC3339)

	// First, find the person (child) entity
	childSearchCriteria := &models.SearchCriteria{
		Kind: &models.Kind{
			Major: "Person",
			Minor: childType,
		},
		Name: child,
	}

	childResults, err := c.SearchEntities(childSearchCriteria)
	if err != nil {
		return fmt.Errorf("failed to search for child entity: %w", err)
	}
	// Filter for exact name match
	childResults = utils.FilterByExactName(childResults, child)
	if len(childResults) == 0 {
		return fmt.Errorf("child entity not found: %s", child)
	}
	if len(childResults) > 1 {
		return fmt.Errorf("multiple child entities found with name '%s'", child)
	}
	childID := childResults[0].ID

	// Find the ministry by checking the person's active relationships
	var parentID string
	var activeRel *models.Relationship

	if isMinisterType(parentType) {
		// Get all active relationships from the person to find the ministry
		personRelations, err := c.GetRelatedEntities(childID, &models.Relationship{
			Name: relType,
		})
		if err != nil {
			return fmt.Errorf("failed to get person's relationships: %w", err)
		}

		// Find the active relationship to the specified ministry
		for _, rel := range personRelations {
			if rel.EndTime == "" { // Only active relationships
				// Get the ministry entity to check if it matches the parent name
				ministryResults, err := c.SearchEntities(&models.SearchCriteria{
					ID: rel.RelatedEntityID,
				})
				if err != nil || len(ministryResults) == 0 {
					continue
				}

				ministry := ministryResults[0]
				// Check if this ministry is under the correct president and matches the parent name
				if isMinisterType(ministry.Kind.Minor) && ministry.Name == parent {
					// Verify this minister is under the specified president
					_, err = c.GetActiveMinisterByPresident(presidentName, parent, dateISO)
					if err == nil {
						parentID = ministry.ID
						// We found the ministry, now we need to get the relationship from ministry to person
						// to get the relationship ID for termination
						ministryRelations, err := c.GetRelatedEntities(ministry.ID, &models.Relationship{
							Name:            relType,
							RelatedEntityID: childID,
						})
						if err != nil {
							return fmt.Errorf("failed to get ministry's relationship to person: %w", err)
						}

						// Find the active relationship
						for _, mRel := range ministryRelations {
							if mRel.EndTime == "" {
								activeRel = &mRel
								break
							}
						}
						break
					}
				}
			}
		}

		if parentID == "" {
			return fmt.Errorf("no active relationship found between person '%s' (ID: %s) and ministry '%s' under president '%s'", child, childID, parent, presidentName)
		}
	} else {
		// For other parent types, use the original logic
		searchCriteria := &models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Organisation",
				//Minor: parentType,
			},
			Name: parent,
		}
		parentResults, err := c.SearchEntities(searchCriteria)
		if err != nil {
			return fmt.Errorf("failed to search for parent entity: %w", err)
		}
		// Filter for exact name match
		parentResults = utils.FilterByExactName(parentResults, parent)
		if len(parentResults) == 0 {
			return fmt.Errorf("parent entity not found: %s", parent)
		}
		if len(parentResults) > 1 {
			return fmt.Errorf("multiple parent entities found with name '%s'", parent)
		}
		parentID = parentResults[0].ID
	}

	// If we haven't found the active relationship yet (for non-minister parent types), search for it
	if activeRel == nil {
		// Get the specific relationship that is still active (no end date)
		relations, err := c.GetRelatedEntities(parentID, &models.Relationship{
			RelatedEntityID: childID,
			Name:            relType,
		})
		if err != nil {
			return fmt.Errorf("failed to get relationship: %w", err)
		}

		// FIXME: Is it possible to have more than one active relationship? For orgchart case only it won't happen
		// Manually filter only active relationships (EndTime == "")
		var activeRelations []models.Relationship
		for _, rel := range relations {
			if rel.EndTime == "" {
				activeRelations = append(activeRelations, rel)
			}
		}

		// Find the active relationship (no end time)
		if len(activeRelations) > 0 {
			activeRel = &activeRelations[0]
		}
	}

	if activeRel == nil {
		return fmt.Errorf("no active relationship found between %s and %s with type %s", parentID, childID, relType)
	}

	// Update the relationship to set the end date
	_, err = c.UpdateEntity(parentID, &models.Entity{
		ID: parentID,
		Relationships: []models.RelationshipEntry{
			{
				Key: activeRel.ID,
				Value: models.Relationship{
					EndTime: dateISO,
					ID:      activeRel.ID,
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to terminate relationship: %w", err)
	}

	return nil
}

// MovePerson moves a person from one portfolio to another (limits functionality to only minister)
// TODO: Take the parent type from the transaction such that this function can be used generic
//
//	for moving person from any institution to another
func (c *Client) MovePerson(transaction map[string]interface{}) error {
	// Extract details from the transaction
	newParent := transaction["new_parent"].(string)
	oldParent := transaction["old_parent"].(string)
	child := transaction["child"].(string)
	dateStr := transaction["date"].(string)
	relType := "AS_APPOINTED"

	// Validate president name is provided
	presidentName, ok := transaction["president"].(string)
	if !ok || presidentName == "" {
		return fmt.Errorf("president name is required and must be a non-empty string")
	}

	// Parse the date
	date, err := time.Parse("2006-01-02", strings.TrimSpace(dateStr))
	if err != nil {
		return fmt.Errorf("failed to parse date: %w", err)
	}
	dateISO := date.Format(time.RFC3339)

	// Get the new minister (parent) entity ID -> only supports moving person to and from minister
	newParentEntity, err := c.GetActiveMinisterByPresident(presidentName, newParent, dateISO)
	if err != nil {
		return fmt.Errorf("failed to get new parent entity: %w", err)
	}
	newParentID := newParentEntity.ID

	// Get the department (child) entity ID
	childResults, err := c.SearchEntities(&models.SearchCriteria{
		Kind: &models.Kind{
			Major: "Person",
			Minor: "citizen",
		},
		Name: child,
	})
	if err != nil {
		return fmt.Errorf("failed to search for child entity: %w", err)
	}
	// Filter for exact name match
	childResults = utils.FilterByExactName(childResults, child)
	if len(childResults) == 0 {
		return fmt.Errorf("child entity not found: %s", child)
	}
	if len(childResults) > 1 {
		return fmt.Errorf("multiple child entities found with name '%s'", child)
	}
	childID := childResults[0].ID

	// Create new relationship between new minister and person
	// Use transaction ID and current timestamp to ensure unique relationship ID
	currentTimestamp := fmt.Sprintf("%s_%s", strings.ReplaceAll(time.Now().Format(time.RFC3339), ":", "-"), uuid.New().String()[:8])
	uniqueRelationshipID := fmt.Sprintf("%s_%s_%s", newParentID, childID, currentTimestamp)

	newRelationship := &models.Entity{
		ID: newParentID,
		Relationships: []models.RelationshipEntry{
			{
				Key: uniqueRelationshipID,
				Value: models.Relationship{
					RelatedEntityID: childID,
					StartTime:       dateISO,
					EndTime:         "",
					ID:              uniqueRelationshipID,
					Name:            relType,
				},
			},
		},
	}

	_, err = c.UpdateEntity(newParentID, newRelationship)
	if err != nil {
		return fmt.Errorf("failed to create new relationship: %w", err)
	}

	// Terminate the old relationship
	terminateTransaction := map[string]interface{}{
		"parent":      oldParent,
		"child":       child,
		"date":        dateStr,
		"parent_type": ministerTypeFromName(oldParent),
		"child_type":  "citizen",
		"rel_type":    relType,
		"president":   presidentName,
	}

	err = c.TerminatePersonEntity(terminateTransaction)
	if err != nil {
		return fmt.Errorf("failed to terminate old relationship: %w", err)
	}

	return nil
}
