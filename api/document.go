package api

import (
	"fmt"
	"strings"
	"time"

	"orgchart_nexoan/internal/utils"
	"orgchart_nexoan/models"

	"github.com/google/uuid"
)

// Document Entity Handling
// Unlike other entities, Documents are not terminated, but there is an aspect to a document being
// regarded in various states. So this needs to be thoroughly thought and represented in the system.
// For now we are only adding the documents and not maintaining any other states.

// AddDocumentEntity creates a new document entity and establishes its relationship with a parent entity.
// The document type is determined by the parent entity type (Organization or Person).
// Assumes the parent entity already exists.
func (c *Client) AddDocumentEntity(transaction map[string]interface{}, entityCounters map[string]int) (int, error) {
	// Extract details from the transaction with validation
	parent, ok := transaction["parent"].(string)
	if !ok || parent == "" {
		return 0, fmt.Errorf("parent is required and must be a string")
	}

	child, ok := transaction["child"].(string)
	if !ok || child == "" {
		return 0, fmt.Errorf("child is required and must be a string")
	}

	dateStr, ok := transaction["date"].(string)
	if !ok || dateStr == "" {
		return 0, fmt.Errorf("date is required and must be a string")
	}

	parentType, ok := transaction["parent_type"].(string)
	if !ok || parentType == "" {
		return 0, fmt.Errorf("parent_type is required and must be a string")
	}

	childType, ok := transaction["child_type"].(string)
	if !ok || childType == "" {
		return 0, fmt.Errorf("child_type is required and must be a string")
	}

	transactionID, ok := transaction["transaction_id"].(string)
	if !ok || transactionID == "" {
		return 0, fmt.Errorf("transaction_id is required and must be a string")
	}

	// Parse the date
	date, err := time.Parse("2006-01-02", strings.TrimSpace(dateStr))
	if err != nil {
		return 0, fmt.Errorf("failed to parse date: %w", err)
	}
	dateISO := date.Format(time.RFC3339)

	// Get the parent entity ID (which is always gonna be an organisation)
	searchCriteria := &models.SearchCriteria{
		Name: parent,
		Kind: &models.Kind{
			Major: "Organisation",
			Minor: parentType,
		},
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
	parentID := searchResults[0].ID

	// Check if document already exists
	documentSearchCriteria := &models.SearchCriteria{
		Kind: &models.Kind{
			Major: "Document",
			Minor: childType,
		},
		Name: child,
	}

	documentResults, err := c.SearchEntities(documentSearchCriteria)
	if err != nil {
		return 0, fmt.Errorf("failed to search for document entity: %w", err)
	}
	// Filter for exact name match
	documentResults = utils.FilterByExactName(documentResults, child)
	if len(documentResults) > 1 {
		return 0, fmt.Errorf("multiple entities found for document: %s", child)
	}

	var childID string
	entityCounter := 0
	if len(documentResults) == 1 {
		// Document exists, use existing ID
		childID = documentResults[0].ID
	} else {
		// Generate new entity ID
		// Get the part before the first underscore for the prefix
		prefixPart := strings.Split(transactionID, "_")[0]
		prefix := fmt.Sprintf("%s_doc", prefixPart)
		entityCounter = entityCounters["document"] + 1
		newEntityID := fmt.Sprintf("%s_%d", prefix, entityCounter)

		// Create the new document entity
		documentEntity := &models.Entity{
			ID: newEntityID,
			Kind: models.Kind{
				Major: "Document",
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

		// Create the document entity
		createdDocument, err := c.CreateEntity(documentEntity)
		if err != nil {
			return 0, fmt.Errorf("failed to create document entity: %w", err)
		}
		childID = createdDocument.ID
	}

	// Update the parent entity to add the relationship to the document
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
					Name:            "AS_DOCUMENT",
				},
			},
		},
	}

	_, err = c.UpdateEntity(parentID, parentEntity)
	if err != nil {
		return 0, fmt.Errorf("failed to update parent entity: %w", err)
	}

	return entityCounter, nil
}
