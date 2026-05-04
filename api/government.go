package api

import (
	"fmt"

	"orgchart_nexoan/internal/utils"
	"orgchart_nexoan/models"
)

// CreateGovernmentNode creates the initial government node
func (c *Client) CreateGovernmentNode() (*models.Entity, error) {
	// Create the government entity
	governmentEntity := &models.Entity{
		ID:      "gov_01",
		Created: "1978-09-07T00:00:00Z",
		Kind: models.Kind{
			Major: "Organisation",
			Minor: "government",
		},
		Name: models.TimeBasedValue{
			StartTime: "1978-09-07T00:00:00Z",
			Value:     "Government of Sri Lanka",
		},
	}

	// Create the entity
	createdEntity, err := c.CreateEntity(governmentEntity)
	if err != nil {
		return nil, fmt.Errorf("failed to create government entity: %w", err)
	}

	return createdEntity, nil
}

// GetPresidentByGovernment retrieves a president entity (citizen with AS_PRESIDENT relationship to government) by name
func (c *Client) GetPresidentByGovernment(presidentName string) (*models.Entity, error) {
	// Get the president entity ID - presidents are citizens with AS_PRESIDENT relationship
	presidentResults, err := c.SearchEntities(&models.SearchCriteria{
		Kind: &models.Kind{
			Major: "Person",
			Minor: "citizen",
		},
		Name: presidentName,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to search for president entity: %w", err)
	}

	// Filter for exact name match
	presidentResults = utils.FilterByExactName(presidentResults, presidentName)

	if len(presidentResults) == 0 {
		return nil, fmt.Errorf("president entity not found: %s", presidentName)
	}

	if len(presidentResults) > 1 {
		return nil, fmt.Errorf("multiple entities found with name '%s'", presidentName)
	}

	// Get government node
	governmentResults, err := c.SearchEntities(&models.SearchCriteria{
		Kind: &models.Kind{
			Major: "Organisation",
			Minor: "government",
		},
	})
	if err != nil || len(governmentResults) == 0 {
		return nil, fmt.Errorf("failed to find government entity: %w", err)
	}
	governmentID := governmentResults[0].ID

	// Find the president by checking if they have AS_PRESIDENT relationship to government
	for _, president := range presidentResults {
		// Check if this citizen has AS_PRESIDENT relationship to government
		presidentRelations, err := c.GetRelatedEntities(governmentID, &models.Relationship{
			Name:            "AS_PRESIDENT",
			RelatedEntityID: president.ID,
			Direction:       "OUTGOING",
		})
		if err != nil {
			continue
		}

		// If there are any AS_PRESIDENT relationships (active or not), return the president
		if len(presidentRelations) > 0 {
			// Convert SearchResult to Entity
			entity := &models.Entity{
				ID:         president.ID,
				Kind:       president.Kind,
				Created:    president.Created,
				Terminated: president.Terminated,
				Name: models.TimeBasedValue{
					Value: president.Name,
				},
				Metadata:      []models.MetadataEntry{},
				Attributes:    []models.AttributeEntry{},
				Relationships: []models.RelationshipEntry{},
			}
			return entity, nil
		}
	}

	return nil, fmt.Errorf("president entity not found or not active: %s", presidentName)
}
