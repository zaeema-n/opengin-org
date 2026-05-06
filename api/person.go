package api

import (
	"fmt"
	"strings"
	"time"

	"orgchart_nexoan/internal/utils"
	"orgchart_nexoan/models"
)

// AddPersonEntity creates a new person entity and establishes its relationship with a parent entity.
// Assumes the parent entity already exists.
func (c *Client) AddPersonEntity(transaction map[string]interface{}, entityCounters map[string]int) (int, error) {
	parent := transaction["parent"].(string)
	child := transaction["child"].(string)
	dateStr := transaction["date"].(string)
	parentType := transaction["parent_type"].(string)
	childType := transaction["child_type"].(string)
	transactionID := transaction["transaction_id"].(string)

	var presidentName string
	var role string

	switch {
	case isMinisterType(parentType):
		var ok bool
		presidentName, ok = transaction["president"].(string)
		if !ok || presidentName == "" {
			return 0, fmt.Errorf("president name is required and must be a non-empty string when adding a person to a minister")
		}
		role, ok = transaction["role"].(string)
		if !ok || role == "" {
			return 0, fmt.Errorf("role is required and must be either 'minister' or 'secretary' when adding a person to a minister")
		}
	case parentType == "government":
		relType, _ := transaction["rel_type"].(string)
		if relType != "AS_PRESIDENT" {
			return 0, fmt.Errorf("adding a person under government requires rel_type AS_PRESIDENT, got %q", relType)
		}
	default:
		return 0, fmt.Errorf("adding a person is only supported for minister or government parents")
	}

	date, err := time.Parse("2006-01-02", strings.TrimSpace(dateStr))
	if err != nil {
		return 0, fmt.Errorf("failed to parse date: %w", err)
	}
	dateISO := date.Format(time.RFC3339)

	var parentID string

	switch {
	case isMinisterType(parentType):
		ministerEntity, err := c.GetActiveMinisterByPresident(presidentName, parent, dateISO)
		if err != nil {
			return 0, fmt.Errorf("failed to get parent minister entity: %w", err)
		}
		parentID = ministerEntity.ID
	case parentType == "government":
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
		searchResults = utils.FilterByExactName(searchResults, parent)
		if len(searchResults) == 0 {
			return 0, fmt.Errorf("parent entity not found: %s", parent)
		}
		if len(searchResults) > 1 {
			return 0, fmt.Errorf("multiple parent entities found with name '%s'", parent)
		}
		parentID = searchResults[0].ID
	default:
		return 0, fmt.Errorf("adding a person is only supported for minister or government parents")
	}

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
	personResults = utils.FilterByExactName(personResults, child)
	if len(personResults) > 1 {
		return 0, fmt.Errorf("multiple entities found for person: %s", child)
	}

	var childID string
	if len(personResults) == 1 {
		childID = personResults[0].ID
	} else {
		if _, exists := entityCounters[childType]; !exists {
			return 0, fmt.Errorf("unknown child type: %s", childType)
		}

		prefixPart := strings.Split(transactionID, "_")[0]
		prefix := fmt.Sprintf("%s_%s", prefixPart, strings.ToLower(childType[:3]))
		entityCounters[childType]++
		newEntityID := fmt.Sprintf("%s_%d", prefix, entityCounters[childType])

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

		createdChild, err := c.CreateEntity(childEntity)
		if err != nil {
			return 0, fmt.Errorf("failed to create child entity: %w", err)
		}
		childID = createdChild.ID
	}

	switch {
	case isMinisterType(parentType):
		targetNodeID, err := roleNodeID(parentID, role)
		if err != nil {
			return 0, err
		}
		if err := c.ensureRoleNodeHasNoActiveAssignment(targetNodeID, dateISO); err != nil {
			return 0, err
		}
		if err := c.createASRole(childID, targetNodeID, dateISO); err != nil {
			return 0, err
		}
	case parentType == "government":
		if err := c.ensureGovernmentHasNoActivePresident(parentID, dateISO); err != nil {
			return 0, err
		}
		if err := c.createASPresidentRelationship(parentID, childID, dateISO); err != nil {
			return 0, err
		}
	}

	return entityCounters[childType], nil
}

// TerminatePersonEntity ends an AS_ROLE edge from the person to the minister's role node (minister or secretary) at the given date.
func (c *Client) TerminatePersonEntity(transaction map[string]interface{}) error {
	// Extract details from the transaction
	parent := transaction["parent"].(string)
	child := transaction["child"].(string)
	dateStr := transaction["date"].(string)
	parentType := transaction["parent_type"].(string)
	childType := transaction["child_type"].(string)

	if !isMinisterType(parentType) {
		return fmt.Errorf("terminating a person is only supported when the parent is a minister")
	}

	presidentName, ok := transaction["president"].(string)
	if !ok || presidentName == "" {
		return fmt.Errorf("president name is required and must be a non-empty string when terminating a person under a minister")
	}

	role, ok := transaction["role"].(string)
	if !ok || role == "" {
		return fmt.Errorf("role is required and must be either 'minister' or 'secretary' when terminating a person under a minister")
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

	ministerEntity, err := c.GetActiveMinisterByPresident(presidentName, parent, dateISO)
	if err != nil {
		return fmt.Errorf("failed to get parent minister entity: %w", err)
	}

	targetNodeID, err := roleNodeID(ministerEntity.ID, role)
	if err != nil {
		return err
	}

	// Relations endpoint: AS_ROLE to this role node, active at termination date.
	roleRels, err := c.GetRelatedEntities(childID, &models.Relationship{
		Name:            "AS_ROLE",
		RelatedEntityID: targetNodeID,
		ActiveAt:        dateISO,
	})
	if err != nil {
		return fmt.Errorf("failed to get AS_ROLE relationships for terminate: %w", err)
	}
	if len(roleRels) == 0 {
		return fmt.Errorf("no AS_ROLE relationship active for person '%s' to minister '%s' role '%s' at %s", child, parent, role, dateISO)
	}
	for _, rel := range roleRels {
		if err := c.terminateRelationship(childID, rel.ID, dateISO); err != nil {
			return err
		}
	}
	return nil
}

// MovePerson moves a person's AS_ROLE edge from one minister portfolio to another for a given slot:
// role "minister" uses each minister's *_minister node; "secretary" uses *_secretary.
func (c *Client) MovePerson(transaction map[string]interface{}) error {
	newParent := transaction["new_parent"].(string)
	oldParent := transaction["old_parent"].(string)
	child := transaction["child"].(string)
	dateStr := transaction["date"].(string)

	presidentName, ok := transaction["president"].(string)
	if !ok || presidentName == "" {
		return fmt.Errorf("president name is required and must be a non-empty string")
	}

	role, ok := transaction["role"].(string)
	if !ok || role == "" {
		return fmt.Errorf("role is required and must be either 'minister' or 'secretary' when moving a person between ministers")
	}

	childType := "citizen"
	if v, ok := transaction["child_type"].(string); ok && v != "" {
		childType = v
	}

	dateISO, err := parseDateISO(dateStr)
	if err != nil {
		return err
	}

	newParentEntity, err := c.GetActiveMinisterByPresident(presidentName, newParent, dateISO)
	if err != nil {
		return fmt.Errorf("failed to get new parent entity: %w", err)
	}
	oldParentEntity, err := c.GetActiveMinisterByPresident(presidentName, oldParent, dateISO)
	if err != nil {
		return fmt.Errorf("failed to get old parent entity: %w", err)
	}

	oldTargetNodeID, err := roleNodeID(oldParentEntity.ID, role)
	if err != nil {
		return err
	}
	newTargetNodeID, err := roleNodeID(newParentEntity.ID, role)
	if err != nil {
		return err
	}

	childResults, err := c.SearchEntities(&models.SearchCriteria{
		Kind: &models.Kind{
			Major: "Person",
			Minor: childType,
		},
		Name: child,
	})
	if err != nil {
		return fmt.Errorf("failed to search for child entity: %w", err)
	}
	childResults = utils.FilterByExactName(childResults, child)
	if len(childResults) == 0 {
		return fmt.Errorf("child entity not found: %s", child)
	}
	if len(childResults) > 1 {
		return fmt.Errorf("multiple child entities found with name '%s'", child)
	}
	childID := childResults[0].ID

	roleRels, err := c.GetRelatedEntities(childID, &models.Relationship{
		Name:            "AS_ROLE",
		RelatedEntityID: oldTargetNodeID,
		ActiveAt:        dateISO,
	})
	if err != nil {
		return fmt.Errorf("failed to get AS_ROLE relationships for move: %w", err)
	}
	if len(roleRels) == 0 {
		return fmt.Errorf("no AS_ROLE to '%s' role slot active for person '%s' under minister '%s' at %s", role, child, oldParent, dateISO)
	}

	if err := c.ensureRoleNodeHasNoActiveAssignment(newTargetNodeID, dateISO); err != nil {
		return err
	}

	for _, rel := range roleRels {
		if err := c.terminateRelationship(childID, rel.ID, dateISO); err != nil {
			return err
		}
	}
	if err := c.createASRole(childID, newTargetNodeID, dateISO); err != nil {
		return err
	}

	return nil
}
