package api

import (
	"fmt"
	"strings"
	"time"

	"orgchart_nexoan/internal/utils"
	"orgchart_nexoan/models"

	"github.com/google/uuid"
)

// AddSecretaryEntity creates or finds a citizen, finds the correct minister that is
// active at the appointment date (via the president's AS_MINISTER relationships),
// creates an AS_ROLE relationship from the citizen to the minister's Secretary node.
func (c *Client) AddSecretaryEntity(transaction map[string]interface{}, entityCounters map[string]int) (int, error) {
	// Extract fields from transaction
	child, ok := transaction["child"].(string)
	if !ok {
		return 0, fmt.Errorf("transaction missing or invalid 'child'")
	}

	childType, ok := transaction["child_type"].(string)
	if !ok {
		return 0, fmt.Errorf("transaction missing or invalid 'child_type'")
	}

	parent, ok := transaction["parent"].(string)
	if !ok {
		return 0, fmt.Errorf("transaction missing or invalid 'parent'")
	}

	parentType, ok := transaction["parent_type"].(string)
	if !ok {
		return 0, fmt.Errorf("transaction missing or invalid 'parent_type'")
	}

	dateStr, ok := transaction["date"].(string)
	if !ok {
		return 0, fmt.Errorf("transaction missing or invalid 'date'")
	}

	transactionID, ok := transaction["transaction_id"].(string)
	if !ok {
		return 0, fmt.Errorf("transaction missing or invalid 'transaction_id'")
	}

	presidentName, ok := transaction["president"].(string)
	if !ok {
		return 0, fmt.Errorf("transaction missing or invalid 'president'")
	}

	date, err := time.Parse("2006-01-02", strings.TrimSpace(dateStr))
	if err != nil {
		return 0, fmt.Errorf("failed to parse date: %w", err)
	}
	dateISO := date.Format(time.RFC3339)

	// Step 1: Check if citizen exists; create if not.
	personResults, err := c.SearchEntities(&models.SearchCriteria{
		Kind: &models.Kind{Major: "Person"},
		Name: child,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to search for citizen '%s': %w", child, err)
	}
	// Filter for exact name match
	personResults = utils.FilterByExactName(personResults, child)
	if len(personResults) > 1 {
		return 0, fmt.Errorf("multiple citizens found with name '%s'", child)
	}

	var citizenID string
	if len(personResults) == 1 {
		citizenID = personResults[0].ID
	} else {
		if _, exists := entityCounters[childType]; !exists {
			return 0, fmt.Errorf("unknown child type: %s", childType)
		}
		prefixPart := strings.Split(transactionID, "_")[0]
		prefix := fmt.Sprintf("%s_%s", prefixPart, strings.ToLower(childType[:3]))
		entityCounters[childType]++
		newID := fmt.Sprintf("%s_%d", prefix, entityCounters[childType])

		created, err := c.CreateEntity(&models.Entity{
			ID:            newID,
			Kind:          models.Kind{Major: "Person", Minor: childType},
			Created:       dateISO,
			Terminated:    "",
			Name:          models.TimeBasedValue{StartTime: dateISO, Value: child},
			Metadata:      []models.MetadataEntry{},
			Attributes:    []models.AttributeEntry{},
			Relationships: []models.RelationshipEntry{},
		})
		if err != nil {
			return 0, fmt.Errorf("failed to create citizen '%s': %w", child, err)
		}
		citizenID = created.ID
	}

	// Step 2: Look up the president.
	presidentEntity, err := c.GetPresidentByGovernment(presidentName)
	if err != nil {
		return 0, fmt.Errorf("failed to get president '%s': %w", presidentName, err)
	}

	// Step 3: Get AS_MINISTER relationships active at dateISO.
	ministerRels, err := c.GetRelatedEntities(presidentEntity.ID, &models.Relationship{
		Name:     "AS_MINISTER",
		ActiveAt: dateISO,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get AS_MINISTER relationships for president '%s': %w", presidentName, err)
	}

	// Step 4: Search for all ministers with the given name and kind, then
	// intersect with the IDs returned by the active AS_MINISTER relationships.
	// This avoids one SearchEntities call per relationship.
	candidateResults, err := c.SearchEntities(&models.SearchCriteria{
		Kind: &models.Kind{Major: "Organisation", Minor: parentType},
		Name: parent,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to search for minister '%s' (%s): %w", parent, parentType, err)
	}
	// Filter for exact name match
	candidateResults = utils.FilterByExactName(candidateResults, parent)

	// Build a set of IDs that are active at dateISO (from the president's AS_MINISTER rels).
	activeMinisterIDs := make(map[string]struct{}, len(ministerRels))
	for _, rel := range ministerRels {
		activeMinisterIDs[rel.RelatedEntityID] = struct{}{}
	}

	// Match: candidate must also be in the active set.
	var matchedMinisterID string
	for _, candidate := range candidateResults {
		if _, active := activeMinisterIDs[candidate.ID]; active {
			if matchedMinisterID != "" {
				return 0, fmt.Errorf("multiple ministers match '%s' (%s) at %s", parent, parentType, dateISO)
			}
			matchedMinisterID = candidate.ID
		}
	}
	if matchedMinisterID == "" {
		return 0, fmt.Errorf("no active minister '%s' (%s) found at %s", parent, parentType, dateISO)
	}

	// Guard: check if the secretary node already has an active AS_ROLE relationship at dateISO.
	// If it does, a secretary is already active for this minister.
	secretaryNodeID := fmt.Sprintf("%s_secretary", matchedMinisterID)
	existingRoleRels, err := c.GetRelatedEntities(secretaryNodeID, &models.Relationship{
		Name:      "AS_ROLE",
		ActiveAt:  dateISO,
		Direction: "INCOMING",
	})
	if err != nil {
		return 0, fmt.Errorf("failed to check existing active secretaries for minister '%s': %w", matchedMinisterID, err)
	}
	if len(existingRoleRels) > 0 {
		return 0, fmt.Errorf("minister '%s' already has an active secretary at %s", matchedMinisterID, dateISO)
	}

	// Step 5: AS_ROLE from citizen → Secretary node (<matchedMinisterID>_secretary).

	roleRelID := fmt.Sprintf("%s_%s_%s", citizenID, secretaryNodeID, uuid.New().String())
	_, err = c.UpdateEntity(citizenID, &models.Entity{
		ID:         citizenID,
		Metadata:   []models.MetadataEntry{},
		Attributes: []models.AttributeEntry{},
		Relationships: []models.RelationshipEntry{{
			Key: roleRelID,
			Value: models.Relationship{
				RelatedEntityID: secretaryNodeID, Name: "AS_ROLE",
				StartTime: dateISO, EndTime: "", ID: roleRelID,
			},
		}},
	})
	if err != nil {
		return 0, fmt.Errorf("failed to add AS_ROLE relationship: %w", err)
	}

	return entityCounters[childType], nil
}

// TerminateSecretaryEntity terminates both relationships that AddSecretaryEntity created:
//   - AS_ROLE from citizen → minister's Secretary node (<ministerID>_secretary)
//
// The minister is resolved the same way as AddSecretaryEntity:
//
//	president → active AS_MINISTER relationships at dateISO → name match.
func (c *Client) TerminateSecretaryEntity(transaction map[string]interface{}) error {
	// Extract fields from transaction
	child, ok := transaction["child"].(string)
	if !ok || child == "" {
		return fmt.Errorf("transaction missing or invalid 'child'")
	}

	childType, ok := transaction["child_type"].(string)
	if !ok || childType == "" {
		return fmt.Errorf("transaction missing or invalid 'child_type'")
	}

	parent, ok := transaction["parent"].(string)
	if !ok || parent == "" {
		return fmt.Errorf("transaction missing or invalid 'parent'")
	}

	parentType, ok := transaction["parent_type"].(string)
	if !ok || parentType == "" {
		return fmt.Errorf("transaction missing or invalid 'parent_type'")
	}

	dateStr, ok := transaction["date"].(string)
	if !ok || dateStr == "" {
		return fmt.Errorf("transaction missing or invalid 'date'")
	}

	date, err := time.Parse("2006-01-02", strings.TrimSpace(dateStr))
	if err != nil {
		return fmt.Errorf("failed to parse date: %w", err)
	}
	dateISO := date.Format(time.RFC3339)

	// Step 1: Find the citizen.
	personResults, err := c.SearchEntities(&models.SearchCriteria{
		Kind: &models.Kind{Major: "Person"}, Name: child,
	})
	if err != nil {
		return fmt.Errorf("failed to search for citizen '%s': %w", child, err)
	}
	// Filter for exact name match
	personResults = utils.FilterByExactName(personResults, child)
	if len(personResults) == 0 {
		return fmt.Errorf("citizen '%s' not found", child)
	}
	if len(personResults) > 1 {
		return fmt.Errorf("multiple citizens found with name '%s'", child)
	}
	citizenID := personResults[0].ID

	// Step 2: Get all active AS_ROLE relationships from the citizen.
	allRoleRels, err := c.GetRelatedEntities(citizenID, &models.Relationship{
		Name: "AS_ROLE",
	})
	if err != nil {
		return fmt.Errorf("failed to get AS_ROLE relationships for citizen '%s': %w", citizenID, err)
	}

	// Step 3: Walk active rels that point to a secretary node, back-derive the
	// ministerID by stripping "_secretary", look up the minister name, and match
	// against the parent name provided in the transaction.
	var activeRoleRel *models.Relationship
	for _, rel := range allRoleRels {
		// Only consider open (active) relationships.
		if rel.EndTime != "" {
			continue
		}
		// Only consider rels pointing to a secretary node.
		if !strings.HasSuffix(rel.RelatedEntityID, "_secretary") {
			continue
		}
		// Derive ministerID and look it up.
		ministerID := strings.TrimSuffix(rel.RelatedEntityID, "_secretary")
		ministerResults, err := c.SearchEntities(&models.SearchCriteria{
			ID: ministerID,
		})
		if err != nil || len(ministerResults) == 0 {
			continue
		}
		// Check if this minister's name matches the parent name in the transaction.
		if ministerResults[0].Name != parent {
			continue
		}
		if activeRoleRel != nil {
			return fmt.Errorf("multiple active AS_ROLE relationships found for citizen '%s' pointing to minister '%s'", citizenID, parent)
		}
		r := rel
		activeRoleRel = &r
	}
	if activeRoleRel == nil {
		return fmt.Errorf("no active AS_ROLE relationship found for citizen '%s' as secretary of minister '%s'", citizenID, parent)
	}

	// Terminate the AS_ROLE relationship from the citizen to the secretary node.
	_, err = c.UpdateEntity(citizenID, &models.Entity{
		ID: citizenID,
		Relationships: []models.RelationshipEntry{{
			Key: activeRoleRel.ID,
			Value: models.Relationship{
				EndTime: dateISO,
				ID:      activeRoleRel.ID,
			},
		}},
	})
	if err != nil {
		return fmt.Errorf("failed to terminate AS_ROLE relationship: %w", err)
	}

	return nil
}
