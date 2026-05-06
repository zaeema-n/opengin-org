package tests

import (
	"orgchart_nexoan/models"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Add your people-specific test functions here
func TestCreatePeople(t *testing.T) {
	// Initialize entity counters
	ministerEntityCounters := map[string]int{
		"minister": 0,
	}
	personEntityCounters := map[string]int{
		"citizen": 0,
	}

	// Test cases for creating ministers
	ministersTestCases := []struct {
		transactionID string
		parent        string
		parentType    string
		child         string
		childType     string
		relType       string
		date          string
	}{
		{
			transactionID: "2157-12_tr_01",
			parent:        "Ranil Wickremesinghe",
			parentType:    "citizen",
			child:         "Minister of Irrigation and Water Resources and Disaster Management",
			childType:     "cabinetMinister",
			relType:       "AS_MINISTER",
			date:          "2018-11-01",
		},
		{
			transactionID: "2157-12_tr_02",
			parent:        "Ranil Wickremesinghe",
			parentType:    "citizen",
			child:         "Minister of Skills Development & Vocational Training",
			childType:     "cabinetMinister",
			relType:       "AS_MINISTER",
			date:          "2018-11-01",
		},
	}

	// Create each minister
	for _, tc := range ministersTestCases {
		t.Logf("Creating minister: %s", tc.child)

		// Create transaction map for AddEntity
		transaction := map[string]interface{}{
			"parent":         tc.parent,
			"child":          tc.child,
			"date":           tc.date,
			"parent_type":    tc.parentType,
			"child_type":     tc.childType,
			"rel_type":       tc.relType,
			"transaction_id": tc.transactionID}

		// Use AddEntity to create the minister
		_, err := client.AddOrgEntity(transaction, ministerEntityCounters)
		assert.NoError(t, err)

		// Update the counter for the next iteration
		ministerEntityCounters["minister"]++

		// Verify the minister was created by searching for it
		searchCriteria := &models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Organisation",
				Minor: tc.childType,
			},
			Name: tc.child,
		}

		results, err := client.SearchEntities(searchCriteria)
		assert.NoError(t, err)
		assert.Len(t, results, 1)
		assert.Equal(t, tc.child, results[0].Name)

		// Verify the relationship was created by checking parent's relationships
		parentResults, err := client.SearchEntities(&models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Person",
				Minor: tc.parentType,
			},
			Name: tc.parent,
		})
		assert.NoError(t, err)
		assert.Len(t, parentResults, 1)

		// Get parent's metadata to verify relationship
		metadata, err := client.GetEntityMetadata(parentResults[0].ID)
		assert.NoError(t, err)
		assert.NotNil(t, metadata)
	}

	// Test cases for creating people
	peopleTestCases := []struct {
		transactionID string
		parent        string
		parentType    string
		child         string
		childType     string
		relType       string
		date          string
	}{
		{
			transactionID: "2095-17_tr_01",
			parent:        "Minister of Irrigation and Water Resources and Disaster Management",
			parentType:    "cabinetMinister",
			child:         "Duminda Dissanayake",
			childType:     "citizen",
			relType:       "AS_APPOINTED",
			date:          "2018-11-01",
		},
		{
			transactionID: "2095-17_tr_02",
			parent:        "Minister of Skills Development & Vocational Training",
			parentType:    "cabinetMinister",
			child:         "Dayasiri Jayasekara",
			childType:     "citizen",
			relType:       "AS_APPOINTED",
			date:          "2018-11-01",
		},
	}

	// Create each person
	for _, tc := range peopleTestCases {
		t.Logf("Creating person: %s", tc.child)

		// Create transaction map for AddEntity
		transaction := map[string]interface{}{
			"parent":         tc.parent,
			"child":          tc.child,
			"date":           tc.date,
			"parent_type":    tc.parentType,
			"child_type":     tc.childType,
			"rel_type":       tc.relType,
			"role":           "minister",
			"transaction_id": tc.transactionID,
		}

		// Add president field if parent is a minister
		if tc.parentType == "cabinetMinister" || tc.parentType == "stateMinister" {
			transaction["president"] = "Ranil Wickremesinghe"
		}

		// Use AddEntity to create the person
		_, err := client.AddPersonEntity(transaction, personEntityCounters)
		assert.NoError(t, err)

		// Update the counter for the next iteration
		personEntityCounters[tc.childType]++

		// Verify the person was created by searching for it
		searchCriteria := &models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Person",
				Minor: tc.childType,
			},
			Name: tc.child,
		}

		results, err := client.SearchEntities(searchCriteria)
		assert.NoError(t, err)
		assert.Len(t, results, 1)
		assert.Equal(t, tc.child, results[0].Name)

		// Verify the relationship was created by checking parent's relationships
		parentResults, err := client.SearchEntities(&models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Organisation",
				Minor: tc.parentType,
			},
			Name: tc.parent,
		})
		assert.NoError(t, err)
		assert.Len(t, parentResults, 1)

		// Get parent's metadata to verify relationship
		// TODO: Implement this
	}
}

func TestCreatePeopleWithManyMinisters(t *testing.T) {
	// Initialize entity counters
	ministerEntityCounters := map[string]int{
		"minister": 0,
	}
	personEntityCounters := map[string]int{
		"citizen": 0,
	}

	// Test cases for creating ministers
	ministersTestCases := []struct {
		transactionID string
		parent        string
		parentType    string
		child         string
		childType     string
		relType       string
		date          string
	}{
		{
			transactionID: "2157-13_tr_01",
			parent:        "Ranil Wickremesinghe",
			parentType:    "citizen",
			child:         "Minister of Defence and Urban Development",
			childType:     "cabinetMinister",
			relType:       "AS_MINISTER",
			date:          "2018-11-01",
		},
		{
			transactionID: "2157-13_tr_02",
			parent:        "Ranil Wickremesinghe",
			parentType:    "citizen",
			child:         "Minister of Health and Indigenous Medicine",
			childType:     "cabinetMinister",
			relType:       "AS_MINISTER",
			date:          "2018-11-01",
		},
		{
			transactionID: "2157-13_tr_03",
			parent:        "Ranil Wickremesinghe",
			parentType:    "citizen",
			child:         "Minister of Education and Lifelong Learning",
			childType:     "cabinetMinister",
			relType:       "AS_MINISTER",
			date:          "2018-11-01",
		},
		{
			transactionID: "2157-13_tr_04",
			parent:        "Ranil Wickremesinghe",
			parentType:    "citizen",
			child:         "Minister of Finance and Economic Development",
			childType:     "cabinetMinister",
			relType:       "AS_MINISTER",
			date:          "2018-11-01",
		},
		{
			transactionID: "2157-13_tr_05",
			parent:        "Ranil Wickremesinghe",
			parentType:    "citizen",
			child:         "Minister of Transport and Civil Aviation",
			childType:     "cabinetMinister",
			relType:       "AS_MINISTER",
			date:          "2018-11-01",
		},
	}

	// Create each minister
	for _, tc := range ministersTestCases {
		t.Logf("Creating minister: %s", tc.child)

		// Create transaction map for AddEntity
		transaction := map[string]interface{}{
			"parent":         tc.parent,
			"child":          tc.child,
			"date":           tc.date,
			"parent_type":    tc.parentType,
			"child_type":     tc.childType,
			"rel_type":       tc.relType,
			"transaction_id": tc.transactionID,
		}

		// Use AddEntity to create the minister
		_, err := client.AddOrgEntity(transaction, ministerEntityCounters)
		assert.NoError(t, err)

		// Update the counter for the next iteration
		ministerEntityCounters["minister"]++

		// Verify the minister was created by searching for it
		searchCriteria := &models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Organisation",
				Minor: tc.childType,
			},
			Name: tc.child,
		}

		results, err := client.SearchEntities(searchCriteria)
		assert.NoError(t, err)
		assert.Len(t, results, 1)
		assert.Equal(t, tc.child, results[0].Name)

		// Verify the relationship was created by checking parent's relationships
		parentResults, err := client.SearchEntities(&models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Person",
				Minor: tc.parentType,
			},
			Name: tc.parent,
		})
		assert.NoError(t, err)
		assert.Len(t, parentResults, 1)

		// Get parent's metadata to verify relationship
		metadata, err := client.GetEntityMetadata(parentResults[0].ID)
		assert.NoError(t, err)
		assert.NotNil(t, metadata)
	}

	// Test cases for creating people
	peopleTestCases := []struct {
		transactionID string
		parent        string
		parentType    string
		child         string
		childType     string
		relType       string
		date          string
		president     string
	}{
		{
			transactionID: "2095/20_tr_01",
			parent:        "Minister of Defence and Urban Development",
			parentType:    "cabinetMinister",
			child:         "Saman Kumara",
			childType:     "citizen",
			relType:       "AS_APPOINTED",
			date:          "2018-12-01",
			president:     "Ranil Wickremesinghe",
		},
		{
			transactionID: "2095/20_tr_02",
			parent:        "Minister of Health and Indigenous Medicine",
			parentType:    "cabinetMinister",
			child:         "Saman Kumara",
			childType:     "citizen",
			relType:       "AS_APPOINTED",
			date:          "2018-12-01",
			president:     "Ranil Wickremesinghe",
		},
		{
			transactionID: "2095/20_tr_03",
			parent:        "Minister of Education and Lifelong Learning",
			parentType:    "cabinetMinister",
			child:         "Saman Kumara",
			childType:     "citizen",
			relType:       "AS_APPOINTED",
			date:          "2018-12-01",
			president:     "Ranil Wickremesinghe",
		},
		{
			transactionID: "2095/20_tr_04",
			parent:        "Minister of Finance and Economic Development",
			parentType:    "cabinetMinister",
			child:         "Sandamali Perera",
			childType:     "citizen",
			relType:       "AS_APPOINTED",
			date:          "2018-12-01",
			president:     "Ranil Wickremesinghe",
		},
		{
			transactionID: "2095/20_tr_04",
			parent:        "Minister of Transport and Civil Aviation",
			parentType:    "cabinetMinister",
			child:         "Sandamali Perera",
			childType:     "citizen",
			relType:       "AS_APPOINTED",
			date:          "2018-12-01",
			president:     "Ranil Wickremesinghe",
		},
	}

	// Create each person
	for _, tc := range peopleTestCases {
		t.Logf("Creating person: %s", tc.child)

		// Create transaction map for AddEntity
		transaction := map[string]interface{}{
			"parent":         tc.parent,
			"child":          tc.child,
			"date":           tc.date,
			"parent_type":    tc.parentType,
			"child_type":     tc.childType,
			"rel_type":       tc.relType,
			"role":           "minister",
			"transaction_id": tc.transactionID,
			"president":      tc.president,
		}

		// Use AddEntity to create the person
		_, err := client.AddPersonEntity(transaction, personEntityCounters)
		assert.NoError(t, err)

		// Update the counter for the next iteration
		personEntityCounters[tc.childType]++

		// Verify the person was created by searching for it
		searchCriteria := &models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Person",
				Minor: tc.childType,
			},
			Name: tc.child,
		}

		results, err := client.SearchEntities(searchCriteria)
		assert.NoError(t, err)
		assert.Len(t, results, 1)
		assert.Equal(t, tc.child, results[0].Name)

		// Verify the relationship was created by checking parent's relationships
		parentResults, err := client.SearchEntities(&models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Organisation",
				Minor: tc.parentType,
			},
			Name: tc.parent,
		})
		assert.NoError(t, err)
		assert.Len(t, parentResults, 1)

		// Get parent's metadata to verify relationship
		// TODO: Implement this
	}
}

func TestTerminatePerson(t *testing.T) {
	// Initialize entity counters
	ministerEntityCounters := map[string]int{
		"minister": 0,
	}
	personEntityCounters := map[string]int{
		"citizen": 0,
	}

	// Test cases for creating ministers
	ministersTestCases := []struct {
		transactionID string
		parent        string
		parentType    string
		child         string
		childType     string
		relType       string
		date          string
		president     string
	}{
		{
			transactionID: "2127-12_tr_01",
			parent:        "Ranil Wickremesinghe",
			parentType:    "citizen",
			child:         "Minister of Health and Space Exploration",
			childType:     "cabinetMinister",
			relType:       "AS_MINISTER",
			date:          "2019-11-01",
			president:     "Ranil Wickremesinghe",
		},
	}

	// Create each minister
	for _, tc := range ministersTestCases {
		t.Logf("Creating minister: %s", tc.child)

		// Create transaction map for AddEntity
		transaction := map[string]interface{}{
			"parent":         tc.parent,
			"child":          tc.child,
			"date":           tc.date,
			"parent_type":    tc.parentType,
			"child_type":     tc.childType,
			"rel_type":       tc.relType,
			"role":           "minister",
			"transaction_id": tc.transactionID,
			"president":      tc.president,
		}

		// Use AddEntity to create the minister
		_, err := client.AddOrgEntity(transaction, ministerEntityCounters)
		assert.NoError(t, err)

		// Update the counter for the next iteration
		ministerEntityCounters["minister"]++

		// Verify the minister was created by searching for it
		searchCriteria := &models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Organisation",
				Minor: tc.childType,
			},
			Name: tc.child,
		}

		results, err := client.SearchEntities(searchCriteria)
		assert.NoError(t, err)
		assert.Len(t, results, 1)
		assert.Equal(t, tc.child, results[0].Name)

		// Verify the relationship was created by checking parent's relationships
		parentResults, err := client.SearchEntities(&models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Person",
				Minor: tc.parentType,
			},
			Name: tc.parent,
		})
		assert.NoError(t, err)
		assert.Len(t, parentResults, 1)

		// Get parent's metadata to verify relationship
		metadata, err := client.GetEntityMetadata(parentResults[0].ID)
		assert.NoError(t, err)
		assert.NotNil(t, metadata)
	}

	// Test cases for creating people
	peopleTestCases := []struct {
		transactionID string
		parent        string
		parentType    string
		child         string
		childType     string
		relType       string
		date          string
		president     string
	}{
		{
			transactionID: "2065-17_tr_01",
			parent:        "Minister of Health and Space Exploration",
			parentType:    "cabinetMinister",
			child:         "Sanath Abeywardena",
			childType:     "citizen",
			relType:       "AS_APPOINTED",
			date:          "2019-11-01",
			president:     "Ranil Wickremesinghe",
		},
	}

	// Create each person
	for _, tc := range peopleTestCases {
		t.Logf("Creating person: %s", tc.child)

		// Create transaction map for AddEntity
		transaction := map[string]interface{}{
			"parent":         tc.parent,
			"child":          tc.child,
			"date":           tc.date,
			"parent_type":    tc.parentType,
			"child_type":     tc.childType,
			"rel_type":       tc.relType,
			"role":           "minister",
			"transaction_id": tc.transactionID,
			"president":      tc.president,
		}

		// Use AddEntity to create the person
		_, err := client.AddPersonEntity(transaction, personEntityCounters)
		assert.NoError(t, err)

		// Update the counter for the next iteration
		personEntityCounters[tc.childType]++

		// Verify the person was created by searching for it
		searchCriteria := &models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Person",
				Minor: tc.childType,
			},
			Name: tc.child,
		}

		results, err := client.SearchEntities(searchCriteria)
		assert.NoError(t, err)
		assert.Len(t, results, 1)
		assert.Equal(t, tc.child, results[0].Name)

		// Verify the relationship was created by checking parent's relationships
		parentResults, err := client.SearchEntities(&models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Organisation",
				Minor: tc.parentType,
			},
			Name: tc.parent,
		})
		assert.NoError(t, err)
		assert.Len(t, parentResults, 1)

		// Get parent's metadata to verify relationship
		// TODO: Implement this
	}

	parent_minister := "Minister of Health and Space Exploration"
	child_person := "Sanath Abeywardena"

	// Create transaction map for terminating the person
	transaction := map[string]interface{}{
		"parent":      parent_minister,
		"child":       child_person,
		"date":        "2019-11-01",
		"parent_type": "cabinetMinister",
		"child_type":  "citizen",
		"rel_type":    "AS_APPOINTED",
		"president":   "Ranil Wickremesinghe",
		"role":        "minister",
	}

	// Terminate the person relationship
	err := client.TerminatePersonEntity(transaction)
	assert.NoError(t, err)

	// Find the minister role node to verify the relationship.
	ministerResults, err := client.SearchEntities(&models.SearchCriteria{
		Kind: &models.Kind{
			Major: "Organisation",
			Minor: "cabinetMinister",
		},
		Name: parent_minister,
	})
	assert.NoError(t, err)
	assert.Len(t, ministerResults, 1)
	ministerNodeID := ministerResults[0].ID + "_minister"

	// Find the department
	personResults, err := client.SearchEntities(&models.SearchCriteria{
		Kind: &models.Kind{
			Major: "Person",
			Minor: "citizen",
		},
		Name: child_person,
	})
	assert.NoError(t, err)
	assert.Len(t, personResults, 1)
	personID := personResults[0].ID

	// Verify the AS_ROLE relationship to minister node is terminated.
	allRelations, err := client.GetRelatedEntities(personID, &models.Relationship{
		RelatedEntityID: ministerNodeID,
		Name:            "AS_ROLE",
	})
	assert.NoError(t, err)
	found := false
	for _, rel := range allRelations {
		assert.Equal(t, "2019-11-01T00:00:00Z", rel.EndTime)
		found = true
		break
	}
	assert.True(t, found, "Should find the terminated relationship")
}

func TestTerminateMultipleMinistersForPerson(t *testing.T) {
	// Initialize entity counters
	ministerEntityCounters := map[string]int{
		"minister": 0,
	}
	personEntityCounters := map[string]int{
		"citizen": 0,
	}

	// Test cases for creating ministers
	ministersTestCases := []struct {
		transactionID string
		parent        string
		parentType    string
		child         string
		childType     string
		relType       string
		date          string
		president     string
	}{
		{
			transactionID: "2127/13_tr_01",
			parent:        "Ranil Wickremesinghe",
			parentType:    "citizen",
			child:         "Minister of Science and Technology",
			childType:     "cabinetMinister",
			relType:       "AS_MINISTER",
			date:          "2019-11-01",
			president:     "Ranil Wickremesinghe",
		},
		{
			transactionID: "2127/13_tr_02",
			parent:        "Ranil Wickremesinghe",
			parentType:    "citizen",
			child:         "Minister of Sports and Youth Affairs",
			childType:     "cabinetMinister",
			relType:       "AS_MINISTER",
			date:          "2019-11-01",
			president:     "Ranil Wickremesinghe",
		},
		{
			transactionID: "2127/13_tr_03",
			parent:        "Ranil Wickremesinghe",
			parentType:    "citizen",
			child:         "Minister of Tourism and Culture",
			childType:     "cabinetMinister",
			relType:       "AS_MINISTER",
			date:          "2019-11-01",
			president:     "Ranil Wickremesinghe",
		},
	}

	// Create each minister
	for _, tc := range ministersTestCases {
		t.Logf("Creating minister: %s", tc.child)

		// Create transaction map for AddEntity
		transaction := map[string]interface{}{
			"parent":         tc.parent,
			"child":          tc.child,
			"date":           tc.date,
			"parent_type":    tc.parentType,
			"child_type":     tc.childType,
			"rel_type":       tc.relType,
			"role":           "minister",
			"transaction_id": tc.transactionID,
			"president":      tc.president,
		}

		// Use AddEntity to create the minister
		_, err := client.AddOrgEntity(transaction, ministerEntityCounters)
		assert.NoError(t, err)

		// Update the counter for the next iteration
		ministerEntityCounters["minister"]++

		// Verify the minister was created
		searchCriteria := &models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Organisation",
				Minor: tc.childType,
			},
			Name: tc.child,
		}

		results, err := client.SearchEntities(searchCriteria)
		assert.NoError(t, err)
		assert.Len(t, results, 1)
		assert.Equal(t, tc.child, results[0].Name)
	}

	// Create a person with relationships to all three ministers
	personName := "John Smith"
	peopleTestCases := []struct {
		transactionID string
		parent        string
		parentType    string
		child         string
		childType     string
		relType       string
		date          string
		president     string
	}{
		{
			transactionID: "2065/18_tr_01",
			parent:        "Minister of Science and Technology",
			parentType:    "cabinetMinister",
			child:         personName,
			childType:     "citizen",
			relType:       "AS_APPOINTED",
			date:          "2019-11-01",
			president:     "Ranil Wickremesinghe",
		},
		{
			transactionID: "2065/18_tr_02",
			parent:        "Minister of Sports and Youth Affairs",
			parentType:    "cabinetMinister",
			child:         personName,
			childType:     "citizen",
			relType:       "AS_APPOINTED",
			date:          "2019-11-01",
			president:     "Ranil Wickremesinghe",
		},
		{
			transactionID: "2065/18_tr_03",
			parent:        "Minister of Tourism and Culture",
			parentType:    "cabinetMinister",
			child:         personName,
			childType:     "citizen",
			relType:       "AS_APPOINTED",
			date:          "2019-11-01",
			president:     "Ranil Wickremesinghe",
		},
	}

	// Create the person and their relationships
	for _, tc := range peopleTestCases {
		t.Logf("Creating person relationship with minister: %s", tc.parent)

		transaction := map[string]interface{}{
			"parent":         tc.parent,
			"child":          tc.child,
			"date":           tc.date,
			"parent_type":    tc.parentType,
			"child_type":     tc.childType,
			"rel_type":       tc.relType,
			"role":           "minister",
			"transaction_id": tc.transactionID,
			"president":      tc.president,
		}

		_, err := client.AddPersonEntity(transaction, personEntityCounters)
		personEntityCounters[tc.childType]++
		assert.NoError(t, err)
	}

	// Verify the person was created
	personResults, err := client.SearchEntities(&models.SearchCriteria{
		Kind: &models.Kind{
			Major: "Person",
			Minor: "citizen",
		},
		Name: personName,
	})
	assert.NoError(t, err)
	assert.Len(t, personResults, 1)
	personID := personResults[0].ID

	// Terminate relationships with Science and Sports ministers
	terminateCases := []struct {
		ministerName string
		date         string
	}{
		{
			ministerName: "Minister of Science and Technology",
			date:         "2020-01-01",
		},
		{
			ministerName: "Minister of Sports and Youth Affairs",
			date:         "2020-02-01",
		},
	}

	for _, tc := range terminateCases {
		// Create transaction map for terminating the relationship
		transaction := map[string]interface{}{
			"parent":      tc.ministerName,
			"child":       personName,
			"date":        tc.date,
			"parent_type": "cabinetMinister",
			"child_type":  "citizen",
			"rel_type":    "AS_APPOINTED",
			"president":   "Ranil Wickremesinghe",
			"role":        "minister",
		}

		// Terminate the relationship
		err := client.TerminatePersonEntity(transaction)
		assert.NoError(t, err)

		// Find the minister
		ministerResults, err := client.SearchEntities(&models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Organisation",
				Minor: "cabinetMinister",
			},
			Name: tc.ministerName,
		})
		assert.NoError(t, err)
		assert.Len(t, ministerResults, 1)
		ministerID := ministerResults[0].ID

		// Verify the AS_ROLE relationship to this minister node is terminated.
		ministerNodeID := ministerID + "_minister"
		allRelations, err := client.GetRelatedEntities(personID, &models.Relationship{
			RelatedEntityID: ministerNodeID,
			Name:            "AS_ROLE",
		})

		assert.NoError(t, err)
		found := false
		for _, rel := range allRelations {
			assert.Equal(t, tc.date+"T00:00:00Z", rel.EndTime)
			found = true
			break
		}
		assert.True(t, found, "Should find the terminated relationship with %s", tc.ministerName)
	}

	// Verify the relationship with Tourism minister node is still active.
	tourismResults, err := client.SearchEntities(&models.SearchCriteria{
		Kind: &models.Kind{
			Major: "Organisation",
			Minor: "cabinetMinister",
		},
		Name: "Minister of Tourism and Culture",
	})
	assert.NoError(t, err)
	assert.Len(t, tourismResults, 1)
	tourismID := tourismResults[0].ID

	tourismNodeID := tourismID + "_minister"
	tourismRelations, err := client.GetRelatedEntities(personID, &models.Relationship{
		RelatedEntityID: tourismNodeID,
		Name:            "AS_ROLE",
	})
	assert.NoError(t, err)
	var found bool
	for _, rel := range tourismRelations {
		assert.Equal(t, "", rel.EndTime, "Tourism minister relationship should still be active")
		found = true
		break
	}
	assert.True(t, found, "Should find the active relationship with Tourism minister")
}

func TestMovePerson(t *testing.T) {
	// Initialize entity counters
	ministerEntityCounters := map[string]int{
		"minister": 0,
	}
	personEntityCounters := map[string]int{
		"citizen": 0,
	}

	// Test cases for creating ministers
	ministersTestCases := []struct {
		transactionID string
		parent        string
		parentType    string
		child         string
		childType     string
		relType       string
		date          string
		president     string
	}{
		{
			transactionID: "2127/14_tr_01",
			parent:        "Ranil Wickremesinghe",
			parentType:    "citizen",
			child:         "Minister of Agriculture and Food Security",
			childType:     "cabinetMinister",
			relType:       "AS_MINISTER",
			date:          "2019-11-01",
			president:     "Ranil Wickremesinghe",
		},
		{
			transactionID: "2127/14_tr_02",
			parent:        "Ranil Wickremesinghe",
			parentType:    "citizen",
			child:         "Minister of Environment and Climate Change",
			childType:     "cabinetMinister",
			relType:       "AS_MINISTER",
			date:          "2019-11-01",
			president:     "Ranil Wickremesinghe",
		},
	}

	// Create each minister
	for _, tc := range ministersTestCases {
		t.Logf("Creating minister: %s", tc.child)

		// Create transaction map for AddEntity
		transaction := map[string]interface{}{
			"parent":         tc.parent,
			"child":          tc.child,
			"date":           tc.date,
			"parent_type":    tc.parentType,
			"child_type":     tc.childType,
			"rel_type":       tc.relType,
			"role":           "minister",
			"transaction_id": tc.transactionID,
			"president":      tc.president,
		}

		// Use AddEntity to create the minister
		_, err := client.AddOrgEntity(transaction, ministerEntityCounters)
		assert.NoError(t, err)

		// Update the counter for the next iteration
		ministerEntityCounters["minister"]++

		// Verify the minister was created
		searchCriteria := &models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Organisation",
				Minor: tc.childType,
			},
			Name: tc.child,
		}

		results, err := client.SearchEntities(searchCriteria)
		assert.NoError(t, err)
		assert.Len(t, results, 1)
		assert.Equal(t, tc.child, results[0].Name)
	}

	// Create a person with relationship to the first minister
	personName := "Robert Johnson"
	peopleTestCases := []struct {
		transactionID string
		parent        string
		parentType    string
		child         string
		childType     string
		relType       string
		date          string
		president     string
	}{
		{
			transactionID: "2065/19_tr_01",
			parent:        "Minister of Agriculture and Food Security",
			parentType:    "cabinetMinister",
			child:         personName,
			childType:     "citizen",
			relType:       "AS_APPOINTED",
			date:          "2019-11-01",
			president:     "Ranil Wickremesinghe",
		},
	}

	// Create the person and their relationship
	for _, tc := range peopleTestCases {
		t.Logf("Creating person relationship with minister: %s", tc.parent)

		transaction := map[string]interface{}{
			"parent":         tc.parent,
			"child":          tc.child,
			"date":           tc.date,
			"parent_type":    tc.parentType,
			"child_type":     tc.childType,
			"rel_type":       tc.relType,
			"role":           "minister",
			"transaction_id": tc.transactionID,
			"president":      tc.president,
		}

		_, err := client.AddPersonEntity(transaction, personEntityCounters)
		assert.NoError(t, err)
	}

	// Verify the person was created
	personResults, err := client.SearchEntities(&models.SearchCriteria{
		Kind: &models.Kind{
			Major: "Person",
			Minor: "citizen",
		},
		Name: personName,
	})
	assert.NoError(t, err)
	assert.Len(t, personResults, 1)
	personID := personResults[0].ID

	// Create transaction map for moving the person from one minister to another
	transaction := map[string]interface{}{
		"old_parent": "Minister of Agriculture and Food Security",
		"new_parent": "Minister of Environment and Climate Change",
		"child":      personName,
		"type":       "AS_APPOINTED",
		"date":       "2020-01-01",
		"president":  "Ranil Wickremesinghe",
		"role":       "minister",
	}

	// Move the person
	err = client.MovePerson(transaction)
	assert.NoError(t, err)

	// Find the old minister to verify the old relationship is terminated
	oldMinisterResults, err := client.SearchEntities(&models.SearchCriteria{
		Kind: &models.Kind{
			Major: "Organisation",
			Minor: "cabinetMinister",
		},
		Name: "Minister of Agriculture and Food Security",
	})
	assert.NoError(t, err)
	assert.Len(t, oldMinisterResults, 1)
	oldMinisterID := oldMinisterResults[0].ID

	// Verify the old AS_ROLE relationship is terminated.
	oldRelations, err := client.GetRelatedEntities(personID, &models.Relationship{
		RelatedEntityID: oldMinisterID + "_minister",
		Name:            "AS_ROLE",
	})

	assert.NoError(t, err)
	found := false
	for _, rel := range oldRelations {
		assert.Equal(t, "2020-01-01T00:00:00Z", rel.EndTime)
		found = true
		break
	}
	assert.True(t, found, "Should find the terminated old relationship")

	// Find the new minister to verify the new relationship
	newMinisterResults, err := client.SearchEntities(&models.SearchCriteria{
		Kind: &models.Kind{
			Major: "Organisation",
			Minor: "cabinetMinister",
		},
		Name: "Minister of Environment and Climate Change",
	})
	assert.NoError(t, err)
	assert.Len(t, newMinisterResults, 1)
	newMinisterID := newMinisterResults[0].ID

	// Verify the new AS_ROLE relationship exists.
	newRelations, err := client.GetRelatedEntities(personID, &models.Relationship{
		RelatedEntityID: newMinisterID + "_minister",
		Name:            "AS_ROLE",
	})

	assert.NoError(t, err)
	found = false
	for _, rel := range newRelations {
		assert.Equal(t, "2020-01-01T00:00:00Z", rel.StartTime)
		assert.Equal(t, "", rel.EndTime) // Should be active (no end time)
		found = true
		break
	}
	assert.True(t, found, "Should find the new relationship")
}

func TestSwapMultiplePeople(t *testing.T) {
	// Initialize entity counters
	ministerEntityCounters := map[string]int{
		"minister": 0,
	}
	personEntityCounters := map[string]int{
		"citizen": 0,
	}

	// Test cases for creating ministers
	ministersTestCases := []struct {
		transactionID string
		parent        string
		parentType    string
		child         string
		childType     string
		relType       string
		date          string
		president     string
	}{
		{
			transactionID: "2127/15_tr_01",
			parent:        "Ranil Wickremesinghe",
			parentType:    "citizen",
			child:         "Minister of Foreign Affairs and International Trade",
			childType:     "cabinetMinister",
			relType:       "AS_MINISTER",
			date:          "2020-01-01",
			president:     "Ranil Wickremesinghe",
		},
		{
			transactionID: "2127/15_tr_02",
			parent:        "Ranil Wickremesinghe",
			parentType:    "citizen",
			child:         "Minister of Justice and Law and Order",
			childType:     "cabinetMinister",
			relType:       "AS_MINISTER",
			date:          "2020-01-01",
			president:     "Ranil Wickremesinghe",
		},
		{
			transactionID: "2127/15_tr_03",
			parent:        "Ranil Wickremesinghe",
			parentType:    "citizen",
			child:         "Minister of Education and Vocational Development",
			childType:     "cabinetMinister",
			relType:       "AS_MINISTER",
			date:          "2020-01-01",
			president:     "Ranil Wickremesinghe",
		},
	}

	// Create each minister
	for _, tc := range ministersTestCases {
		t.Logf("Creating minister: %s", tc.child)

		transaction := map[string]interface{}{
			"parent":         tc.parent,
			"child":          tc.child,
			"date":           tc.date,
			"parent_type":    tc.parentType,
			"child_type":     tc.childType,
			"rel_type":       tc.relType,
			"role":           "minister",
			"transaction_id": tc.transactionID,
			"president":      tc.president,
		}

		_, err := client.AddOrgEntity(transaction, ministerEntityCounters)
		assert.NoError(t, err)
		ministerEntityCounters["minister"]++

		// Verify the minister was created
		searchCriteria := &models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Organisation",
				Minor: tc.childType,
			},
			Name: tc.child,
		}

		results, err := client.SearchEntities(searchCriteria)
		assert.NoError(t, err)
		assert.Len(t, results, 1)
		assert.Equal(t, tc.child, results[0].Name)
	}

	// Create three people with initial relationships
	peopleTestCases := []struct {
		transactionID string
		parent        string
		parentType    string
		child         string
		childType     string
		relType       string
		date          string
		president     string
	}{
		{
			transactionID: "2068/20_tr_01",
			parent:        "Minister of Foreign Affairs and International Trade",
			parentType:    "cabinetMinister",
			child:         "Alice Brown",
			childType:     "citizen",
			relType:       "AS_APPOINTED",
			date:          "2020-01-01",
			president:     "Ranil Wickremesinghe",
		},
		{
			transactionID: "2068/20_tr_02",
			parent:        "Minister of Justice and Law and Order",
			parentType:    "cabinetMinister",
			child:         "Bob Wilson",
			childType:     "citizen",
			relType:       "AS_APPOINTED",
			date:          "2020-01-01",
			president:     "Ranil Wickremesinghe",
		},
		{
			transactionID: "2068/20_tr_03",
			parent:        "Minister of Education and Vocational Development",
			parentType:    "cabinetMinister",
			child:         "Carol Davis",
			childType:     "citizen",
			relType:       "AS_APPOINTED",
			date:          "2020-01-01",
			president:     "Ranil Wickremesinghe",
		},
	}

	// Create the people and their initial relationships
	for _, tc := range peopleTestCases {
		t.Logf("Creating person relationship with minister: %s", tc.parent)

		transaction := map[string]interface{}{
			"parent":         tc.parent,
			"child":          tc.child,
			"date":           tc.date,
			"parent_type":    tc.parentType,
			"child_type":     tc.childType,
			"rel_type":       tc.relType,
			"role":           "minister",
			"transaction_id": tc.transactionID,
			"president":      tc.president,
		}

		_, err := client.AddPersonEntity(transaction, personEntityCounters)
		personEntityCounters[tc.childType]++
		assert.NoError(t, err)
	}

	// Verify all people were created
	personNames := []string{"Alice Brown", "Bob Wilson", "Carol Davis"}
	personIDs := make(map[string]string)
	for _, name := range personNames {
		results, err := client.SearchEntities(&models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Person",
				Minor: "citizen",
			},
			Name: name,
		})
		assert.NoError(t, err)
		assert.Len(t, results, 1)
		personIDs[name] = results[0].ID
	}

	// Define the swap moves
	swapMoves := []struct {
		oldParent string
		newParent string
		person    string
		date      string
		president string
	}{
		{
			oldParent: "Minister of Foreign Affairs and International Trade",
			newParent: "Minister of Justice and Law and Order",
			person:    "Alice Brown",
			date:      "2021-01-01",
			president: "Ranil Wickremesinghe",
		},
		{
			oldParent: "Minister of Justice and Law and Order",
			newParent: "Minister of Education and Vocational Development",
			person:    "Bob Wilson",
			date:      "2021-01-01",
			president: "Ranil Wickremesinghe",
		},
		{
			oldParent: "Minister of Education and Vocational Development",
			newParent: "Minister of Foreign Affairs and International Trade",
			person:    "Carol Davis",
			date:      "2021-01-01",
			president: "Ranil Wickremesinghe",
		},
	}

	// Execute the swap moves
	for _, move := range swapMoves {
		transaction := map[string]interface{}{
			"old_parent": move.oldParent,
			"new_parent": move.newParent,
			"child":      move.person,
			"type":       "AS_APPOINTED",
			"date":       move.date,
			"president":  move.president,
			"role":       "minister",
		}

		err := client.MovePerson(transaction)
		assert.NoError(t, err)
	}

	// Verify all relationships after the swap using person -> ministerRoleNode AS_ROLE edges.
	expectedPersonToMinister := map[string]string{
		"Alice Brown": "Minister of Justice and Law and Order",
		"Bob Wilson":  "Minister of Education and Vocational Development",
		"Carol Davis": "Minister of Foreign Affairs and International Trade",
	}

	for personName, ministerName := range expectedPersonToMinister {
		ministerResults, err := client.SearchEntities(&models.SearchCriteria{
			Kind: &models.Kind{
				Major: "Organisation",
				Minor: "cabinetMinister",
			},
			Name: ministerName,
		})
		assert.NoError(t, err)
		assert.Len(t, ministerResults, 1)
		targetNodeID := ministerResults[0].ID + "_minister"

		personRels, err := client.GetRelatedEntities(personIDs[personName], &models.Relationship{
			Name: "AS_ROLE",
		})
		assert.NoError(t, err)

		activeFound := false
		terminatedFound := false
		for _, rel := range personRels {
			if rel.RelatedEntityID == targetNodeID && rel.EndTime == "" {
				activeFound = true
			}
			if rel.EndTime == "2021-01-01T00:00:00Z" {
				terminatedFound = true
			}
		}
		assert.True(t, activeFound, "Should find active AS_ROLE for %s -> %s", personName, ministerName)
		assert.True(t, terminatedFound, "Should have at least one terminated prior AS_ROLE for %s", personName)
	}
}
