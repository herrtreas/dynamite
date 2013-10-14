package dynamite

import (
	"github.com/crowdmob/goamz/dynamodb"
)

func (e *Entity) Read() (err error) {
	key := dynamodb.Key{HashKey: e.GetHashKeyValue(), RangeKey: e.GetRangeKeyValue()}
	attrs, err := e.table().GetItem(&key)

	if err == nil && len(attrs) != 0 {
		if len(e.Attributes) == 0 {
			e.Attributes = make(map[string]dynamodb.Attribute)
		}

		for _, attr := range attrs {
			e.Attributes[attr.Name] = *attr
		}
	}

	return
}

func (e *Entity) Store() (bool, error) {
	attrs := make([]dynamodb.Attribute, 0, len(e.Attributes))
	for _, attr := range e.Attributes {
		attrs = append(attrs, attr)
	}
	return e.table().PutItem(e.GetHashKeyValue(), e.GetRangeKeyValue(), attrs)
}

func (e *Entity) Delete() (success bool, err error) {
	key := &dynamodb.Key{HashKey: e.GetHashKeyValue(), RangeKey: e.GetRangeKeyValue()}
	success, err = e.table().DeleteItem(key)
	return
}

// Load one ore more Entities defined by template
func LoadEntities(template Entity) (entities []Entity, err error) {
	var comparisons []dynamodb.AttributeComparison

	hashKeyName := template.PrimaryKey.KeyAttribute.Name
	hashKeyValue := template.GetHashKeyValue()

	hashKeyComparison := dynamodb.NewEqualStringAttributeComparison(hashKeyName, hashKeyValue)
	comparisons = append(comparisons, *hashKeyComparison)

	if template.GetRangeKeyValue() != "" {
		rangeKeyName := template.PrimaryKey.RangeAttribute.Name
		rangeKeyValue := template.GetRangeKeyValue()

		rangeKeyComparison := dynamodb.NewEqualStringAttributeComparison(rangeKeyName, rangeKeyValue)
		comparisons = append(comparisons, *rangeKeyComparison)
	}

	items, err := template.table().Query(comparisons)

	if err == nil {
		entities, err = mapItemsToTemplate(items, template)
	}

	return
}

// Store one ore more entities on Dynamo
func StoreEntities(entities ...*Entity) (success bool, err error) {

	var storedEntities []*Entity

	entities = flattenEntities(entities)

	// Store them all, break on error
	for _, entity := range entities {
		success, err = entity.Store()
		if err == nil && success == true {
			storedEntities = append(storedEntities, entity)
		} else {
			break
		}
	}

	// If err remove all stored entities
	if err != nil || success == false {
		for _, entity := range storedEntities {
			entity.Delete()
		}
	}

	return
}

func DeleteEntities(entities ...*Entity) (success bool, err error) {
	entities = flattenEntities(entities)

	var deletedEntities []*Entity

	// Delete them all
	for _, entity := range entities {
		success, err = entity.Delete()
		if success == true && err == nil {
			deletedEntities = append(deletedEntities, entity)
		} else {
			break
		}
	}

	// Recreate on error
	if err != nil || success == false {
		for _, entity := range deletedEntities {
			entity.Store()
		}
	}

	return
}

// Builds a flat list of nested/related entities
func flattenEntities(entities []*Entity) []*Entity {

	var flatEntities []*Entity

	for _, entity := range entities {
		flatEntities = append(flatEntities, entity)

		var relatedEntities []*Entity
		for _, relatedEntity := range entity.Related {
			relatedEntities = append(relatedEntities, relatedEntity)
		}

		for _, e := range flattenEntities(relatedEntities) {
			flatEntities = append(flatEntities, e)
		}

	}

	return flatEntities
}

// Returns a list of Entities mapped from items
// using the template Entity as sample
func mapItemsToTemplate(items []map[string]*dynamodb.Attribute, template Entity) (entities []Entity, err error) {

	for _, item := range items {

		entity := NewEntity(
			template.TableName,
			template.PrimaryKey.KeyAttribute,
			template.PrimaryKey.RangeAttribute,
		)

		for _, attr := range item {
			if attr.Name == entity.PrimaryKey.KeyAttribute.Name {
				entity.SetHashKeyValue(attr.Value)
			} else if entity.PrimaryKey.RangeAttribute != nil && attr.Name == entity.PrimaryKey.RangeAttribute.Name {
				entity.SetRangeKeyValue(attr.Value)
			} else {
				entity.Add(attr.Type, attr.Name, attr.Value)
			}
		}

		entities = append(entities, entity)
	}

	return
}
