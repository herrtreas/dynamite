package dynamite

import (
	"errors"
	"github.com/crowdmob/goamz/dynamodb"
)

// Return an Attributes value by name
func (e *Entity) Get(attrName string) string {
	if e.PrimaryKey.KeyAttribute != nil && e.PrimaryKey.KeyAttribute.Name == attrName {
		return e.GetHashKeyValue()
	} else if e.PrimaryKey.RangeAttribute != nil && e.PrimaryKey.RangeAttribute.Name == attrName {
		return e.GetRangeKeyValue()
	} else {
		return e.Attributes[attrName].Value
	}
}

// Set an Attribute value by name
// Returns an error if the Attribute does not exist
func (e *Entity) Set(attrName string, attrValue string) error {
	attr := e.Attributes[attrName]

	if attr.Name != "" {
		attr.Value = attrValue
		e.Attributes[attrName] = attr
		return nil
	} else {
		return errors.New("Attribute " + attrName + " does not exist. Use Add to define a new one")
	}
}

// Add a new Attribute
func (e *Entity) Add(attrType string, attrName string, attrValue string) {
	e.Attributes[attrName] = dynamodb.Attribute{
		Type:  attrType,
		Name:  attrName,
		Value: attrValue,
	}
}

// Set the HashKey's value
func (e *Entity) SetHashKeyValue(value string) {
	e.PrimaryKey.KeyAttribute.Value = value
}

// Return the HashKey's value
func (e *Entity) GetHashKeyValue() string {
	return e.PrimaryKey.KeyAttribute.Value
}

// Set the RangeKey's value
// If the RangeKey is not defined this silently does nothing
func (e *Entity) SetRangeKeyValue(value string) {
	if e.PrimaryKey.RangeAttribute != nil {
		e.PrimaryKey.RangeAttribute.Value = value
	}
}

// Return the RangeKey's value
func (e *Entity) GetRangeKeyValue() string {
	if e.PrimaryKey.RangeAttribute != nil {
		return e.PrimaryKey.RangeAttribute.Value
	} else {
		return ""
	}
}
