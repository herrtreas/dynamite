package dynamite

import (
	"github.com/crowdmob/goamz/dynamodb"
	. "launchpad.net/gocheck"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type AttributeSuite struct {
	Entity Entity
}

var _ = Suite(&AttributeSuite{})

func (s *AttributeSuite) SetUpTest(c *C) {
	s.Entity = NewEntity(
		"table",
		&dynamodb.Attribute{Type: "S", Name: "id", Value: "AB123"},
		&dynamodb.Attribute{Type: "N", Name: "date", Value: "123456789"},
	)
	s.Entity.Attributes["name"] = dynamodb.Attribute{Name: "name", Value: "peter"}
}

func (s *AttributeSuite) TestEntityGet(c *C) {
	// Return an Attributes value by name
	c.Check(s.Entity.Get("name"), Equals, "peter")

	// Asking for a HashKey
	// It returns the HashKey's value
	c.Check(s.Entity.Get("id"), Equals, "AB123")

	// Asking for a RangeKey
	// It returns the RangeKey's value
	c.Check(s.Entity.Get("date"), Equals, "123456789")

	// It returns an empty string if the RangeKey does not exist
	e := NewEntity("table", nil, nil)
	c.Check(e.Get("date"), Equals, "")
}

func (s *AttributeSuite) TestEntitySet(c *C) {
	// Set an Attributes value by name
	s.Entity.Set("name", "hans")
	c.Check(s.Entity.Attributes["name"].Value, Equals, "hans")

	// It returns an Error if the Attribute by name does not exist
	err := s.Entity.Set("age", "18")
	c.Check(err, NotNil)
}

func (s *AttributeSuite) TestEntityAdd(c *C) {
	// It adds a new Attribute
	s.Entity.Add("S", "city", "dedelow")
	c.Check(s.Entity.Attributes["city"].Type, Equals, "S")
	c.Check(s.Entity.Attributes["city"].Name, Equals, "city")
	c.Check(s.Entity.Attributes["city"].Value, Equals, "dedelow")
}

func (s *AttributeSuite) TestEntitySetHashKeyValue(c *C) {
	// It sets the HashKey's value
	s.Entity.SetHashKeyValue("XX99")
	c.Check(s.Entity.PrimaryKey.KeyAttribute.Value, Equals, "XX99")
}

func (s *AttributeSuite) TestEntityGetHashKeyValue(c *C) {
	// Asking for a HashKey
	// It returns the HashKey's value
	c.Check(s.Entity.GetHashKeyValue(), Equals, "AB123")
}

func (s *AttributeSuite) TestEntitySetRangeKeyValue(c *C) {
	// It sets the RangeKey's value
	s.Entity.SetRangeKeyValue("0000")
	c.Check(s.Entity.PrimaryKey.RangeAttribute.Value, Equals, "0000")

	// It make sure that setting a non existing RangeKey does not panic
	e := NewEntity("table", nil, nil)
	e.SetRangeKeyValue("000")
}

func (s *AttributeSuite) TestEntityGetRangeKeyValue(c *C) {
	// It returns the RangeKey's value
	c.Check(s.Entity.GetRangeKeyValue(), Equals, "123456789")

	// It returns an empty string if the RangeKey does not exist
	e := NewEntity("table", nil, nil)
	c.Check(e.GetRangeKeyValue(), Equals, "")
}
