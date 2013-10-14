package dynamite

import (
	_ "fmt"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/dynamodb"
)

type Entity struct {
	PrimaryKey dynamodb.PrimaryKey
	TableName  string
	Attributes map[string]dynamodb.Attribute
	Related    map[string]*Entity
}

var awsAuth aws.Auth
var awsRegion aws.Region
var dynamoServer dynamodb.Server

func Setup(auth aws.Auth, region aws.Region) {
	awsAuth = auth
	awsRegion = region
	dynamoServer = dynamodb.Server{Auth: auth, Region: region}
}

func NewEntity(tableName string, hashKey *dynamodb.Attribute, rangeKey *dynamodb.Attribute) (e Entity) {
	e = Entity{
		PrimaryKey: dynamodb.PrimaryKey{
			KeyAttribute:   hashKey,
			RangeAttribute: rangeKey,
		},
		TableName:  tableName,
		Attributes: make(map[string]dynamodb.Attribute),
		Related:    make(map[string]*Entity),
	}
	return
}

func (e *Entity) table() *dynamodb.Table {
	return dynamoServer.NewTable(e.TableName, e.PrimaryKey)
}
