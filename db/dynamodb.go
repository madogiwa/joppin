/*
Copyright © 2021-2022 Hidenori Sugiyama <madogiwa@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package db

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"log"
	"time"
)

type LockClient interface {
	Lock()
	Unlock()
}

type DynamoDBLockClient struct {
	tableName string
	svc       *dynamodb.DynamoDB
}

type LockItem struct {
	LockID string
	Ttl    int64
}

func NewDynamoDBLockClient(tableName string) *DynamoDBLockClient {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := dynamodb.New(sess)

	return &DynamoDBLockClient{
		tableName: tableName,
		svc:       svc,
	}
}

func (c *DynamoDBLockClient) Lock(key string, timeout int64) {
	dt := time.Now()
	ttl := dt.Unix() + timeout

	item := LockItem{
		LockID: key,
		Ttl:    ttl,
	}

	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		log.Fatalf("Got error marshalling item: %s", err)
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(c.tableName),
		Expected: map[string]*dynamodb.ExpectedAttributeValue{
			"LockID": {
				Exists: aws.Bool(false),
			},
		},
	}

	_, err = c.svc.PutItem(input)
	if err != nil {
		log.Fatalf("Got error calling PutItem: %s", err)
	}
}

func (c *DynamoDBLockClient) Unlock(key string) {
	// Delete item from table
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(c.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"LockID": {
				S: aws.String(key),
			},
		},
	}

	_, err := c.svc.DeleteItem(input)
	if err != nil {
		log.Fatalf("Got error calling DeleteItem: %s", err)
	}
}
