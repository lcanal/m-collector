package main

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

//InitDynamoConnection initializes dynamo connection.
func InitDynamoConnection() {
	DynamoSession = dynamodb.New(session.New())
}

//CreateDynamoTable create the main used dynamo table
func CreateDynamoTable() error {
	MainTableName := "ApplicationModules"

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("AppName"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("ModuleName"),
				AttributeType: aws.String("S"),
			},
			// {
			// 	AttributeName: aws.String("ModuleVersion"),
			// 	AttributeType: aws.String("S"),
			// },
			// {
			// 	AttributeName: aws.String("ModuleDescription"),
			// 	AttributeType: aws.String("S"),
			// },
			// {
			// 	AttributeName: aws.String("ModuleHomepage"),
			// 	AttributeType: aws.String("S"),
			// },
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("AppName"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("ModuleName"),
				KeyType:       aws.String("RANGE"),
			},
			// {
			// 	AttributeName: aws.String("ModuleVersion"),
			// 	KeyType:       aws.String("RANGE"),
			// },
			// {
			// 	AttributeName: aws.String("ModuleDescription"),
			// 	KeyType:       aws.String("RANGE"),
			// },
			// {
			// 	AttributeName: aws.String("ModuleHomepage"),
			// 	KeyType:       aws.String("RANGE"),
			// },
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		TableName: aws.String(MainTableName),
	}

	_, err := DynamoSession.CreateTable(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeResourceInUseException:
				log.Println(dynamodb.ErrCodeResourceInUseException, aerr.Error())
			/*case dynamodb.ErrCodeLimitExceededException:
				fmt.Println(dynamodb.ErrCodeLimitExceededException, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())*/
			default:
				log.Println(aerr.Error())
				return err
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			log.Println(err.Error())
			return err
		}
	}
	return nil
}

//WriteDynamoItem writes the item to dynamo
func WriteDynamoItem(nae NewAppEntry) {
	MainTableName := "ApplicationModules"

	input := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"AppName": {
				S: aws.String(nae.AppName),
			},
			"ModuleName": {
				S: aws.String(nae.ModulesUsed[0].Name),
			},
			"ModuleVersion": {
				S: aws.String(nae.ModulesUsed[0].Version),
			},
		},
		ReturnConsumedCapacity: aws.String("TOTAL"),
		TableName:              aws.String(MainTableName),
	}

	_, err := DynamoSession.PutItem(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				fmt.Println(dynamodb.ErrCodeConditionalCheckFailedException, aerr.Error())
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				fmt.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
	}

	return
}
