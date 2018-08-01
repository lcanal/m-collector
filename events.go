package main

import (
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
	BatchSize := 20
	var writeRequests []*dynamodb.WriteRequest

	//Batch limit is 400kb or 25 items. Using 20 by default
	for index := 0; index < len(nae.ModulesUsed); index++ {
		moduleEntry := nae.ModulesUsed[index]
		newPutRequest := &dynamodb.PutRequest{
			Item: map[string]*dynamodb.AttributeValue{
				"AppName": {
					S: aws.String(nae.AppName),
				},
				"ModuleName": {
					S: aws.String(moduleEntry.Name),
				},
			},
		}
		writeRequests = append(writeRequests, &dynamodb.WriteRequest{
			PutRequest: newPutRequest,
		})

		if index%BatchSize == 0 || index == (len(nae.ModulesUsed)-1) {
			//Time to create full batchwriteiteminput from putrequest arrays
			input := &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]*dynamodb.WriteRequest{
					MainTableName: writeRequests,
				},
			}
			sendDynamoItems(input)
			writeRequests = nil
		}
	}
	return
}

func sendDynamoItems(itemsInput *dynamodb.BatchWriteItemInput) {
	result, err := DynamoSession.BatchWriteItem(itemsInput)
	if err != nil {
		log.Printf("Error writing batch items: %v\n", err.Error())
		return
	}
	log.Printf("Success: %s\n", result.GoString())
}
