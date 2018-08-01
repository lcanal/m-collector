package main

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/lcanal/mcollector/mmodels"
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
				AttributeName: aws.String("ModuleName"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("UsedInApp"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("ModuleName"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("UsedInApp"),
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
func WriteDynamoItem(nae mmodels.NewAppEntry) {
	MainTableName := "ApplicationModules"
	BatchSize := 20
	var writeRequests []*dynamodb.WriteRequest

	checkEmpties(&nae)

	//Batch limit is 400kb or 25 items. Using 20 by default
	for index := 0; index < len(nae.ModulesUsed); index++ {
		moduleEntry := nae.ModulesUsed[index]
		newPutRequest := &dynamodb.PutRequest{
			Item: map[string]*dynamodb.AttributeValue{
				"ModuleName": {
					S: aws.String(moduleEntry.Name),
				},
				"UsedInApp": {
					S: aws.String(nae.AppName),
				},
				"ModuleVersion": {
					S: aws.String(moduleEntry.Version),
				},
				"ModuleDescription": {
					S: aws.String(moduleEntry.Description),
				},
				"ModuleHomepage": {
					S: aws.String(moduleEntry.Homepage),
				},
				"DateAdded": {
					S: aws.String(time.Now().Format(time.RFC3339)),
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

func checkEmpties(n *mmodels.NewAppEntry) {
	//check empty values and give them happy defaults
	for idx, module := range n.ModulesUsed {
		if len(module.Description) == 0 {
			n.ModulesUsed[idx].Description = "No description listed"
		}
		if len(module.Homepage) == 0 {
			n.ModulesUsed[idx].Homepage = "No homepage listed"
		}
	}
}

func sendDynamoItems(itemsInput *dynamodb.BatchWriteItemInput) {
	_, err := DynamoSession.BatchWriteItem(itemsInput)
	if err != nil {
		log.Printf("Error writing batch items: %v\n", err.Error())
		time.Sleep(3 * time.Second)
		log.Printf("Retrying..")
		_, err := DynamoSession.BatchWriteItem(itemsInput)
		if err != nil {
			log.Printf("Error writing batch items: %v\n", err.Error())
			time.Sleep(9 * time.Second)
			log.Printf("Retrying last time..")
			_, err := DynamoSession.BatchWriteItem(itemsInput)
			if err != nil {
				log.Printf("Could not write batch items. Error: %v\nInput:%v", err.Error(), itemsInput)
			}
		}
	}
	return
}
