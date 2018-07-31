package main

import (
	"encoding/json"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

//DynamoSession main service session to Dynamo
var DynamoSession *dynamodb.DynamoDB

//Handler Main lambda entrypoint
func Handler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	//Request has lots of context stuff. Just want the body.
	var entry NewAppEntry
	err := json.Unmarshal([]byte(request.Body), &entry)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       err.Error(),
		}, err
	}

	/* Only needed if responding back in json object*/
	jsonResponse, err := json.Marshal(entry)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       err.Error(),
		}, err
	}
	/* END Only needed if responding back in json object*/

	WriteDynamoItem(entry)

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       string(jsonResponse),
	}, nil
}

func main() {
	log.Printf("Go on take everything!!")
	//Init db connection and check if table exists
	InitDynamoConnection()
	err := CreateDynamoTable()
	if err != nil {
		log.Printf("Error: %e", err)
	}
	lambda.Start(Handler)
}
