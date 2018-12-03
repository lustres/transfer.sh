package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	sess *session.Session

	region     string
	domain     string
	s3Bucket   string
	dynmoTable string
	keyLen     int
)

var (
	defaultKeyLen = 5
)

func init() {
	region = os.Getenv("REGION")
	domain = os.Getenv("DOMAIN")
	s3Bucket = os.Getenv("S3_BUCKET")
	dynmoTable = os.Getenv("DYNMO_TABLE")

	l, err := strconv.Atoi(os.Getenv("KEY_LEN"))
	if err != nil {
		keyLen = defaultKeyLen
	} else {
		keyLen = l
	}

	sess = session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))
}

type transferItem struct {
	S3Key string `json:"s3key"`

	Filename string `json:"filename"`
	IP       string `json:"ip"`
	ExpireAt int64  `json:"expire_at"`
	Times    int    `json:"times"`
}

func (k *transferItem) GenKey() error {
	b := make([]byte, keyLen)
	if _, err := rand.Read(b); err != nil {
		return err
	}

	k.S3Key = hex.EncodeToString(b)
	return nil
}

func handleRequest(ctx context.Context, req events.APIGatewayProxyRequest) (resp events.APIGatewayProxyResponse, err error) {
	switch req.RequestContext.HTTPMethod {
	case http.MethodPut:
		return put(ctx, req)
	case http.MethodGet:
		return get(ctx, req)

	default:
		resp.StatusCode = http.StatusMethodNotAllowed
		return
	}
}

func put(ctx context.Context, req events.APIGatewayProxyRequest) (resp events.APIGatewayProxyResponse, err error) {
	var (
		av map[string]*dynamodb.AttributeValue

		dynmo = dynamodb.New(sess)

		r = transferItem{
			Filename: req.PathParameters["proxy"],
			IP:       req.RequestContext.Identity.SourceIP,
			ExpireAt: time.Now().Add(3 * 24 * time.Hour).Unix(),
		}
	)

	for {
		if err = r.GenKey(); err != nil {
			resp.StatusCode = http.StatusInternalServerError
			return
		}

		av, err = dynamodbattribute.MarshalMap(r)
		if err != nil {
			resp.StatusCode = http.StatusInternalServerError
			return
		}

		_, err = dynmo.PutItem(&dynamodb.PutItemInput{
			Item:                av,
			TableName:           aws.String(dynmoTable),
			ConditionExpression: aws.String("attribute_not_exists(s3key)"),
		})

		if err == nil {
			break
		}

		aerr, ok := err.(awserr.Error)
		if !ok || aerr.Code() != dynamodb.ErrCodeConditionalCheckFailedException {
			resp.StatusCode = http.StatusInternalServerError
			return
		}
	}

	// upload to s3
	_, err = s3.New(sess).PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(r.S3Key),
		Body:   strings.NewReader(req.Body),

		ContentDisposition: aws.String(fmt.Sprintf(`attachment; filename="%s"`, r.Filename)),
	})
	if err != nil {
		resp.StatusCode = http.StatusInternalServerError

		dynmo.DeleteItem(&dynamodb.DeleteItemInput{
			Key: map[string]*dynamodb.AttributeValue{
				"key": {
					S: aws.String(r.S3Key),
				},
			},
			TableName: aws.String(dynmoTable),
		})
		return
	}

	resp.StatusCode = 200
	resp.Body = domain + "/" + r.S3Key + "/" + r.Filename

	return
}

func get(ctx context.Context, req events.APIGatewayProxyRequest) (resp events.APIGatewayProxyResponse, err error) {
	parts := strings.SplitN(req.PathParameters["proxy"], "/", 2)

	if len(parts) != 2 {
		resp.StatusCode = http.StatusNotFound
		return
	}

	s3key, filename := parts[0], parts[1]

	if s3key == "" || filename == "" {
		resp.StatusCode = http.StatusNotFound
		return
	}

	// sign download url
	objReq, _ := s3.New(sess).GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(s3key),
	})

	url, err := objReq.Presign(15 * time.Minute)
	if err != nil {
		resp.StatusCode = http.StatusInternalServerError
		return
	}

	// update dynamodb
	_, err = dynamodb.New(sess).UpdateItem(&dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"s3key": {
				S: aws.String(s3key),
			},
		},
		TableName:           aws.String(dynmoTable),
		ReturnValues:        aws.String("NONE"),
		UpdateExpression:    aws.String("ADD times :one"),
		ConditionExpression: aws.String("attribute_exists(s3key) and times < :three"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":one": {
				N: aws.String("1"),
			},
			":three": {
				N: aws.String("3"),
			},
		},
	})

	if err == nil {
		resp.StatusCode = http.StatusFound
		resp.Headers = map[string]string{
			"Location": url,
		}
		return
	}

	if aerr, ok := err.(awserr.Error); ok && aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
		resp.StatusCode = http.StatusNotFound
		err = nil
		return
	}

	resp.StatusCode = http.StatusInternalServerError
	return
}

func main() {
	lambda.Start(handleRequest)
}
