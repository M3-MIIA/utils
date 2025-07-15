from fastapi import FastAPI
from mangum import Mangum

from utils import serverless_warmup


SERVERLESS_WARMUP_STANDARD_EVENT = {
    "source": "serverless-plugin-warmup"
}

DUMMY_LAMBDA_HANDLER_SUCCESS = "Hello Loren Ipsum world! Lambda handler ran, input: "


@serverless_warmup
def dummy_lambda_handler(event, context):
    output =  DUMMY_LAMBDA_HANDLER_SUCCESS + event.get("input")
    return output


def test_standard_warmup():
    output = dummy_lambda_handler(SERVERLESS_WARMUP_STANDARD_EVENT, None)
    assert output == {}

def test_standard_execution():
    value = "42"
    output = dummy_lambda_handler({ "input": value }, None)
    assert output == DUMMY_LAMBDA_HANDLER_SUCCESS + value


api = FastAPI()

@api.get("/")
def mock_route(input: str):
    return DUMMY_LAMBDA_HANDLER_SUCCESS + input

dummy_mangum_handler = serverless_warmup(Mangum(api))

def test_mangum_warmup():
    output = dummy_mangum_handler(SERVERLESS_WARMUP_STANDARD_EVENT, None)
    assert output == {}

def test_mangum_execution():
    value = "129"

    request_event = {
        "version": "2.0",
        "routeKey": "$default",
        "rawPath": "/",
        "rawQueryString": f"input={value}",
        "cookies": [ ],
        "headers": { },
        "queryStringParameters": { "input": value },
        "requestContext": {
            "accountId": "123456789012",
            "apiId": "api-id",
            "authentication": {
                "clientCert": {
                    "clientCertPem": "CERT_CONTENT",
                    "subjectDN": "www.example.com",
                    "issuerDN": "Example issuer",
                    "serialNumber": "a1:a1:a1:a1:a1:a1:a1:a1:a1:a1:a1:a1:a1:a1:a1:a1",
                    "validity": {
                        "notBefore": "May 28 12:30:02 2019 GMT",
                        "notAfter": "Aug  5 09:36:04 2021 GMT"
                    }
                }
            },
            "authorizer": {
                "jwt": { },
            },
            "domainName": "id.execute-api.us-east-1.amazonaws.com",
            "domainPrefix": "id",
            "http": {
                "method": "GET",
                "path": "/",
                "protocol": "HTTP/1.1",
                "sourceIp": "192.0.2.1",
                "userAgent": "agent"
            },
            "requestId": "id",
            "routeKey": "$default",
            "stage": "$default",
            "time": "12/Mar/2020:19:03:58 +0000",
            "timeEpoch": 1583348638390
        },
        "body": "Hello from Lambda",
        "pathParameters": { },
        "isBase64Encoded": False,
        "stageVariables": { }
    }

    output = dummy_mangum_handler(request_event, None)
    assert output.get("body") == f'"{DUMMY_LAMBDA_HANDLER_SUCCESS + value}"'
