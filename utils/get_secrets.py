import json

import boto3
from botocore.exceptions import ClientError


def _parse_secret(secret_obj):
    return f"postgresql+psycopg://{secret_obj['username']}:{secret_obj['password']}@{secret_obj['host']}:5432/{secret_obj['dbname']}"


def _get_secret():

    secret_name = "essay/postgres"
    region_name = "sa-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response["SecretString"]

    if not secret:
        raise ValueError

    return json.loads(secret)


def get_secret():
    secret = _get_secret()
    return _parse_secret(secret)

