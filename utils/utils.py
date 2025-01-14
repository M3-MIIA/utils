import os, sys, re, json, logging

import jwt

from datetime import date, datetime, time
from decimal import Decimal

from fastapi import FastAPI, APIRouter, Request, HTTPException, Depends
from fastapi.responses import JSONResponse
from mangum import Mangum

from async_lru import alru_cache

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.middleware.cors import CORSMiddleware

from botocore.exceptions import ClientError

from dbconn import DB

import boto3

service = os.environ['SERVICE_NAME']
region_name = os.environ['DEPLOY_AWS_REGION']

# Create a Secrets Manager client
boto3_session = boto3.session.Session()
secret_manager_client = boto3_session.client(service_name="secretsmanager", region_name=region_name)

TENANT_REGEX_PRD = re.compile(r"https://(?P<tenant>.+).miia.tech")
TENANT_REGEX_HML = re.compile(r"https://.*--m3par-miia.netlify.app")

IS_LOCAL = os.environ.get("ENVIRONMENT") == "local"

class ErrorResponse(RuntimeError):
    def __init__(self, *args, **kwargs):
        resp = make_error_response(*args, **kwargs)
        self.response = resp

        super().__init__(f"{resp['statusCode']} {resp.get('body', '-')}")


def echo_request(event):
    params = event["queryStringParameters"] or {}

    echo = "echo_request" in params and event["requestContext"]["stage"] == "dev"

    if echo:
        del params["echo_request"]
        del event["multiValueQueryStringParameters"]["echo_request"]

    return echo


def fetchone_to_dict(result):
    col_names = result.keys()
    row = result.fetchone()
    return dict(zip(col_names, row)) if row else None


def fetchall_to_dict(result):
    col_names = result.keys()
    data = result.fetchall()
    return [dict(zip(col_names, row)) for row in data]


def to_json(obj):
    if isinstance(obj, (date, datetime, time)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)

    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def make_response(status_code, body=None):
    headers = {"Access-Control-Allow-Origin": "*"}

    response = {"statusCode": status_code, "headers": headers}

    if body:
        headers["Content-Type"] = "application/json"
        response["body"] = json.dumps(body, default=to_json)

    return response


def make_error_response(
    status_code,
    message,
    extra_fields={},
    *,
    error_code: str = None,
    details: str = None,
):
    """
    Create an error response object using the same format as the AWS API Gateway
    errors (return the error in a 'message' field).
    """

    extra_fields = extra_fields.copy()

    if error_code is not None:
        extra_fields["error_code"] = error_code
    if details is not None:
        extra_fields["details"] = details

    return make_response(status_code, {"message": message, **extra_fields})


def get_tenant_id_from_headers(
    http_headers: dict,
) -> tuple[str, None] | tuple[None, dict]:
    """
    Use the tenant ID from the `X-Tenant-Id` HTTP request header (if given) or
    derive it from the URL in the `Origin` header.

    On success, return the tenant ID and None.

    If none of those headers are given, or if the URL in the `Origin` header
    doesn't match the expected platform domains, return None and an error
    response.
    """

    tenant_id = http_headers.get("x-tenant-id")
    if tenant_id is not None:
        return tenant_id, None

    origin = http_headers.get("origin")
    if origin is None:
        return None, make_error_response(400, "Missing origin header")

    match = TENANT_REGEX_PRD.match(origin)
    if match:
        tenant_id = match.group("tenant")
        return tenant_id, None

    match = TENANT_REGEX_HML.match(origin)
    if match:
        # Assume test deploys that don't send the X-Tenant-Id header as
        # belonging to the portal tenant.
        return "portal", None

    return None, make_error_response(400, "Invalid url")


def parse_body(body):
    if not body:
        raise HTTPException(400, "Missing request body")

    try:
        body = json.loads(body)
        return body
    except json.JSONDecodeError as e:
        logging.error(str(e))
        raise HTTPException(400, detail={"message":"Invalid request body format","error_code":"bad_request_body_format"})


def handle_param_id(param_id, param_name='param'):
    # For INTERGER parameters

    if not param_id:
        raise HTTPException(400, f"Missing {param_name}")
    try:
        param_id = int(param_id)
    except ValueError:
        raise HTTPException(400, f"Invalid {param_name} format")
    if param_id.bit_length() > 32:
        raise HTTPException(400, f"Invalid {param_name}")

    return param_id


def log_time(label: str, func):
    from time import time

    start = time()
    ret = func()
    end = time()

    logging.info(f"{label}: {end - start:.1f}s")
    return ret


async def a_log_time(label: str, func):
    from time import time

    start = time()
    ret = await func()
    end = time()

    logging.info(f"{label}: {end - start:.1f}s")
    return ret


def iam(event):
    http_headers = {k.lower(): v for k, v in event["headers"].items()}

    api_token = http_headers.get("x-api-key")
    if not api_token:
        raise HTTPException(400, "Must pass an API Key")

    try:
        tenant_code, _api_key = api_token.split("-")
    except ValueError:
        raise HTTPException(400, "Invalid API Key format")

    return tenant_code


@alru_cache
async def check_tenant(tenant_code, DB):
    sql = """
        INSERT INTO tenant (code)
        VALUES (:tenant_code)
        ON CONFLICT (code) DO UPDATE 
        SET code = EXCLUDED.code
        RETURNING id
    """

    async with DB.begin() as conn:
        result = await conn.execute(text(sql), {"tenant_code": tenant_code})
        return fetchone_to_dict(result)


async def parse_event(request):
    if IS_LOCAL:
        return {
            "headers": dict(request.headers),
            "body": await request.body(),
            "queryStringParameters": dict(request.query_params),
            "pathParameters": dict(request.path_params),
            "httpMethod": request.method,
            "resource": request.scope.get('path')
        }
    return request.scope["aws.event"]


def get_secret_key(aws_client, secret_name, key_name):
    try:
        return json.loads(aws_client.get_secret_value(SecretId=secret_name)["SecretString"]).get(key_name)
    except ClientError as e:
        logging.error(f"Erro ao obter o valor do secreto {secret_name}: {e}")
        return

async def get_session():
    async_session = sessionmaker(
            bind=DB,
            class_=AsyncSession,
            expire_on_commit=False,
        )
    async with async_session() as session:
        yield session

def _get_secret():

    secret_name = f"{service}/jwt-access-key"

    try:
        get_secret_value_response = secret_manager_client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response["SecretString"]

    if not secret:
        raise ValueError

    return json.loads(secret)

class JWTMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, secret_key: str, algorithm: str = "HS256"):
        super().__init__(app)
        self.secret_key = secret_key
        self.algorithm = algorithm

    async def dispatch(self, request: Request, call_next):
        token = request.headers.get("Authorization")
        if token:
            try:
                token = token.split(" ")[1]  # Remove 'Bearer' prefix
                payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
                request.state.user = payload  # Adiciona o payload ao estado da requisição
            except jwt.ExpiredSignatureError:
                return JSONResponse(status_code=401, content={"detail": "Token expired"})
            except jwt.InvalidTokenError:
                return JSONResponse(status_code=401, content={"detail": "Invalid token"})
        else:
            request.state.user = None

        response = await call_next(request)
        return response
   


def config(file=__file__):

    if not IS_LOCAL:
        router = FastAPI()  
        if service == 'portal':
            ACCESS_TOKEN_SECRET_KEY = _get_secret()["ACCESS_TOKEN_SECRET_KEY"]
            router.add_middleware(JWTMiddleware, secret_key=ACCESS_TOKEN_SECRET_KEY)
        router.add_middleware(CORSMiddleware,
                   allow_origins=['*'],
                   allow_credentials=True,
                   allow_methods=["*"],
                   allow_headers=["*"],)
        lambda_handler = Mangum(app=router)
    else:
        router = APIRouter()
        lambda_handler = None

    parent_dir = os.path.dirname(os.path.abspath(file))
    sys.path.insert(0, parent_dir)
    sys.path.insert(0, os.path.dirname(parent_dir))

    return router, lambda_handler, Request, parent_dir


async def set_schema(tenant_id, session):
    if tenant_id == 'portal': 
        await session.execute(text("SET search_path TO public"))
    else:
        await session.execute(text(f"SET search_path TO tenant_{tenant_id}"))