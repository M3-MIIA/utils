import os, re, json, logging

import jwt

from datetime import date, datetime, time
from decimal import Decimal

from fastapi import FastAPI, APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from mangum import Mangum

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.middleware.cors import CORSMiddleware

from pydantic import ValidationError

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
        return float(obj)

    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


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


async def parse_event(request):
    try:
        body = await request.json()
    except Exception:
        body = {}
        
    if IS_LOCAL:
        return {
            "headers": dict(request.headers),
            "body": body,
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

class SessionFactory:
    def __init__(self, session):
        self._session = session

    async def get_session(self, tenant_code):
        async with self._session.begin():
            sql = """
                INSERT INTO tenant (code)
                VALUES (:tenant_code)
                ON CONFLICT (code) DO UPDATE 
                SET code = EXCLUDED.code
                RETURNING id
            """

            result = await self._session.execute(text(sql), {"tenant_code": tenant_code})
            tenant_id = fetchone_to_dict(result)['id']
            
            logging.info(f"Connected with tenant: {tenant_code} - ID: {tenant_id}")
            
            return self._session, tenant_id


async def session_factory():
    async_session = sessionmaker(
            bind=DB,
            class_=AsyncSession,
            expire_on_commit=False,
        )
    
    async with async_session() as session:
        yield SessionFactory(session)

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
            return JSONResponse(status_code=401, content={"detail": "Missing Authorization Credentials"})

        response = await call_next(request)
        return response
   


def config(jwt_auth=False, access_token_secret_key=None):
    if not IS_LOCAL:
        app = FastAPI()  
        if jwt_auth:
            if not access_token_secret_key:
                access_token_secret_key = _get_secret()["ACCESS_TOKEN_SECRET_KEY"]
            app.add_middleware(JWTMiddleware, secret_key=access_token_secret_key)
        app.add_middleware(CORSMiddleware,
                   allow_origins=['*'],
                   allow_credentials=True,
                   allow_methods=["*"],
                   allow_headers=["*"],)

        @app.exception_handler(Exception)
        async def global_exception_handler(request: Request, exc: Exception):
            logging.error(f"Server error occurred: {exc}")
            return JSONResponse(
                status_code=500,
                content={"message": "Internal server error.", "error_code": "internal_server_error"},
            )
        
        @app.middleware("http")
        async def custom_middleware(request: Request, call_next):
            try:
                response = await call_next(request)
                return response
            except ValidationError as exc:
                logging.error(f"Erro de validação (APIRouter): {exc}")
                return JSONResponse(
                    status_code=422,
                    content={
                        "errors": exc.errors(),
                        "error_code": "invalid_field_value",
                        "message": "Invalid request body format"
                    }
                )
        lambda_handler = Mangum(app=app)
    else:
        app = APIRouter()
        lambda_handler = None


    return app, lambda_handler


async def set_schema(tenant_id, session):
    if tenant_id == 'portal': 
        await session.execute(text("SET search_path TO public"))
    else:
        await session.execute(text(f"SET search_path TO tenant_{tenant_id}"))
