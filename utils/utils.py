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
from fastapi.exceptions import RequestValidationError

from pydantic import ValidationError

from botocore.exceptions import ClientError

import boto3


class _InitOnFirstUse:
    """
    Proxy class to initialize an object on its first use.

    The `init` function given in the constructor is called when the first class
    member is accessed. E.g.:
    ```
    def _init_db():
        from dbconn import connect_to_db
        return connect_to_db()

    DB = _InitOnFirstUse(_init_db)

    with DB.begin():  # Access to `begin` member will trigger `init`
        ...
    ```
    """

    def __init__(self, init):
        self._obj = None
        self._init = init

    def __getattr__(self, name: str):
        if self._obj is None:  # Init on first use
            self._obj = self._init()

        return getattr(self._obj, name)

def _init_db():
    from dbconn import connect_to_db
    return connect_to_db()

DB = _InitOnFirstUse(_init_db)

def _init_secrets_manager_client():
    # Create a Secrets Manager client
    boto3_session = boto3.session.Session()
    return boto3_session.client(service_name="secretsmanager")

secret_manager_client = _InitOnFirstUse(_init_secrets_manager_client)

TENANT_REGEX_PRD = re.compile(r"https://(?P<tenant>.+).miia.tech")
TENANT_REGEX_HML = re.compile(r"https://.*--m3par-miia.netlify.app")

def is_local():
    env = os.environ.get("ENVIRONMENT")
    return env == "local"


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
    if is_local():
        try:
            body = await request.json()
        except Exception:
            body = {}

        return {
            "headers": dict(request.headers),
            "body": body,
            "queryStringParameters": dict(request.query_params),
            "pathParameters": dict(request.path_params),
            "httpMethod": request.method,
            "resource": request.scope.get('path')
        }

    aws_event = request.scope["aws.event"]
    aws_event["queryStringParameters"] = {} if aws_event["queryStringParameters"] == None else aws_event["queryStringParameters"]

    try:
        aws_event["body"] = json.loads(aws_event["body"])
    except Exception as e:
        print("Error parsing body", str(e))
        pass

    return aws_event


def get_secret_key(aws_client, secret_name, key_name):
    try:
        return json.loads(aws_client.get_secret_value(SecretId=secret_name)["SecretString"]).get(key_name)
    except ClientError as e:
        logging.error(f"Erro ao obter o valor do secreto {secret_name}: {e}")
        return

class SessionFactory:
    def __init__(self, session):
        self._session = session

    async def list_tenants(self):
        async with self._session.begin():
            sql = """
                SELECT code
                FROM tenant
            """
            result = await self._session.execute(text(sql), {})
            tenants = fetchall_to_dict(result)

            logging.info("Tenants listed")

            return [t['code'] for t in tenants]

    async def _set_schema(self,tenant_code):
        if tenant_code == 'portal':
            await self._session.execute(text("SET search_path TO public"))
        else:
            await self._session.execute(text(f"SET search_path TO tenant_{tenant_code}"))

    async def _get_session_portal(self, tenant_code):
        await self._set_schema(tenant_code)

        logging.info(f"Connected with tenant: {tenant_code}")

        return self._session, tenant_code

    async def _get_service_session(self, tenant_code):
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

    async def get_session(self, tenant_code):
        async with self._session.begin():
            if os.environ['SERVICE_NAME'] == 'portal':
                return await self._get_session_portal(tenant_code)
            else:
                return await self._get_service_session(tenant_code)



def _make_session():
    return sessionmaker(
        bind=DB,
        class_=AsyncSession,
        expire_on_commit=False,
    )

async def session_factory():
    """
    Use this function as a dependency in FastAPI routes.
    E.g.:
    ```
    @router.get("…")
    async def root(session: AsyncSession = Depends(session_factory)):
        session, tenant_id = await session_factory.get_session(tenant_code)
        async with session.begin():
            …
    ```
    """
    async_session = _make_session()

    async with async_session() as session:
        yield SessionFactory(session)

def with_session(func):
    """
    Use this function to wrap non-route λ functions (e.g. EventBridge events or
    SQS queue consumers).
    E.g.:
    ```
    def main(event, context, session):
        session, tenant_id = await session_factory.get_session(tenant_code)
        async with session.begin():
            …
    lambda_handler = with_session(main)
    ```
    """
    from functools import wraps
    @wraps(func)
    async def new_func(*args, **kwargs):
        async_session = _make_session()
        async with async_session() as session:
            return await func(*args, **kwargs, session_factory=SessionFactory(session))
    return new_func

def _get_secret():
    service = os.environ['SERVICE_NAME']
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
    if not is_local():
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

        @app.exception_handler(RequestValidationError)
        async def validation_exception_handler(request: Request, exc: RequestValidationError):
            # EM UM PRIMEIRO MOMENTO ESSE FORMATO SERÁ USADO APENAS NO MIIA-ESSAY
            if isinstance(exc.errors(), list):
                for error in exc.errors():
                    if 'ctx' in error and 'error' in error['ctx']:
                        error_obj = error['ctx']['error']
                        if isinstance(error_obj, ValueError) and error_obj.args:
                            if isinstance(error_obj.args[0], dict):
                                raise HTTPException(status_code=400, detail=error_obj.args[0])
                    elif 'msg' in error:
                        if "{" in error['msg'] and "error_code" in error['msg']:
                            start_index = error['msg'].find("{")
                            raise HTTPException(status_code=400, detail=eval(error['msg'][start_index:]))
                raise HTTPException(status_code=400, detail={"errors": exc.errors(), "message": "Validation error"})
            raise HTTPException(status_code=422, detail=exc.errors())


        lambda_handler = Mangum(app=app)
    else:
        app = APIRouter()
        lambda_handler = None


    return app, lambda_handler


def serverless_warmup(func):
    """
    Decorator for lambda functions which use the Serverless Warmup Plugin to
    avoid cold starts.

    This decorator handles warmup invocations and return preemptively without
    running the actual function code as it is not a legit execution invocation.
    Only an INFO-level log entry is created for warmup invocations.

    Decorate the handler Python function that is the entrypoint of the lambda
    function with this decorator:
    ```
    # Standard handler function:
    @serverless_warmup
    def lambda_handler(event, context):
        ...  # Function logic


    # Mangum APIs:
    app = FastAPI()

    @app.get("/")
    def get_root():
        ...  # Route logic

    handler = serverless_warmup(Mangum(app))
    ```

    More info: https://www.serverless.com/plugins/serverless-plugin-warmup#on-the-function-side
    """

    def serverless_warmup_wrapper(event, context):
        if event.get("source") == "serverless-plugin-warmup":
            logging.info("Serverless warmup plugin invocation, nothing to do")
            return {}
        else:
            return func(event, context)

    return serverless_warmup_wrapper