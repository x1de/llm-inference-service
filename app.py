import os
import asyncpg
import hashlib
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, Request, responses
from pydantic import BaseModel
from dotenv import load_dotenv
from arq import create_pool
from arq.connections import RedisSettings
from fastapi.security import APIKeyHeader
import time
import redis.asyncio as aioredis

load_dotenv()

class JobRequest(BaseModel):
    text: str
    task: str

class JobResponse(BaseModel):
    id: str
    user_id: str
    result: str | None = None
    status: str

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False) # Registers security scheme with OpenAPI spec for Swagger UI

# Paths that do not require API key authentication, such as registration and health check endpoints, as well as documentation endpoints.
EXCLUDED_PATHS = {"/register", "/health", "/docs", "/openapi.json", "/redoc"} 

RATE_LIMIT_SCRIPT = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(bucket[1])
local last_refill = tonumber(bucket[2])

-- if bucket doesn't exist yet, initialize it
if tokens == nil then
    tokens = capacity
    last_refill = now
end

-- refill the bucket based on the time elapsed since last refill
local time_elapsed = now - last_refill
tokens = math.min(capacity, tokens + time_elapsed * refill_rate)

local allowed = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
end
redis.call("HSET", key, "tokens", tokens, "last_refill", now)
return allowed
"""

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connection pool ensures concurrent requests can be handled efficiently without the overhead of establishing a new connection for each request.
    app.state.pool = await asyncpg.create_pool(
        min_size=1, 
        max_size=20,
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"))
    app.state.redis_pool = await create_pool(RedisSettings()) # Arq redis pool manages connections to Redis specifically for job queuing
    app.state.redis = aioredis.from_url("redis://localhost:6379") # Redis connection for rate limiting
    yield
    await app.state.pool.close()
    await app.state.redis_pool.close()

app = FastAPI(
    lifespan=lifespan,  # Database and redis are intialized at app startup and closed at shutdown
    swagger_ui_parameters={"persistAuthorization": True}) # Persists auth creds entered across sessions in Swagger UI for convenience during testing

async def get_db(): # Dependency to provide db connection to route handlers.
    async with app.state.pool.acquire() as connection: 
        yield connection

async def get_redis(): # Dependency to provide redis connection to route handlers.
    yield app.state.redis_pool

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if request.url.path in EXCLUDED_PATHS:
        return await call_next(request)
    try:
        api_key = request.headers.get("X-API-Key")
        if api_key:
                hashed_key = hashlib.sha256(api_key.encode()).hexdigest()
        else:
            return responses.JSONResponse(status_code=401, content={"detail": "API Key missing"})
        async with app.state.pool.acquire() as connection: 
            request.state.db = connection 
            user = await request.state.db.fetchrow("SELECT * FROM users WHERE api_key = $1", hashed_key)
            if not user:
                return responses.JSONResponse(status_code=404, content={"detail": "User not found"})
            request.state.user = user
        
        # Rate limiting logic
        user_id = str(request.state.user['id'])
        rate_limit_key = f"rate_limit:{user_id}"
        current_time = int(time.time())
        allowed = await app.state.redis.eval(RATE_LIMIT_SCRIPT, 1, rate_limit_key, 5, 5/60, current_time)# 5 requests per second
        if allowed == 0:
            return responses.JSONResponse(status_code=429, content={"detail": "Rate limit exceeded. Please try again later."})
        
    # 1. Catches native asyncpg connection drops / handshake failures
    except (asyncpg.PostgresConnectionError, asyncpg.InterfaceError) as e:
        print(f"Database network error encountered: {e}")
        return responses.JSONResponse(status_code=503, content={"detail": "Database connection failed. Service is temporarily unavailable."})

    # 2. Catches OS-level errors (e.g., ConnectionRefusedError when port is dead)
    except OSError as e:
        print(f"OS-level network error (DB might be completely powered off): {e}")
        return responses.JSONResponse(status_code=503, content={"detail": "Could not reach the database host server."})
    
    except Exception as e:
        return responses.JSONResponse(status_code=401, content={"detail": "Invalid API Key"})
    
    response = await call_next(request)
    return response


@app.post("/jobs", dependencies=[Depends(api_key_header)])
async def create_job(request: Request, body: JobRequest, db: asyncpg.Connection = Depends(get_db), redis = Depends(get_redis)) -> dict:
    '''
    Endpoint to create a new job. It accepts a JSON payload with 'text' and 'task' fields, inserts a new job into the database, 
    and enqueues the job for processing in Redis.
    Args:
        request (Request): The incoming request object.
        body (JobRequest): The request body containing the text and task type.
        db (asyncpg.Connection): The database connection, provided by the get_db dependency.
        redis: The Redis connection, provided by the get_redis dependency.
    Returns:
        dict: A dictionary containing the result message and the job ID.
    '''
    text = body.text     
    task = body.task
    user_id = str(request.state.user['id']) # Extracts the user ID from the request state set in the auth middleware after validating the API key.
    try:
        job_id = await db.fetchval("""
                        INSERT INTO jobs (user_id,input_text,task_type,status) 
                        VALUES ($1, $2, $3, $4)
                        RETURNING id
                        """, 
                        user_id,text, task, 'pending')
        await redis.enqueue_job('process_job', job_id, text, task) # Enqueue the job for processing in Redis using the 'process_job' function defined in worker.py
        return {"result": "Job created successfully", "job_id": job_id} # Return a success message along with the job ID to the client so that they don't have to wait for the job to complete and can check back later for the result.
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs/{job_id}", dependencies=[Depends(api_key_header)])
async def get_job(request: Request,job_id: str, db: asyncpg.Connection = Depends(get_db)) -> JobResponse:
    '''
    Endpoint to retrieve the status and result of a job by its ID. It queries the database for the job details and returns them.
    Args:
        job_id (str): The ID of the job to retrieve.
        db (asyncpg.Connection): The database connection, provided by the get_db dependency.
    Returns:
        JobResponse: An instance of the JobResponse Pydantic model containing the job details.
    '''
    user_id = str(request.state.user['id'])
    job = await db.fetchrow("SELECT id, user_id, result, status FROM jobs WHERE id = $1 and user_id = $2", job_id, user_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    job_dict = dict(job)
    job_dict['id'] = str(job_dict['id'])
    return JobResponse(**job_dict)