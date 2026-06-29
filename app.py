import os
import asyncpg
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from arq import create_pool
from arq.connections import RedisSettings

load_dotenv()

class JobRequest(BaseModel):
    text: str
    task: str

class JobResponse(BaseModel):
    id: str
    user_id: str
    result: str | None = None
    status: str

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
    yield
    await app.state.pool.close()
    await app.state.redis_pool.close()
app = FastAPI(lifespan=lifespan) # Database and redis are intialized at app startup and closed at shutdown

async def get_db(): # Dependency to provide db connection to route handlers.
    async with app.state.pool.acquire() as connection: 
        yield connection

async def get_redis(): # Dependency to provide redis connection to route handlers.
    yield app.state.redis_pool

@app.post("/jobs")
async def create_job(request: JobRequest, db: asyncpg.Connection = Depends(get_db), redis = Depends(get_redis)) -> dict:
    '''
    Endpoint to create a new job. It accepts a JSON payload with 'text' and 'task' fields, inserts a new job into the database, 
    and enqueues the job for processing in Redis.
    Args:
        request (JobRequest): The request body containing the text and task type.
        db (asyncpg.Connection): The database connection, provided by the get_db dependency.
        redis: The Redis connection, provided by the get_redis dependency.
    Returns:
        dict: A dictionary containing the result message and the job ID.
    '''
    text = request.text     
    task = request.task      
    try:
        job_id = await db.fetchval("""
                        INSERT INTO jobs (user_id,input_text,task_type,status) 
                        VALUES ('test-user', $1, $2, $3)
                        RETURNING id
                        """, 
                        text, task, 'pending')
        await redis.enqueue_job('process_job', job_id, text, task) # Enqueue the job for processing in Redis using the 'process_job' function defined in worker.py
        return {"result": "Job created successfully", "job_id": job_id} # Return a success message along with the job ID to the client so that they don't have to wait for the job to complete and can check back later for the result.
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs/{job_id}")
async def get_job(job_id: str, db: asyncpg.Connection = Depends(get_db)) -> JobResponse:
    '''
    Endpoint to retrieve the status and result of a job by its ID. It queries the database for the job details and returns them.
    Args:
        job_id (str): The ID of the job to retrieve.
        db (asyncpg.Connection): The database connection, provided by the get_db dependency.
    Returns:
        JobResponse: An instance of the JobResponse Pydantic model containing the job details.
    '''
    job = await db.fetchrow("Select id, user_id, result, status from jobs where id = $1", job_id) 
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobResponse(**dict(job))