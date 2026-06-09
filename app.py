import os
import asyncpg
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

class JobRequest(BaseModel):
    text: str
    task: str

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
    yield
    await app.state.pool.close()

app = FastAPI(lifespan=lifespan)
client = genai.Client(api_key=os.getenv("GENAI_API_KEY"))

async def get_db():
    async with app.state.pool.acquire() as connection:
        yield connection

@app.post("/jobs")
async def create_job(request: JobRequest, db: asyncpg.Connection = Depends(get_db)):
    text = request.text     
    task = request.task     
    try:
        # client.aio exposes the async version of the Gemini client which is necessary to avoid blocking the event loop during LLM inference
        response = await client.aio.models.generate_content(
            model="gemini-3.5-flash",
            contents=f"Perform the following task: {task} on the following text: {text}",  # Basic prompt to instruct the model's behavior
            config=types.GenerateContentConfig(
                thinking_config=types.ThinkingConfig(thinking_level=types.ThinkingLevel.LOW)),  # Low thinking level as it is an analysis task
        )
        job_id = await db.fetchval("""
                        INSERT INTO jobs (user_id,input_text,task_type,result,status) 
                        VALUES ('test-user', $1, $2, $3, $4)
                        RETURNING id
                        """, 
                        text, task, response.text, 'completed')
        return {"result": f"{response.text}", "job_id": job_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
