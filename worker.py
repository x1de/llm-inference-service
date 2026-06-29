import os
import asyncpg
from dotenv import load_dotenv
from google import genai
from google.genai import types
from arq.connections import RedisSettings
from arq import Retry

load_dotenv()
client = genai.Client(api_key=os.getenv("GENAI_API_KEY"))

async def startup(ctx): 
    ctx['db'] = await asyncpg.create_pool( # Creates connection pool at worker startup to allow for concurrent database access by multiple jobs without the overhead of establishing a new connection for each job.
        min_size=1, 
        max_size=20,
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"))

async def shutdown(ctx):
    await ctx['db'].close() # cleans up the connection pool when the worker shuts down

async def process_job(ctx, job_id: str, text: str, task: str):
    db = ctx['db']
    current_retry = ctx['job_try'] # Tracks the no. of retries for the current job. Starts at 1
    try:
        # client.aio exposes the async version of the Gemini client which is necessary to avoid blocking the event loop during LLM inference
        response = await client.aio.models.generate_content( # TODO: Change this to a streaming response to allow for real-time feedback to the user as the model generates content
            model="gemini-3.5-flash",
            contents=f"Perform the following task: {task} on the following text: {text}",  # Basic prompt to instruct the model's behavior
            config=types.GenerateContentConfig(
                thinking_config=types.ThinkingConfig(thinking_level=types.ThinkingLevel.LOW)),  # Low thinking level as it is an analysis task
        )
        # Process the response and save to database
        await db.execute("""
                        UPDATE jobs SET result = $1, status = $2, completed_at = NOW() WHERE id = $3
                        """,
                        response.text, 'completed', job_id) # Update the job status to completed and save the result in the database
    except Exception as e:
        if current_retry < 4: # Retry the job up to 3 times in case of failure
            backoff_delay = 2 ** current_retry # Exponential backoff strategy (e.g., 2s, 4s, 8s)
            raise Retry(defer = backoff_delay) # Raise a JobRetry exception to signal the worker to retry the job
        else:
            await db.execute("""
                        UPDATE jobs SET status = $1, completed_at = NOW() WHERE id = $2
                        """,
                        'failed', job_id) # Update the job status to failed when max retries are exceeded
            raise e # Re-raising the original error marks the job as 'failed' in Redis

class WorkerSettings:
    functions = [process_job]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = RedisSettings()
    max_tries = 4 # 1 initial attempt + 3 retries