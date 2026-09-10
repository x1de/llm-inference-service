import os
import asyncpg
import json
import logging
import time
import asyncio
from dotenv import load_dotenv
from google import genai
from google.genai import types
from arq.connections import RedisSettings
from arq import Retry

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ticketiq.worker")
redis_host = os.getenv("REDIS_HOST", "localhost")

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
    start_time = time.perf_counter()
    try:
        logger.info(json.dumps({"event": "job_processing", "job_id": str(job_id), "attempt": current_retry}))
        if os.getenv("LLM_PROVIDER", "gemini") == "stub":
            # The local stub keeps Docker and load tests free while still behaving like a slow external API call.
            await asyncio.sleep(float(os.getenv("STUB_DELAY_SECONDS", "0.25")))
            result_text = " ".join(text.split()[:40])
            input_tokens = len(text.split())
            output_tokens = len(result_text.split())
            total_tokens = input_tokens + output_tokens
        else:
            client = genai.Client(api_key=os.getenv("GENAI_API_KEY"))
            # client.aio exposes the async version of the Gemini client which is necessary to avoid blocking the event loop during LLM inference
            response = await client.aio.models.generate_content( # TODO: Change this to a streaming response to allow for real-time feedback to the user as the model generates content
                model=os.getenv("GEMINI_MODEL", "gemini-3.5-flash"),
                contents=f"Perform the following task: {task} on the following text: {text}",  # Basic prompt to instruct the model's behavior
                config=types.GenerateContentConfig(
                    thinking_config=types.ThinkingConfig(thinking_level=types.ThinkingLevel.LOW)),  # Low thinking level as it is an analysis task
            )
            result_text = response.text
            usage = response.usage_metadata
            input_tokens = usage.prompt_token_count or 0 if usage else 0
            output_tokens = usage.candidates_token_count or 0 if usage else 0
            total_tokens = usage.total_token_count or input_tokens + output_tokens if usage else 0

        # Process the response and save to database
        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
        await db.execute("""
                        UPDATE jobs SET result = $1, status = $2, completed_at = NOW(),
                        input_tokens = $3, output_tokens = $4, total_tokens = $5 WHERE id = $6
                        """,
                        result_text, 'completed', input_tokens, output_tokens, total_tokens, job_id) # Update the job status to completed and save the result and token usage in the database
        logger.info(json.dumps({
            "event": "job_completed",
            "job_id": str(job_id),
            "duration_ms": duration_ms,
            "tokens_used": total_tokens
        }))
    except Exception as e:
        if current_retry < 4: # Retry the job up to 3 times in case of failure
            backoff_delay = 2 ** current_retry # Exponential backoff strategy (e.g., 2s, 4s, 8s)
            raise Retry(defer = backoff_delay) # Raise a JobRetry exception to signal the worker to retry the job
        else:
            await db.execute("""
                        UPDATE jobs SET status = $1, completed_at = NOW() WHERE id = $2
                        """,
                        'failed', job_id) # Update the job status to failed when max retries are exceeded
            logger.error(json.dumps({
                "event": "job_failed",
                "job_id": str(job_id),
                "attempts": current_retry,
                "duration_ms": round((time.perf_counter() - start_time) * 1000, 2)
            }))
            raise e # Re-raising the original error marks the job as 'failed' in Redis

class WorkerSettings:
    functions = [process_job]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = RedisSettings(host=redis_host)
    max_tries = 4 # 1 initial attempt + 3 retries
