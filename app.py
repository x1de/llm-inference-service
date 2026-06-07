import os
from fastapi import FastAPI, types
from pydantic import BaseModel
from psycopg2 import pool
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

client = genai.Client(api_key=os.getenv("GENAI_API_KEY"))

class JobRequest(BaseModel):
    text: str
    task: str

app = FastAPI()

# Connection pool ensures concurrent requests can be handled efficiently without the overhead of establishing a new connection for each request.
conn_pool = pool.SimpleConnectionPool(
    1, 20,
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"))

@app.post("/jobs")
def create_job(request: JobRequest):
    text = request.text     
    task = request.task     
    connection = None   # Initialize connection variable to None for proper handling in the finally block (if an exception occurs before the connection is established)
    cursor = None   # Initialize cursor variable to None for proper handling in the finally block (if an exception occurs while creating/executing the cursor or if connection fails)
    try:
        connection = conn_pool.getconn()
        cursor = connection.cursor()
        response = client.models.generate_content(
            model="gemini-3.5-flash",
            contents=f"Perform the following task: {task} on the following text: {text}",  # Basic prompt to instruct the model's behavior
            config=types.GenerateContentConfig(
                thinking_config=types.ThinkingConfig(thinking_level=types.ThinkingLevel.LOW)),  # Low thinking level as it is an analysis task
        )
        cursor.execute("""
                    INSERT INTO jobs (user_id,input_text,task_type,result,status) 
                    VALUES ('test-user', %s, %s, %s, %s)
                    RETURNING id
                    """, 
                    (text, task, response.text, 'completed'))
        job_id = cursor.fetchone()[0] 
        connection.commit()        
        return {"result": f"{response.text}", "job_id": job_id}
    except Exception as e:
        return(f"Error: {e}")
    finally:
        if cursor:      # Ensure the cursor is closed after the operation
            cursor.close()
        if connection:      # Ensure the connection is returned to the pool after the operation
            conn_pool.putconn(connection)

