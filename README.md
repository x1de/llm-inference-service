# LLM Inference Service

A multi-tenant document summarization service built on FastAPI, Redis, and PostgreSQL.

## Key design decisions

**Why async FastAPI over Django:**
- FastAPI is async-native, whereas Django was primarily built with synchronicity in mind. Even though Django has added async support over time, it uses an ASGI handler wrapper which adds context switching overhead compared to FastAPI's native async implementation.
- It has inbuilt OpenAPI. OpenAPI is used to describe how the application's REST API behaves. This includes listing existing endpoints, expected data format (headers, query params, and payload), status codes, etc. All of this is described in either JSON or YAML.
- It also has out of the box SwaggerUI. SwaggerUI is used to generate a clean and interactive web page for the OpenAPI schema. It executes live API calls and lets you try endpoints directly through the webpage.

**Use of Pydantic**
- Pydantic is used for input data validation & parsing. It enforces static type checking at runtime and can also be used to add specific constraints to input data so bad inputs are rejected at runtime. It also has automated graceful error handling.

**Current Flow**
Incoming Request -> Calls Gemini -> Saves result to DB -> Returns Result

**Flow with Redis**

Incoming Request -> Saves req to DB with status = pending -> Saves job_id in Redis Job Queue -> Returns job_id to Client

Background: Arq Worker monitors Redis -> Fetches job from Redis -> Calls Gemini -> Saves result to DB with status=completed

**Justification for Rate-limit Algorithm**
- I used the Token Bucket algorithm over alternatives like sliding window because it's less memory-expensive. Sliding window requires storing exact timestamps for every request per user, whereas token bucket just stores two numbers (current tokens and last refill time) in a Redis hash. 
- It also handles burst traffic naturally. If a user sends 5 requests at once, they're allowed up to bucket capacity before getting throttled, instead of being immediately rejected. This is important for a ticket processing service where support teams might submit a batch of tickets simultaneously. 
- Widely used in production by Stripe, Gemini, and most major APIs.