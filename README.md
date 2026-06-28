# LLM Inference Service

A multi-tenant document summarization service built on FastAPI, Redis, and PostgreSQL.

## Key design decisions

**Why async FastAPI over Django:**
- FastAPI is async-native, whereas Django was primarily built with synchronicity in mind. Even though Django has added async support over time, it uses an ASGI handler wrapper which adds context switching overhead compared to FastAPI's native async implementation.
- It has inbuilt OpenAPI. OpenAPI is used to describe how the application's REST API behaves. This includes listing existing endpoints, expected data format (headers, query params, and payload), status codes, etc. All of this is described in either JSON or YAML.
- It also has out of the box SwaggerUI. SwaggerUI is used to generate a clean and interactive web page for the OpenAPI schema. It executes live API calls and lets you try endpoints directly through the webpage.

**Use of Pydantic**
- Pydantic is used for input data validation & parsing. It enforces static type checking at runtime and can also be used to add specific constraints to input data so bad inputs are rejected at runtime. It also has automated graceful error handling.

