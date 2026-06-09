# LLM Inference Service

A multi-tenant document summarization service built on FastAPI, Redis, and PostgreSQL.

## Key design decisions

**Why async FastAPI over Django:**
- FastAPI is async-native, whereas Django was primarily built with synchronicity in mind. Even though Django has added async support over time, it uses an ASGI handler wrapper which adds context switching overhead compared to FastAPI's native async implementation.