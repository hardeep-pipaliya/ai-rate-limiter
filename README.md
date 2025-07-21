# AI Rate Limiter API

A Flask-based microservice for managing workspaces, provider keys, worker scaling, and message processing via RabbitMQ. This README outlines project structure, installation steps (including Docker), configuration details, and all available API endpoints with request/response examples.

---

## Table of Contents

- [Project Structure](#project-structure)
- [Installation](#installation)
- [Running with Docker](#running-with-docker)
- [Configuration](#configuration)
- [API Endpoints](#api-endpoints)

  - [Workspaces](#workspaces)
  - [Provider Keys](#provider-keys)
  - [Workers](#workers)
  - [Messages](#messages)

- [Usage Examples](#usage-examples)

---

## Project Structure

```
├── app/
│   ├── config/         # Configuration files (logger, settings)
│   ├── connection/     # Database and RabbitMQ connection modules
│   ├── models/         # SQLAlchemy ORM models
│   ├── providers/      # External provider integration
│   ├── routes/         # Flask blueprints and route definitions
│   ├── utils/          # Helper functions and utilities
│   └── workers/        # Background worker logic
├── migrations/         # Alembic database migrations
├── requirements.txt    # Python dependencies
├── Dockerfile          # Container build instructions
└── README.md           # This documentation file
```

---

## Installation

1. **Clone the repository**

   ```bash
   git clone https://github.com/your-org/ai-rate-limiter.git
   cd ai-rate-limiter
   ```

2. **Create and activate a virtual environment**

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Initialize the database**

   ```bash
   flask db upgrade
   ```

5. **Run the application locally**

   ```bash
   flask run
   ```

---

## Running with Docker

You can build and run the service inside a Docker container, either standalone or with Docker Compose.

1. **Build the image**

   ```bash
   docker build -t ai-rate-limiter .
   ```

2. **Run without Compose**

   ```bash
   docker run -d \
     --name ai-rate-limiter \        # container name
     -e FLASK_APP=app.py \           # Flask entry point
     -e FLASK_ENV=production \       # production mode
     -e SQLALCHEMY_DATABASE_URI=postgresql://<db_user>:<db_pass>@<db_host>:5432/<db_name> \  # Postgres URL
     -e RABBITMQ_URL=amqp://<user>:<pass>@<rabbitmq_host>:5672/ \                          # RabbitMQ URL
     -p 5000:5000 \                  # expose port 5000
     ai-rate-limiter
   ```

3. **Run with Docker Compose**

   If you have a `docker-compose.yml` that defines services for the app, Postgres, and RabbitMQ:

   ```bash
   docker-compose up -d
   ```

---

## API Endpoints

Base URL: `http://<host>:<port>`

### Workspaces

| Method | Path                     | Description                                              |
| ------ | ------------------------ | -------------------------------------------------------- |
| GET    | `/workspaces/`           | List all workspaces (optional filter by `?workspace_id`) |
| POST   | `/workspace/register`    | Register a new workspace and provider keys               |
| DELETE | `/workspace/delete/{id}` | Delete an existing workspace                             |

**Register Workspace**

```http
POST /workspace/register
Content-Type: application/json

{
  "workspace_id": "<uuid>",
  "providers": [
    {
      "name": "openai",
      "api_key": "sk-...",
      "rate_limit": 100,
      "rate_limit_period": 60,
      "config": { "model": "gpt-4", "api_version": "2023-05-15" }
    }
  ]
}
```

### Provider Keys

| Method | Path                            | Description                  |
| ------ | ------------------------------- | ---------------------------- |
| GET    | `/provider-keys/`               | List all provider keys       |
| POST   | `/provider-key/add`             | Add a new provider key       |
| DELETE | `/provider-key/delete/{key_id}` | Delete a provider key        |
| PUT    | `/provider-key/update/{key_id}` | Update provider key settings |

### Workers

| Method | Path                            | Description                      |
| ------ | ------------------------------- | -------------------------------- |
| POST   | `/workers/scale/{workspace_id}` | Scale number of worker processes |
| GET    | `/workers/{workspace_id}`       | List all workers for a workspace |

**Scale Workers**

```http
POST /workers/scale/<workspace_id>
Content-Type: application/json

{ "count": 1 }
```

### Messages

| Method | Path                    | Description                          |
| ------ | ----------------------- | ------------------------------------ |
| POST   | `/message/publish`      | Publish a new message for processing |
| GET    | `/message/{message_id}` | Retrieve message status and result   |

**Publish Message**

```http
POST /message/publish
Content-Type: application/json

{
  "prompt": "What is the capital of France?",
  "content": "I need to know about France's capital city.",
  "workspace_id": "<uuid>",
  "model": "gpt-4",
  "system_prompt": "You are a helpful assistant.",
  "sequence_index": 1,
  "html_tag": "p",
  "response_format": "json"
}
```

**Get Message**

```http
GET /message/<message_id>
```

---

## Usage Examples

1. **Register a workspace**

   ```bash
   curl -X POST http://localhost:5000/workspace/register \
     -H 'Content-Type: application/json' \
     -d '{"workspace_id":"abc-123","providers":[...]}'
   ```

2. **Publish a message**

   ```bash
   curl -X POST http://localhost:5000/message/publish \
     -H 'Content-Type: application/json' \
     -d '{"prompt":"Hello","content":"Test","workspace_id":"abc-123"}'
   ```

3. **Fetch message result**

   ```bash
   curl http://localhost:5000/message/<message_id>
   ```
