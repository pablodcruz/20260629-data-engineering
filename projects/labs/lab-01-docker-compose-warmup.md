# Lab 1 - Docker Compose Warmup

## Objective

Understand how Docker Compose runs multiple services together and how to inspect a local containerized environment.

## Scenario

StreamFlow will eventually run several services at the same time: Kafka, Spark, Airflow, and supporting containers.
Before using those tools together, this lab gives you a small two-service Compose stack to practice the basic workflow.

## What You Will Build

You will run:

* A `web` container serving a tiny HTML page.
* A `worker` container printing a heartbeat message every few seconds.

This is intentionally simple.
The goal is to learn the container commands before the stack becomes more complex.

## Prerequisites

* Docker Desktop or Docker Engine is running.
* Docker Compose is available through the `docker compose` command.

Check your setup:

```bash
docker --version
docker compose version
```

## Suggested Folder

From your lab workspace:

```bash
mkdir -p lab-01-docker/site
cd lab-01-docker
touch docker-compose.yml site/index.html
```

## Starter Files

Create `docker-compose.yml`:

```yaml
services:
  web:
    image: python:3.11-slim
    container_name: streamflow_lab01_web
    working_dir: /app
    command: python -m http.server 8000
    ports:
      - "8000:8000"
    volumes:
      - ./site:/app

  worker:
    image: python:3.11-slim
    container_name: streamflow_lab01_worker
    command: >
      python -c "import time;
      i = 1;
      print('worker started', flush=True);
      [print(f'heartbeat {n}', flush=True) or time.sleep(5) for n in range(1, 100)]"
```

Create `site/index.html`:

```html
<!doctype html>
<html>
  <head>
    <title>StreamFlow Lab 1</title>
  </head>
  <body>
    <h1>StreamFlow Docker Compose Warmup</h1>
    <p>If you can read this in the browser, the web container is running.</p>
  </body>
</html>
```

## Steps

1. Start the stack.

```bash
docker compose up -d
```

2. List the running containers.

```bash
docker compose ps
```

3. View logs for all services.

```bash
docker compose logs
```

4. View logs for one service.

```bash
docker compose logs worker
```

5. Test the web service.

```bash
curl http://localhost:8000
```

You can also open `http://localhost:8000` in a browser.

6. Restart one service.

```bash
docker compose restart worker
docker compose logs worker
```

7. Stop the stack.

```bash
docker compose down
```

## Checkpoints

You are done when:

* `docker compose ps` shows both services while the stack is running.
* `docker compose logs worker` shows heartbeat messages.
* `http://localhost:8000` returns the HTML page.
* `docker compose down` removes the running containers.

## Deliverables

Submit:

* Your `docker-compose.yml`.
* A screenshot or copied terminal output from `docker compose ps`.
* One paragraph explaining the difference between the `web` service and the `worker` service.
* One Docker command you found useful and what it showed you.

## Common Issues

| Problem | Likely Cause | Fix |
| ------- | ------------ | --- |
| `docker` command not found | Docker is not installed or not on PATH | Start Docker Desktop or reinstall Docker |
| Port `8000` is already allocated | Another process is using port 8000 | Change `"8000:8000"` to `"8001:8000"` and use `http://localhost:8001` |
| Container exits immediately | YAML indentation or command issue | Run `docker compose logs` and inspect the error |
| Browser cannot connect | Stack is not running or wrong port | Run `docker compose ps` and confirm the port mapping |
