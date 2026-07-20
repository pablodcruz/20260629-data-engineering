# Lab 1 Solution - Docker Compose Warmup

Original lab: [`../../labs/lab-01-docker-compose-warmup.md`](../../labs/lab-01-docker-compose-warmup.md)

## Files

- `docker-compose.yml` defines the `web` and `worker` services.
- `site/index.html` is the page served by the `web` container.

The site volume is mounted read-only because the web service only needs to
serve the file; it does not need to modify it.

## Run and Verify

From this directory, run:

```bash
docker compose config --quiet
docker compose up -d
docker compose ps
docker compose logs worker
curl http://localhost:8000
```

Expected results:

- `docker compose ps` lists both `streamflow_lab01_web` and
  `streamflow_lab01_worker` as running.
- The worker log contains `worker started` followed by numbered heartbeat
  messages.
- The HTTP response contains the heading `StreamFlow Docker Compose Warmup`.

To test a single-service restart:

```bash
docker compose restart worker
docker compose logs --since 1m worker
```

The recent log should show the worker starting again and restarting its
heartbeat count.

## Deliverable Responses

The `web` service runs Python's built-in HTTP server and publishes container
port 8000 to port 8000 on the host. It also mounts the local `site` directory at
`/app`, allowing the container to serve `index.html`. The `worker` service has
no published port and does not accept requests. It runs a long-lived Python
command that writes a heartbeat to standard output every five seconds. Together
they demonstrate two common service roles: a network-facing process and a
background process.

`docker compose ps` is especially useful because it gives a quick view of the
services in the current Compose project, including their state and published
ports. It is usually the first command to run when a service is not reachable.

## Runtime Evidence

Run the commands above and save the real `docker compose ps` output or a
screenshot here if required. Runtime evidence is intentionally not prefilled.

## Cleanup

```bash
docker compose down
```
