# Windows Notes

## General
- Prefer a non-OneDrive path for active Docker volumes to avoid sync/lock contention.
- If Docker Desktop is unstable under load, increase memory/CPU in Docker settings.
- Use UTC everywhere to avoid timestamp drift in queries and alerts.
- If ports are busy, remap in docker-compose and update `.env` accordingly.

## Flink
- The Flink Web UI is available at http://localhost:8081 after `docker compose up -d`.
- Submit PyFlink jobs via: `docker exec heartbeat-flink-jobmanager flink run -py /opt/flink/jobs/<job>.py`
- Monitor backpressure and checkpoint health in the Flink Web UI → Jobs → Running Jobs → Back Pressure tab.
- If Flink task slots are exhausted, scale by adding more TaskManager instances:
  `docker compose up --scale flink-taskmanager=3 -d`
- Flink checkpoint storage is in a named Docker volume (`flink_checkpoints`). Reset with `docker compose down -v`.
- Flink logs are accessible via `docker logs heartbeat-flink-jobmanager` / `heartbeat-flink-taskmanager`.

## Troubleshooting
- If `flink run` hangs, check that Kafka and PostgreSQL are healthy: `docker compose ps`.
- If Flink jobs fail immediately, check logs for JAR classpath issues — the custom Dockerfile
  downloads connector JARs into `/opt/flink/lib/`.
- PowerShell scripts may require execution policy: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.
