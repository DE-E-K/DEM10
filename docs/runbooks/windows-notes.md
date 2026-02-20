# Windows Notes

- Prefer a non-OneDrive path for active Docker volumes to avoid sync/lock contention.
- If Docker Desktop is unstable under load, increase memory/CPU in Docker settings.
- Use UTC everywhere to avoid timestamp drift in queries and alerts.
- If ports are busy, remap in docker-compose and update `.env` accordingly.
