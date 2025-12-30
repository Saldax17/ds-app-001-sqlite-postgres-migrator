# Infrastructure — SQLite → PostgreSQL Migrator

This folder contains the local and cloud deployment infrastructure.

---

## 🚀 Local Environment (Docker Compose)

Start all services:

```bash
docker-compose up -d
```

Stop everything:

```bash
/docker-compose down
```

Restart:

```bash
docker-compose down -v
docker-compose up --build
```

🧱 Services

```bash
| Service  | Port | Description                              |
| -------- | ---- | ---------------------------------------- |
| Postgres | 5432 | Target DB used for loading migrated data |
| API      | 8000 | REST service to trigger migrations       |
```
