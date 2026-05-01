# Cloud DB Cutover Checklist

This project is already PostgreSQL-native. The safe cloud move is:

1. keep the current local PostgreSQL workflow running
2. provision a managed PostgreSQL target
3. run the same migrations there
4. export local base tables
5. import them into the cloud database
6. refresh materialized views
7. switch the app and pipelines by changing environment variables only

## Local-ready config

The runtime now supports two config modes:

- `DATABASE_URL`
- or discrete `.env` keys:
  - `DB_HOST`
  - `DB_PORT`
  - `DB_NAME`
  - `DB_USER`
  - `DB_PASSWORD`
  - `DB_SSLMODE`

`DATABASE_URL` wins when both exist. That keeps local development unchanged and makes cloud cutover a config-only step.

## One-day cutover plan

### 1. Morning: provision the target database

- Create a managed PostgreSQL instance.
- Copy the cloud connection string.
- If the provider requires TLS, set `DB_SSLMODE=require` or append `sslmode=require` to `DATABASE_URL`.

### 2. Export the current local data

Run:

```powershell
python -m scripts.export_db_bundle --output-dir exports/cutover_bundle
```

This exports the base tables as CSV files plus a `metadata.json` manifest.

### 3. Point the migration runner to the cloud database

For a one-off cutover, prefer a temporary shell override instead of editing the local `.env`:

```powershell
$env:DATABASE_URL="postgresql://user:password@host:5432/dbname?sslmode=require"
python -m scripts.run_migrations
```

### 4. Import the exported data into cloud PostgreSQL

```powershell
python -m scripts.import_db_bundle --bundle-dir exports/cutover_bundle
```

The import script:

- loads base tables in dependency order
- resets serial sequences
- refreshes materialized views

### 5. Smoke-test the cloud target

Recommended checks:

- run one A101 category pipeline
- run one Migros category pipeline
- open Streamlit
- validate key searches like:
  - `domates`
  - `salatalık`
  - `mantar`
  - `ekmek`

### 6. Flip application runtime to cloud

Use one of these:

- set `DATABASE_URL` in the deployment environment
- or update `.env` on the host running Streamlit/pipelines
- or set Streamlit `DATABASE_URL` secret

### 7. Keep rollback simple

- keep the local database untouched until cloud smoke tests pass
- keep the export bundle
- if cloud fails, restore the old `DATABASE_URL` / `.env` and restart the app

## Migration checklist

- [ ] Managed PostgreSQL provisioned
- [ ] Firewall / allowlist ready
- [ ] Cloud `DATABASE_URL` copied
- [ ] `sslmode` confirmed
- [ ] Local export bundle created
- [ ] Cloud migrations applied
- [ ] Bundle imported
- [ ] Materialized views refreshed
- [ ] `domates`, `salatalık`, `mantar`, `ekmek` smoke-tested
- [ ] Pipelines tested against cloud
- [ ] Streamlit app tested against cloud
- [ ] Rollback path documented
