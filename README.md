# BGB Portal Automation

Bulk meter-data extraction from the British Gas Business portal. Upload a spreadsheet of MPANs and MPRNs, and the app drives a pool of headless Chromium browsers to look each one up, streams progress to a live dashboard, and returns a standardised Excel workbook plus per-fuel CSV exports.

Built with FastAPI, Playwright and pandas. Single-page dashboard, no build step.

---

## Table of contents

- [What it does](#what-it-does)
- [Repository layout](#repository-layout)
- [Quick start (local)](#quick-start-local)
- [Configuration](#configuration)
- [How a job runs](#how-a-job-runs)
- [API reference](#api-reference)
- [WebSocket protocol](#websocket-protocol)
- [Data model](#data-model)
- [Output files](#output-files)
- [Deployment](#deployment)
- [Known issues and technical debt](#known-issues-and-technical-debt)
- [Security notes](#security-notes)

---

## What it does

1. **Upload** one or more `.xlsx` / `.xls` files containing meter numbers.
2. The app **scans every sheet** in every file, reports how many meters it found in each, and lets you tick which sheets to process.
3. On start, it spins up a **pool of concurrent Playwright browser workers**, logs each into the BGB portal with your stored portal credentials, and looks up meters in parallel.
4. Results **stream to the dashboard live** over a WebSocket — per-meter success/failure lines, running counters, success rate, average time per meter and an ETA.
5. Rows are classified as **Electricity**, **Gas** or **Unfound** and written incrementally to disk, so a crashed or stopped job can be **resumed** from its last checkpoint.
6. When the job finishes you download a **4-sheet Excel workbook** or individual **CSV exports**.

The unit of work is a single meter number. A job is a batch of them.

---

## Repository layout

```
├── app_async.py           # THE APPLICATION — FastAPI + Playwright, ~5000 lines
├── templates/
│   └── dashboard.html     # THE ENTIRE UI — self-contained HTML/CSS/JS
├── requirements.txt       # Python dependencies
├── runtime.txt            # python-3.11.9
├── Procfile               # Heroku/Railway-style start command
├── analytics.db           # SQLite results store (created on first run)
│
├── uploads/               # Merged input spreadsheets, per job         (generated)
├── outputs/               # Result workbooks, per job                  (generated)
├── state/                 # Resume checkpoints, per job                (generated)
├── job_meta/              # Job metadata snapshots                     (generated)
├── credentials.json       # Portal login, written by the dashboard     (generated)
│
├── app.py                 # DEAD — CLI user-admin tool for an old Flask build
├── electralink.py         # DEAD — standalone Streamlit app for the Electralink EAC API
├── static/app.js          # DEAD — 0 bytes, never served
├── templates/login.html   # DEAD — orphaned from the old Flask build
├── templates/index.html   # DEAD — 0 bytes
└── templates/job.html     # DEAD — 0 bytes
```

**The running service is `app_async.py` + `templates/dashboard.html`. Nothing else is imported or served.** See [Known issues](#known-issues-and-technical-debt) before deleting the dead files — `electralink.py` contains live API credentials that need rotating first.

---

## Quick start (local)

Requires Python 3.11.

```bash
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

Create a `.env` in the project root:

```ini
PORTAL_USER=your.email@example.com
PORTAL_PASS=your-portal-password
BROWSER_HEADLESS=false            # false shows the browser windows — useful locally
CONCURRENT_WORKERS=10
```

Run it:

```bash
python -m uvicorn app_async:app --reload --port 8000
```

Open <http://localhost:8000>.

`BROWSER_HEADLESS=false` is the local default and opens visible Chromium windows, which is the fastest way to debug a selector that stopped matching. **On a server it must be `true`** — there is no display, and the app will fail to launch a browser otherwise.

---

## Configuration

All settings are environment variables, read at import time in `app_async.py` and loaded from `.env` via `python-dotenv`.

| Variable | Default | What it controls |
|---|---|---|
| `PORTAL_USER` | — | BGB portal username. Required; `/start` returns 400 without it. |
| `PORTAL_PASS` | — | BGB portal password. Required. |
| `BROWSER_HEADLESS` | `false` | Run Chromium headless. **Must be `true` on a server.** |
| `CONCURRENT_WORKERS` | `10` | Browser workers in the pool. The main memory driver. |
| `METER_RETRIES` | `2` | Retry attempts per individual meter. |
| `BACKOFF_BASE` | `0.5` | Base seconds for exponential retry backoff. |
| `INCREMENTAL_SAVE_EVERY` | `25` | Write results to disk every N meters. |
| `MAX_JOB_RETRIES` | `1` | Whole-job retry attempts on unrecoverable failure. |
| `RETRY_DELAY_SECONDS` | `10` | Delay before a job-level retry. |
| `STUCK_TIMEOUT_SECONDS` | `15` | Seconds before a worker is treated as stuck. |
| `PAGE_LOAD_TIMEOUT` | `1200` | Playwright page-load timeout (ms). |
| `HEARTBEAT_TIMEOUT` | `20` | Seconds without a heartbeat before a worker is recycled. |
| `HEARTBEAT_INTERVAL` | `15` | Seconds between worker heartbeats. |
| `CONNECTION_RETRY_ATTEMPTS` | `2` | Browser reconnection attempts after a crash. |

**Credential precedence:** `credentials.json` (written by the dashboard's credentials panel) overrides the `.env` values. If logins are failing with credentials that look correct in `.env`, check for a stale `credentials.json` first — `load_credentials()` reads it before falling back to the environment.

### Sizing

`CONCURRENT_WORKERS` is the setting that matters. Each worker is a full Chromium context.

| RAM | Sensible value |
|---|---|
| 2 GB | 3 (with swap; expect slow runs) |
| 4 GB | 5 |
| 8 GB | 8–10 |

---

## How a job runs

```
POST /start
  │
  ├─ Merge all uploaded files (selected sheets only) → uploads/{job_id}_merged_input.xlsx
  ├─ Extract meter numbers from every sheet
  ├─ Create job record in the in-memory `jobs` dict, return {job_id, total_meters}
  │
  └─ Background task: run_job_async_with_retry()
        │
        ├─ WorkerPool spins up N WorkerState instances
        │     each = playwright → browser → context → page → portal login
        │
        ├─ asyncio.Semaphore(concurrency) gates process_meter_with_worker()
        │     │
        │     ├─ Navigate, fill the search form, scrape the result
        │     ├─ Classify as Electricity / Gas / Unfound
        │     ├─ Broadcast a `progress` message over the WebSocket
        │     ├─ Write to the `results` table in analytics.db
        │     └─ Every INCREMENTAL_SAVE_EVERY meters: checkpoint to state/ and rewrite the workbook
        │
        ├─ heartbeat_monitor() per worker — recycles a browser that stops responding
        │
        └─ On completion: write outputs/{job_id}_output.xlsx, broadcast `done`
```

**Form field matching** is done through `SELECTOR_MAP`, a dictionary of field names to ordered lists of CSS selector candidates — class, ID, name, placeholder and attribute-substring variants are tried in turn. This is the layer that breaks when the portal changes its markup, and the place to add a new selector when it does.

**Unfound classification** is deliberate, not an error path. A meter is Unfound if the scrape returned a known phrase (`not with british gas`, `no site details`, `meter not registered`, `future contract already agreed`, and similar) or if every critical address field came back empty. Unfound rows are exported alongside successes — only hard `failed` rows are excluded from the workbook.

### Resume

Every job writes `state/{job_id}_state.json` and `state/{job_id}_checkpoint.json`, managed by the `JobState` class. If a job is stopped, crashes or the process restarts:

- `GET /check_resume/{job_id}` reports `can_resume` along with processed/total counts.
- `POST /resume/{job_id}` reloads the checkpoint, skips already-processed meters and restarts the worker pool on what's left.

The dashboard persists the job ID in `localStorage`, so reloading the page mid-job reconnects the WebSocket automatically, and after a stop it re-offers the Resume panel.

---

## API reference

### Credentials

| Method | Path | Notes |
|---|---|---|
| `GET` | `/credentials` | Returns whether credentials are configured and the username. Never returns the password. |
| `POST` | `/credentials` | JSON `{username, password}` → writes `credentials.json` and updates the in-process globals. |
| `DELETE` | `/credentials` | Removes `credentials.json`. Implemented, but not wired to any button in the UI. |

### Jobs

| Method | Path | Notes |
|---|---|---|
| `GET` | `/` | Serves `templates/dashboard.html` raw. No Jinja rendering. |
| `POST` | `/preview_sheets` | Multipart `files`. Returns every sheet in every file with row count and detected meter count. |
| `POST` | `/start` | Multipart `files`, optional `selected_sheets` (comma-joined) and `skip_meters`. Returns `{job_id, total_meters}`. |
| `POST` | `/stop/{job_id}` | Graceful cancel. Saves state; response indicates whether the job can be resumed. |
| `GET` | `/check_resume/{job_id}` | Resume eligibility and progress counts. |
| `POST` | `/resume/{job_id}` | Restarts from the last checkpoint. |
| `POST` | `/append/{job_id}` | Multipart `file`. Adds meters to an existing job. Implemented, not used by the UI. |
| `POST` | `/delete/{job_id}` | Removes a job and its artefacts. Implemented, not used by the UI. |

### Results

| Method | Path | Returns |
|---|---|---|
| `GET` | `/stats/{job_id}` | Counters, success rate, timing. |
| `GET` | `/preview/{job_id}/combined` | All rows as JSON. |
| `GET` | `/preview/{job_id}/electricity` | Electricity rows. |
| `GET` | `/preview/{job_id}/gas` | Gas rows. |
| `GET` | `/preview/{job_id}/unfound` | Unfound rows. |
| `GET` | `/download/{job_id}` | The full 4-sheet `.xlsx`. |
| `GET` | `/download/{job_id}/electricity` | Electricity CSV. |
| `GET` | `/download/{job_id}/gas` | Gas CSV. |
| `GET` | `/download/{job_id}/unfound` | Unfound CSV. |

### WebSocket

`WS /ws/{job_id}` — receive-only. The client never sends.

---

## WebSocket protocol

Messages are JSON, shaped `{type, msg, progress, row, total_time, meta}`.

| `type` | Carries |
|---|---|
| `status` | Human-readable stage text ("Filling login credentials…"). |
| `progress` | A completed meter: `row` (the scraped record) and updated `meta`. |
| `done` | Job finished. Includes `success_rate_percent` and `can_resume`. |
| `error` | Fatal or per-meter error text. |
| `warning` | Non-fatal issue. |
| `success` | Milestone confirmation. |
| `info` | General notice. |

The `meta` object drives every live counter on the dashboard:

```
total  processed  success  failed  unfound  remaining
attempt  skipped  active_workers  avg_time_seconds  eta_seconds
```

Note that **row classification into the Electricity / Gas / Unfound tabs happens client-side**, in `isElectricityRow()`, `isGasRow()` and `isUnfoundRow()` in `dashboard.html`. They prefer the server's `row.kind`, then fall back to which consumption fields are populated, then to meter-number length (13 digits ⇒ electricity, 6–10 ⇒ gas). If rows land on the wrong tab, that logic is where to look — not the backend.

---

## Data model

`analytics.db`, SQLite, created by `init_db()` at import.

```sql
CREATE TABLE results (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       TEXT,
    meter        TEXT,
    kind         TEXT,      -- 'Electricity' | 'Gas'
    postcode     TEXT,
    consumption  TEXT,
    start_date   TEXT,
    end_date     TEXT,
    raw_json     TEXT,      -- full scraped record
    status       TEXT,      -- 'success' | 'unfound' | 'failed'
    timestamp    DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE job_attempts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id           TEXT,
    attempt_number   INTEGER,
    status           TEXT,
    error_message    TEXT,
    meters_processed INTEGER,
    timestamp        DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

There is also a `users` table, left over from the old Flask build. Nothing in `app_async.py` reads it.

Live job state lives in the in-memory `jobs` dict and is **not** in SQLite — it is mirrored to `job_meta/` and `state/` so it survives a restart.

---

## Output files

`outputs/{job_id}_output.xlsx` has four sheets: **Combined**, **Electricity**, **Gas**, **Unfound**.

All sheets use the same 12 standardised columns (Unfound adds a 13th, `Status`):

| # | Column | Notes |
|---|---|---|
| 1 | Site Building Name | |
| 2 | Site Street No | |
| 3 | Site Street 1 | |
| 4 | Site Street 2 | |
| 5 | Site Town | |
| 6 | Site Postcode | |
| 7 | MPAN Topline | Electricity only; blank for gas |
| 8 | Meter Number | The input meter, always preserved verbatim |
| 9 | Usage | Electricity or Gas consumption (kWh), by fuel type |
| 10 | Proposed Start Date | |
| 11 | Energization Status | Electricity |
| 12 | Annualised AQ | Gas; falls back to Transportation AQ |

Rows with `status` of `success` or `unfound` are exported. `failed` rows are excluded from the workbook but remain in the `results` table for diagnosis.

---

## Deployment

The app is deployed to a DigitalOcean droplet by `setup.sh`, which installs the service user, virtualenv, Chromium and its shared libraries, a systemd unit, an nginx reverse proxy and ufw rules.

```bash
scp electralink-deploy.tar.gz root@YOUR_DROPLET_IP:/root/
ssh root@YOUR_DROPLET_IP
tar -xzf /root/electralink-deploy.tar.gz -C /root
cd /root/electralink && bash setup.sh
```

The installer is idempotent — re-run it after a code change and it re-syncs the code and dependencies without touching `.env`, `credentials.json`, `analytics.db` or the job directories.

What it configures that matters:

- **`BROWSER_HEADLESS=true`** in the generated `.env`.
- **`CONCURRENT_WORKERS`** sized from the droplet's RAM.
- **4GB swap**, because concurrent Chromium contexts spike hard.
- **nginx** with WebSocket upgrade headers, `client_max_body_size 512M` for spreadsheet uploads, and 24-hour proxy timeouts so long jobs aren't cut off.
- **HTTP basic auth**, because the app itself has none (see below).

Day-to-day:

```bash
journalctl -u electralink -f          # live logs
systemctl restart electralink         # restart
htpasswd /etc/nginx/.htpasswd admin   # change the web password
```

A fuller runbook, including troubleshooting, lives in `ElectraLink-DigitalOcean-Deployment-Runbook.pdf`.

---

## Known issues and technical debt

**The app has no authentication.** `app_async.py` serves the dashboard to anyone who can reach it — there is no login route, no session handling, no user check. `templates/login.html` and the `users` table are orphans from a previous Flask build. Anything exposed to the internet must sit behind a reverse-proxy auth layer; the deployment does this with nginx basic auth. Don't remove it without adding real auth in the app.

**Dead files.** `app.py`, `electralink.py`, `static/app.js`, `templates/login.html`, `templates/index.html` and `templates/job.html` are all unreferenced. `static/app.js` is 0 bytes and `StaticFiles` is imported but never mounted.

**`analytics.db` is 79MB and in the archive.** It should not be in version control. Add it to `.gitignore` along with `uploads/`, `outputs/`, `state/`, `job_meta/`, `credentials.json` and `.env`.

**Selector fragility.** `SELECTOR_MAP` is a best-effort list of CSS selector candidates per form field. A portal redesign breaks lookups silently — meters start coming back Unfound rather than erroring. If Unfound rates jump suddenly, check the selectors before assuming the data is wrong.

**Client-side classification.** Electricity/Gas/Unfound tab assignment is recomputed in JavaScript using meter-digit heuristics, duplicating logic the backend already has. The two can disagree.

**Unused endpoints.** `/append/{job_id}`, `/delete/{job_id}` and `DELETE /credentials` are implemented and reachable but have no UI. They work; they're just undiscoverable.

**Vestigial code in `/start`.** `HARDCODED_SKIP_METERS` / `ENABLE_HARDCODED_SKIP` are debug scaffolding left in the request handler, currently disabled.

**`app_async.py` is ~5000 lines in one file.** Config, selectors, browser management, the worker pool, scraping, export and all routes share a module. Any substantial change should start by splitting it.

---

## Security notes

Three items need action, in order of urgency:

1. **`electralink.py` contains a hardcoded Electralink EAC API key and password in plaintext** (near the top of the file). Treat them as compromised and **rotate them** — the file has been zipped and moved around. Delete the file afterwards; nothing imports it.

2. **`app.py` hardcodes default user accounts with weak passwords** in `reset_default_users()`, hashed with unsalted SHA-256. The table it writes to is unused by the running app, but the script is still runnable against `analytics.db`. Delete it.

3. **`credentials.json` stores the portal password in plaintext** on disk. On the droplet it is owned by the service user in `/opt/electralink/`; keep it at `600` and never commit it.

Also worth doing: put the droplet behind a domain with TLS (`certbot --nginx -d your.domain`) so the basic-auth password and portal credentials aren't crossing the wire in the clear.