





import os
import re
import uuid
import asyncio
import logging
import sqlite3
import json
import time
import threading
from pathlib import Path
from typing import Dict, Any, List, Optional, Set
from datetime import datetime

from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime



import pandas as pd
from fastapi import FastAPI, Request, UploadFile, File, HTTPException, WebSocket, WebSocketDisconnect
from fastapi import BackgroundTasks
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeout


load_dotenv()


# Config
PORTAL_USER = os.getenv("PORTAL_USER")
PORTAL_PASS = os.getenv("PORTAL_PASS")
BROWSER_HEADLESS = os.getenv("BROWSER_HEADLESS", "false").lower() in ("1", "true", "yes")
CONCURRENT_WORKERS = int(os.getenv("CONCURRENT_WORKERS", "10"))  # 16 → 10: less parallel processing
METER_RETRIES = int(os.getenv("METER_RETRIES", "2"))  # 3 → 2: Fewer retry attempts
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "0.5"))  # 1 → 0.5: Faster retry backoff
INCREMENTAL_SAVE_EVERY = int(os.getenv("INCREMENTAL_SAVE_EVERY", "25"))  # 10 → 25: Less I/O overhead
MAX_JOB_RETRIES = int(os.getenv("MAX_JOB_RETRIES", "1"))  # 2 → 1: Fail faster on persistent errors
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "10"))  # 20 → 10: Faster retries
STUCK_TIMEOUT_SECONDS = int(os.getenv("STUCK_TIMEOUT_SECONDS", "15"))  # 20 → 15: Detect stuck jobs sooner
PAGE_LOAD_TIMEOUT = int(os.getenv("PAGE_LOAD_TIMEOUT", "1200"))  # 1200 → 800: Don't wait as long for slow pages
HEARTBEAT_TIMEOUT = int(os.getenv("HEARTBEAT_TIMEOUT", "20"))  # 30 → 20: Faster health detection
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "15"))  # 20 → 15: More frequent heartbeats
CONNECTION_RETRY_ATTEMPTS = int(os.getenv("CONNECTION_RETRY_ATTEMPTS", "2"))  # 3 → 2: Fewer connection retries


BASE_DIR = Path(__file__).parent.resolve()
UPLOAD_DIR = BASE_DIR / "uploads"; UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR = BASE_DIR / "outputs"; OUTPUT_DIR.mkdir(exist_ok=True)
STATE_DIR = BASE_DIR / "state"; STATE_DIR.mkdir(exist_ok=True)
TEMPLATE_DIR = BASE_DIR / "templates"
DB_PATH = BASE_DIR / "analytics.db"
JOB_META_DIR = BASE_DIR / "job_meta"; JOB_META_DIR.mkdir(parents=True, exist_ok=True)

CREDENTIALS_FILE = BASE_DIR / "credentials.json"



# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger("bgb_automator")


# FastAPI app
app = FastAPI()
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))


# Job store
jobs: Dict[str, Dict[str, Any]] = {}
ws_queues: Dict[str, List[WebSocket]] = {}
cancellation_locks = {} # Per-job locks for thread-safe cancellation
heartbeat_tasks = {} # Per-job heartbeat monitoring tasks
meta_locks: Dict[str, threading.Lock] = {}
heartbeat_tasks: Dict[str, asyncio.Task] = {}


# Selector map for form fields

SELECTOR_MAP = {
    "Electricity Consumption (kWh)": [
        "input.Electricity_Consumption_kWh__c",
        "input[id*='Electricity_Consumption_kWh']",
        "input[class*='Electricity_Consumption']",
        "input[name*='Electricity_Consumption']",
        "input[placeholder*='Electricity Consumption']",
        "input[type='text'][id*='elec' i][id*='consumption' i]",
        "input[type='text'][name*='elec' i][name*='consumption' i]"
    ],
    "Gas Consumption (kWh)": [
        "input.Gas_Consumption_kWh__c",
        "input[id*='GasConsumption']",
        "input[id*='Gas_Consumption']",
        "input[class*='Gas_Consumption']",
        "input[name*='Gas_Consumption']",
        "input[placeholder*='Gas Consumption']",
        "input[type='text'][id*='gas' i][id*='consumption' i]",
        "input[type='text'][id*='gas' i][id*='usage' i]"
    ],
    "MPAN Topline": [
        "input.MPAN_Topline__c",
        "input[id*='MPAN_Topline']",
        "input[class*='MPAN_Topline']",
        "input[name*='MPAN']",
        "input[placeholder*='MPAN']",
        "input[type='text'][id*='mpan' i]"
    ],
    "MPRN": [
        "input.MPR__c",
        "input[class*='MPR__c']",
        "input[id*='j_id787:j_id790']",
        "input[class*='MPRN']",
        "input[name*='MPRN']",
        "input[placeholder*='MPRN']",
        "input[type='text'][id*='mprn' i]"
    ],
    "Transportation AQ": [
        "input.Transportation_AQ__c",
        "input[id*='j_id0:mainform:gasBlock:j_id785:j_id786:26:j_id838:j_id840']",  # ← NEW: Specific disabled field
        "input[id*='j_id838:j_id840']",
        "input[id*='j_id0:mainform:gasBlock:j_id785:j_id786']",  # ← NEW: More general match
        "input[id*='Transportation_AQ']",
        "input[class*='Transportation_AQ']",
        "input[name*='Transportation_AQ']",
        "input[placeholder*='Transportation']",
        "input[placeholder*='AQ']",
        "input[id*='transportation' i]",
        "input[id*='annual' i][id*='quantity' i]",
        "input[type='text'][class*='aq' i]",
        "input[name*='aq' i]",
        "input[id*='aq' i]"
    ],
    "Proposed Start Date (Elec)": [
        "input.Proposed_Start_Date_Elec__c",
        "input[id*='Proposed_Start_Date_Elec']",
        "input[class*='Proposed_Start_Date_Elec']",
        "input[name*='Proposed_Start_Date_Elec']",
        "input[placeholder*='Start Date']",
        "input[type='text'][id*='elec' i][id*='date' i]"
    ],
    "Proposed Start Date (Gas)": [
        "input.Proposed_Start_Date_Gas__c",
        "input[id='j_id0:mainform:gasBlock:j_id785:j_id786:4:j_id804:j_id806']",
        "input[id*='j_id804:j_id806']",
        "input[id*='Proposed_Start_Date_Gas']",
        "input[class*='Proposed_Start_Date_Gas']",
        "input[name*='Proposed_Start_Date_Gas']",
        "input[placeholder*='Start Date']",
        "input[type='text'][id*='gas' i][id*='date' i]",
        "input[type='text'][id*='gas' i][id*='start' i]"
    ],
    "Energization Status": [
        "input.EnergizationStatus__c",
        "input[class*='EnergizationStatus__c']",
        "input[class*='EnergizationStatus']",
        "input[id*='EnergizationStatus']",
        "input[name*='EnergizationStatus']",
        "input[id*='j_id420']",
        "input[placeholder*='Energization']",
        "input[aria-label*='Energization']",
        "input[type='text'][id*='energiz' i]",
        "input[type='text'][name*='energiz' i]",
        "input[type='text'][class*='energiz' i]",
        "input[type='text']"  # Fallback: look near labels
    ],
    "Site Building Name": [
        "input.Site_Address_Name__c",
        "input[id='j_id0:mainform:gasBlock:j_id846:j_id847:0:inputSample4']",
        "input[id*='SiteDetailId'][id*='inputSample4']",
        "input[class*='Site_Address_Name']",
        "input[name*='Site_Address_Name']",
        "input[placeholder*='Building Name']"
    ],
    "Site Street No": [
        "input.Site_Address_No__c",
        "input[id='j_id0:mainform:gasBlock:j_id846:j_id847:2:inputSample4']",
        "input[id*='inputSample4']",
        "input[class*='Site_Address_No']",
        "input[name*='Site_Street_No']",
        "input[placeholder*='Street No']"
    ],
    "Site Street 1": [
        "input.Site_Street_1__c",
        "input[id='j_id0:mainform:gasBlock:j_id846:j_id847:4:j_id848:street1']",
        "input[id*='street1']",
        "input[class*='Site_Street_1']",
        "input[name*='Site_Street_1']",
        "input[placeholder*='Street 1']"
    ],
    "Site Street 2": [
        "input.Site_Street_2__c",
        "input[id='j_id0:mainform:gasBlock:j_id846:j_id847:6:inputSample4']",
        "input[class*='Site_Street_2']",
        "input[id*='inputSample4']",
        "input[name*='Site_Street_2']",
        "input[placeholder*='Street 2']"
    ],
    "Site Town": [
        "input.Site_Town__c",
        "input[id='j_id0:mainform:gasBlock:j_id846:j_id847:3:j_id852:town']",
        "input[id*='town']",
        "input[class*='Site_Town']",
        "input[name*='Site_Town']",
        "input[placeholder*='Town']"
    ],
    "Site Postcode": [
        "input.Site_Postcode__c",
        "input[id='j_id0:mainform:gasBlock:j_id846:j_id847:7:j_id860:j_id863']",
        "input[id*='Site_Postcode']",
        "input[class*='Site_Postcode']",
        "input[id*='postcode']",
        "input[id*='j_id860:j_id863']",
        "input[name*='Postcode']",
        "input[placeholder*='Postcode']"
    ]
}



def load_credentials():
    """Load credentials from file (overrides .env)"""
    if CREDENTIALS_FILE.exists():
        try:
            with open(CREDENTIALS_FILE, 'r') as f:
                creds = json.load(f)
                return creds.get('user'), creds.get('password')
        except Exception:
            pass
    return os.getenv("PORTAL_USER"), os.getenv("PORTAL_PASS")

def get_portal_credentials():
    user, password = load_credentials()
    return user, password


def _meters_list_path(job_id: str) -> Path:
    """Path to the stored meters list file"""
    return STATE_DIR / f"{job_id}_meters.json"


def save_meters_list(job_id: str, meters: List[str]):
    """Save meters list to a separate file for resume capability"""
    try:
        meters_path = _meters_list_path(job_id)
        with open(meters_path, 'w', encoding='utf-8') as f:
            json.dump({
                "meters": meters,
                "count": len(meters),
                "timestamp": datetime.now().isoformat()
            }, f, indent=2)
        LOG.info(f"[{job_id}] Saved {len(meters)} meters to {meters_path}")
    except Exception as e:
        LOG.error(f"[{job_id}] Failed to save meters list: {e}")


def load_meters_list(job_id: str) -> Optional[List[str]]:
    """Load meters list from file"""
    try:
        meters_path = _meters_list_path(job_id)
        if meters_path.exists():
            with open(meters_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                meters = data.get("meters", [])
                LOG.info(f"[{job_id}] Loaded {len(meters)} meters from {meters_path}")
                return meters
    except Exception as e:
        LOG.error(f"[{job_id}] Failed to load meters list: {e}")
    return None


def _get_meta_lock(job_id: str):
    if job_id not in meta_locks:
        meta_locks[job_id] = threading.Lock()
    return meta_locks[job_id]


def _meta_path(job_id: str) -> Path:
    return JOB_META_DIR / f"{job_id}.json"


def json_serializer(obj):
    """Custom JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    return str(obj)


async def save_job_meta(job_id: str):
    """Async version - serialize job meta to disk safely"""
    if job_id not in jobs:
        return
    
    meta = jobs[job_id].copy()
    
    # Remove ALL unserializable fields INCLUDING meters_list
    unsafe_keys = {
        "task", "context", "browser", "ws_conn", "ws_list", 
        "playwright", "page", "session", "meters_list"  # ← ADDED meters_list removal
    }
    for k in unsafe_keys:
        meta.pop(k, None)
    
    # Ensure only primitives remain
    try:
        await asyncio.to_thread(_write_meta_file, job_id, meta)
    except Exception as e:
        LOG.warning(f"Failed saving job meta for {job_id}: {e}")
        
        
def _write_meta_file(job_id: str, meta: dict):
    """Synchronous file write (called via to_thread)"""
    path = _meta_path(job_id)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(meta, f, default=json_serializer, indent=2)        


def load_persisted_jobs():
    """Load all JSON meta files into the in-memory jobs dict at startup."""
    for p in JOB_META_DIR.glob("*.json"):
        try:
            with open(p, "r", encoding="utf-8") as f:
                meta = json.load(f)
            job_id = p.stem
            jobs.setdefault(job_id, {})
            jobs[job_id].update(meta)
            # loaded jobs are not running by default
            jobs[job_id].setdefault("status", "stopped")
            jobs[job_id].pop("cancel_requested", None)
            jobs[job_id].pop("is_stopping", None)
        except Exception as e:
            LOG.warning(f"Failed to load job meta {p}: {e}")


def delete_persisted_job(job_id: str):
    try:
        p = _meta_path(job_id)
        if p.exists():
            p.unlink()
    except Exception as e:
        LOG.warning(f"Failed to delete meta for {job_id}: {e}")


async def update_job_status(job_id: str, **kwargs):
    """Update job and persist"""
    if job_id not in jobs:
        jobs[job_id] = {}
    jobs[job_id].update(kwargs)
    await save_job_meta(job_id)

# Load persisted jobs at startup
load_persisted_jobs()


# >>> Add these new globals & helpers <<<
page_locks: Dict[str, asyncio.Lock] = {}  # per-job async locks (not persisted)

def _get_page_lock(job_id: str) -> asyncio.Lock:
    """Return a per-job asyncio.Lock (create if missing). Not persisted."""
    lock = page_locks.get(job_id)
    if lock is None:
        lock = asyncio.Lock()
        page_locks[job_id] = lock
    return lock

async def _save_session_to_meta(job_id: str, context, page):
    """Capture ONLY serializable session data"""
    try:
        cookies = []
        if context:
            cookies = await context.cookies()
            # Clean cookies - remove expires timestamps
            cookies = [
                {k: v for k, v in c.items() if k != "expires"}
                for c in cookies
            ]
        
        local_storage = {}
        if page and not page.is_closed():
            try:
                raw = await page.evaluate(
                    "() => JSON.stringify(Object.fromEntries(Object.entries(window.localStorage)))"
                )
                local_storage = json.loads(raw) if raw else {}
            except Exception as e:
                LOG.debug(f"[{job_id}] localStorage read failed: {e}")
        
        # Store ONLY in memory (don't persist to disk)
        jobs.setdefault(job_id, {})
        jobs[job_id]["session"] = {
            "cookies": cookies,
            "localStorage": local_storage
        }
        
    except Exception as e:
        LOG.debug(f"[{job_id}] _save_session_to_meta error: {e}")
        
        
        
        
async def _restore_session_to_context(job_id: str, context, page):
    try:
        sess = jobs.get(job_id, {}).get("session", {})
        cookies = sess.get("cookies", [])
        local_storage = sess.get("localStorage", {})
                
        if cookies and context:
            try:
                # Create clean copy
                clean_cookies = [
                    {k: v for k, v in c.items() if k != "expires"}
                    for c in cookies
                ]
                await context.add_cookies(clean_cookies)        

            except Exception as e:
                LOG.debug(f"[{job_id}] Failed to restore cookies: {e}")

        if local_storage and page:
            try:
                js = f"""
                () => {{
                    const data = {json.dumps(local_storage)};
                    for (const k in data) {{
                        try {{ window.localStorage.setItem(k, data[k]); }} catch (e) {{ }}
                    }}
                    return true;
                }}
                """
                await page.evaluate(js)
            except Exception as e:
                LOG.debug(f"[{job_id}] Failed to restore localStorage: {e}")
    except Exception as e:
        LOG.debug(f"[{job_id}] _restore_session_to_context error: {e}")



# ==================== STATE PERSISTENCE ====================
class JobState:
    """Manages job state persistence for resume capability"""

    def __init__(self, job_id: str):
        self.job_id = job_id
        self.state_file = STATE_DIR / f"{job_id}_state.json"
        self.checkpoint_file = STATE_DIR / f"{job_id}_checkpoint.json"

    def save_state(self, data: Dict[str, Any]):
        """Save current job state"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            LOG.error(f"Failed to save state for {self.job_id}: {e}")

    def load_state(self) -> Optional[Dict[str, Any]]:
        """Load saved job state"""
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            LOG.error(f"Failed to load state for {self.job_id}: {e}")
        return None

    def save_checkpoint(self, processed_meters: List[str], last_index: int):
        """Save checkpoint of processed meters"""
        try:
            checkpoint = {
                "processed_meters": processed_meters,
                "last_index": last_index,
                "timestamp": datetime.now().isoformat()
            }
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint, f, indent=2)
        except Exception as e:
            LOG.error(f"Failed to save checkpoint for {self.job_id}: {e}")

    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Load checkpoint"""
        try:
            if self.checkpoint_file.exists():
                with open(self.checkpoint_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            LOG.error(f"Failed to load checkpoint for {self.job_id}: {e}")
        return None

    def clear(self):
        """Clear all state files"""
        try:
            if self.state_file.exists():
                self.state_file.unlink()
            if self.checkpoint_file.exists():
                self.checkpoint_file.unlink()
        except Exception as e:
            LOG.error(f"Failed to clear state for {self.job_id}: {e}")


# ==================== THREAD-SAFE CANCELLATION ====================
_global_lock = threading.Lock()

def should_cancel(job_id: str) -> bool:
    with _global_lock:
        if job_id not in cancellation_locks:
            cancellation_locks[job_id] = threading.Lock()
    
    with cancellation_locks[job_id]:
        return jobs.get(job_id, {}).get('cancel_requested', False)


def set_cancel_flag(job_id: str, value: bool):
    """Thread-safe cancellation flag setter"""
    if job_id not in cancellation_locks:
        cancellation_locks[job_id] = threading.Lock()

    with cancellation_locks[job_id]:
        if job_id in jobs:
            jobs[job_id]['cancel_requested'] = value
            save_job_meta(job_id)


# ==================== HEARTBEAT MONITORING ====================
async def heartbeat_monitor(job_id: str, page: Page):
    """Monitor page responsiveness with heartbeat checks without interfering with page operations."""
    last_response = time.time()
    missed_count = 0
    page_lock = _get_page_lock(job_id)

    try:
        while not should_cancel(job_id):
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)

                # Try to acquire lock non-blocking
                try:
                    await asyncio.wait_for(page_lock.acquire(), timeout=0.8)
                    got_lock = True
                except asyncio.TimeoutError:
                    got_lock = False

                if not got_lock:
                    LOG.debug(f"[{job_id}] Heartbeat skipped because page lock busy")
                    continue

                try:
                    # FIXED: Check if page is still valid before evaluating
                    if page.is_closed():
                        LOG.warning(f"[{job_id}] Heartbeat detected closed page, stopping monitor")
                        break
                    
                    # Short evaluate to test liveliness
                    try:
                        await asyncio.wait_for(page.evaluate("() => true"), timeout=3.0)
                        last_response = time.time()
                        missed_count = 0
                    except asyncio.TimeoutError:
                        missed_count += 1
                        elapsed = time.time() - last_response
                        LOG.debug(f"[{job_id}] Heartbeat evaluate timed out ({missed_count})")
                        if elapsed > HEARTBEAT_TIMEOUT:
                            LOG.error(f"Job {job_id} heartbeat timeout ({elapsed}s without response)")
                            await broadcast(job_id, {
                                "type": "error",
                                "msg": f"⚠️ Browser appears hung ({int(elapsed)}s no response)"
                            })
                    except Exception as e:
                        LOG.debug(f"[{job_id}] Heartbeat evaluate error: {e}")

                finally:
                    # always release lock if we acquired it
                    try:
                        page_lock.release()
                    except Exception:
                        pass

            except asyncio.CancelledError:
                LOG.info(f"[{job_id}] Heartbeat monitor cancelled")
                break
            except Exception as e:
                LOG.debug(f"[{job_id}] Heartbeat monitor error: {e}")
    
    finally:
        # Clean up task reference
        heartbeat_tasks.pop(job_id, None)
        LOG.info(f"[{job_id}] Heartbeat monitor stopped")


async def start_heartbeat_monitor(job_id: str, page: Page):
    """Start or restart heartbeat monitor for a job."""
    # Cancel existing monitor if any
    await stop_heartbeat_monitor(job_id)
    
    # Start new monitor
    task = asyncio.create_task(heartbeat_monitor(job_id, page))
    heartbeat_tasks[job_id] = task
    LOG.info(f"[{job_id}] Heartbeat monitor started")
    return task


async def stop_heartbeat_monitor(job_id: str):
    """Stop heartbeat monitor for a job."""
    task = heartbeat_tasks.get(job_id)
    if task and not task.done():
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=2.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass
        LOG.info(f"[{job_id}] Heartbeat monitor stopped")


# ==================== EXPONENTIAL BACKOFF ====================
async def exponential_backoff(attempt: int, base: float = BACKOFF_BASE):
    """Calculate and sleep for exponential backoff delay"""
    delay = min(base ** attempt, 60)  # Cap at 60 seconds
    # simple jitter
    jitter = delay * 0.1 * (0.5 - (time.time() % 1))
    total_delay = delay + jitter
    LOG.info(f"Backing off for {total_delay:.2f}s (attempt {attempt})")
    await asyncio.sleep(total_delay)

# ==================== CONNECTION RETRY WRAPPER ====================
async def retry_on_connection_error(coro, job_id: str, operation: str):
    """Retry async operation on connection errors with exponential backoff"""
    for attempt in range(CONNECTION_RETRY_ATTEMPTS):
        try:
            return await coro
        except Exception as e:
            error_msg = str(e).lower()
            is_connection_error = any(phrase in error_msg for phrase in [
                "target closed",
                "connection closed",
                "disconnected",
                "navigation failed",
                "context closed",
                "browser closed"
            ])

            if is_connection_error and attempt < CONNECTION_RETRY_ATTEMPTS - 1:
                LOG.warning(f"{operation} failed (attempt {attempt + 1}): {e}")
                await broadcast(job_id, {
                    "type": "warning",
                    "msg": f"⚠️ Connection error during {operation}, retrying..."
                })
                await exponential_backoff(attempt)
                continue
            else:
                raise





# Database init
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT,
        meter TEXT,
        kind TEXT,
        postcode TEXT,
        consumption TEXT,
        start_date TEXT,
        end_date TEXT,
        raw_json TEXT,
        status TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS job_attempts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT,
        attempt_number INTEGER,
        status TEXT,
        error_message TEXT,
        meters_processed INTEGER,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    conn.close()

init_db()


def save_result_db(job_id: str, rec: Dict[str, Any]):
    """Save result to database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        start_date = rec.get("Proposed Start Date (Elec)") or rec.get("Proposed Start Date (Gas)")
        c.execute("""
            INSERT INTO results (job_id, meter, kind, postcode, consumption, start_date, end_date, raw_json, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job_id,
            rec.get("meter"),
            rec.get("kind"),
            rec.get("Site Postcode"),
            rec.get("Electricity Consumption (kWh)") or rec.get("Gas Consumption (kWh)"),
            start_date,
            None,
            json.dumps(rec, default=str),
            "ok" if not rec.get("error") else "error"
        ))
        conn.commit()
    except Exception as e:
        LOG.error("DB save_result_db error: %s", e, exc_info=True)
    finally:
        try:
            conn.close()
        except:
            pass


def save_job_attempt(job_id: str, attempt_number: int, status: str, error_message: str = None, meters_processed: int = 0):
    """Track job attempt in database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            INSERT INTO job_attempts (job_id, attempt_number, status, error_message, meters_processed)
            VALUES (?, ?, ?, ?, ?)
        """, (job_id, attempt_number, status, error_message, meters_processed))
        conn.commit()
    except Exception as e:
        LOG.error("DB save_job_attempt error: %s", e, exc_info=True)
    finally:
        try:
            conn.close()
        except:
            pass


def filter_row_by_type(row: Dict[str, Any]) -> Dict[str, Any]:
    """Filter row to only include relevant fields based on meter type"""
    kind = row.get("kind", "")
    
    common_fields = [
        "meter", "kind", "error",
        "Site Postcode", "Site Building Name", "Site Street No", "Site Street 1",
        "Site Street 2", "Site Town"
    ]
    
    if kind == "Electricity":
        allowed_fields = common_fields + [
            "Electricity Consumption (kWh)",
            "MPAN Topline",
            "Proposed Start Date (Elec)",
            "Energization Status"  # NEW FIELD
        ]
    elif kind == "Gas":
        allowed_fields = common_fields + [
            "Gas Consumption (kWh)",
            "MPRN",
            "Proposed Start Date (Gas)",
            "Transportation AQ"
        ]
    else:
        return row
    
    filtered_row = {k: v for k, v in row.items() if k in allowed_fields}
    return filtered_row


async def broadcast(job_id: str, message: dict):
    """Safely broadcast to websockets with error handling and logging"""
    # Always log the message for debugging
    LOG.info(f"[Job {job_id}] Broadcasting: {message.get('type', 'unknown')} - {message.get('msg', '')[:100]}")
    
    # Store in job log
    job = jobs.get(job_id)
    if job is not None:
        job.setdefault("log", []).append(message)
    
    # Get websocket connections
    conns = ws_queues.get(job_id, [])
    if not conns:
        LOG.debug(f"[Job {job_id}] No WebSocket connections to broadcast to")
        return
    
    # Filter row data if present
    if "row" in message and isinstance(message["row"], dict):
        message = message.copy()
        message["row"] = filter_row_by_type(message["row"])
    
    # Send to all connections
    dead_connections = []
    for ws in conns:
        try:
            await ws.send_json(message)
            LOG.debug(f"[Job {job_id}] Successfully sent message to WebSocket")
        except Exception as e:
            LOG.warning(f"[Job {job_id}] WebSocket send failed: {e}")
            dead_connections.append(ws)
    
    # Clean up dead connections
    for ws in dead_connections:
        try:
            conns.remove(ws)
        except:
            pass


def detect_meter_type(meter: str) -> str:
    """Detect if meter is MPAN (Electricity) or MPRN (Gas)"""
    digits_only = re.sub(r"\D", "", meter or "")
    digit_count = len(digits_only)
    
    if digit_count == 13:
        return "Electricity"
    elif 6 <= digit_count <= 10:
        return "Gas"
    elif digit_count > 13:
        return "Electricity"
    else:
        meter_lower = meter.lower()
        if any(keyword in meter_lower for keyword in ["mpan", "elec", "electricity"]):
            return "Electricity"
        elif any(keyword in meter_lower for keyword in ["mprn", "gas"]):
            return "Gas"
        return "Gas"
    
    
    
async def ensure_browser_alive(job_id: str, playwright_ctx, browser, context, page, template_url: str = None) -> tuple:
    """Check if browser/context/page are alive; recreate with session restore if needed and validate page state."""
    try:
        # quick evaluate to check liveliness
        await asyncio.wait_for(page.evaluate("() => true"), timeout=2.0)

        # ensure we are not on login page (simple heuristic)
        current_url = page.url
        if template_url and current_url and "login" in current_url.lower():
            raise Exception("Browser redirected to login - needs reconnection")

        return playwright_ctx, browser, context, page
    except Exception as e:
        LOG.warning(f"[{job_id}] Browser/page unhealthy: {e}, recreating...")
        await broadcast(job_id, {"type": "warning", "msg": "⚠️ Reconnecting browser..."})

        # FIXED: Stop heartbeat before closing browser
        await stop_heartbeat_monitor(job_id)

        # Attempt to capture session before shutdown
        try:
            await _save_session_to_meta(job_id, context, page)
        except Exception:
            pass

        # close with timeouts (best-effort)
        async def safe_close_obj(obj):
            try:
                if obj:
                    await asyncio.wait_for(obj.close(), timeout=3.0)
            except Exception:
                pass

        try:
            await safe_close_obj(page)
            await safe_close_obj(context)
            await safe_close_obj(browser)
            if playwright_ctx:
                try:
                    await asyncio.wait_for(playwright_ctx.stop(), timeout=3.0)
                except Exception:
                    pass
        except Exception:
            pass

        await asyncio.sleep(1.5)

        # recreate playwright/browser/context/page
        playwright_ctx = await async_playwright().start()
        browser = await playwright_ctx.chromium.launch(
            headless=BROWSER_HEADLESS,
            args=['--start-maximized', '--disable-blink-features=AutomationControlled']
                    if not BROWSER_HEADLESS else ['--disable-blink-features=AutomationControlled']
        )
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080} if not BROWSER_HEADLESS else None,
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = await context.new_page()
        page.set_default_timeout(PAGE_LOAD_TIMEOUT)

        # Restore session cookies/localStorage if available
        try:
            await _restore_session_to_context(job_id, context, page)
        except Exception:
            LOG.debug(f"[{job_id}] Session restore failed")

        # ✅ UPDATED: Use new login signature
        login_ok, new_template_url = await login_to_portal(page, job_id, template_url)
        
        if not login_ok:
            raise Exception("Re-login failed")
        
        # Update template URL if we got a new one
        if new_template_url:
            template_url = new_template_url

        # ensure page lock exists for this job
        _get_page_lock(job_id)
        
        # FIXED: Restart heartbeat monitor with new page
        await start_heartbeat_monitor(job_id, page)

        await broadcast(job_id, {"type": "success", "msg": "✅ Browser reconnected and ready"})
        return playwright_ctx, browser, context, page



async def safe_page_operation(page: Page, operation_func, job_id: str, operation_name: str, max_retries: int = 3):
    """Wrapper for page operations with connection recovery, cancellation checks, and per-job page lock."""
    page_lock = _get_page_lock(job_id)

    last_exc = None
    for attempt in range(max_retries):
        try:
            # Check cancellation FIRST
            if should_cancel(job_id):
                LOG.info(f"[{job_id}] {operation_name} cancelled before attempt {attempt + 1}")
                raise asyncio.CancelledError()

            # Check page validity
            if page.is_closed():
                LOG.warning(f"[{job_id}] Page closed before {operation_name}")
                raise Exception("Page is closed")

            # Ensure page still alive before attempting
            try:
                await asyncio.wait_for(page.evaluate("() => true"), timeout=2.0)
            except asyncio.CancelledError:
                raise
            except Exception:
                if should_cancel(job_id):
                    raise asyncio.CancelledError()
                    
                # Try to reconstruct browser/context/page
                job = jobs.get(job_id, {})
                playwright_ctx = job.get("playwright")
                browser = job.get("browser")
                context = job.get("context")
                template_url = job.get("template_url")
                
                playwright_ctx, browser, context, page = await ensure_browser_alive(
                    job_id, playwright_ctx, browser, context, page, template_url
                )
                
                # Persist revived handles back into jobs dict
                job["playwright"] = playwright_ctx
                job["browser"] = browser
                job["context"] = context
                job["page"] = page
                asyncio.create_task(save_job_meta(job_id))

            # Use async context manager for guaranteed lock release
            async with page_lock:
                # Final cancellation check before operation
                if should_cancel(job_id):
                    LOG.info(f"[{job_id}] {operation_name} cancelled during lock acquisition")
                    raise asyncio.CancelledError()
                    
                result = await operation_func()
                return result

        except asyncio.CancelledError:
            LOG.info(f"[{job_id}] {operation_name} cancelled")
            raise
            
        except Exception as e:
            last_exc = e
            
            # Check if cancelled during error handling
            if should_cancel(job_id):
                LOG.info(f"[{job_id}] {operation_name} cancelled during error handling")
                raise asyncio.CancelledError()
            
            error_msg = str(e).lower()
            is_connection_error = any(phrase in error_msg for phrase in [
                "target closed", "connection closed", "disconnected",
                "navigation failed", "context closed", "browser closed",
                "execution context was destroyed", "session closed",
                "page is closed"
            ])

            if is_connection_error:
                LOG.warning(f"{operation_name} connection error (attempt {attempt + 1}): {e}")
                
                if should_cancel(job_id):
                    LOG.info(f"[{job_id}] Cancellation detected, not retrying {operation_name}")
                    raise asyncio.CancelledError()
                
                await broadcast(job_id, {
                    "type": "warning",
                    "msg": f"⚠️ Connection lost during {operation_name}, attempting recovery..."
                })
                
                # Attempt to recreate browser/context/page and retry
                job = jobs.get(job_id, {})
                playwright_ctx = job.get("playwright")
                browser = job.get("browser")
                context = job.get("context")
                template_url = job.get("template_url")
                
                try:
                    playwright_ctx, browser, context, page = await ensure_browser_alive(
                        job_id, playwright_ctx, browser, context, page, template_url
                    )
                    job["playwright"] = playwright_ctx
                    job["browser"] = browser
                    job["context"] = context
                    job["page"] = page
                    await save_job_meta(job_id)
                    
                    # Small backoff before retry
                    await exponential_backoff(attempt)
                    continue
                except Exception as rec_e:
                    if should_cancel(job_id):
                        raise asyncio.CancelledError()
                    LOG.warning(f"[{job_id}] Recovery attempt failed: {rec_e}")
                    await asyncio.sleep(1.0)
                    continue
                    
            # Non-connection error or out of retries
            raise last_exc
            
    # If we exit loop
    raise last_exc


async def verify_page_state(job_id: str, page: Page, required_selectors: List[str], template_url: str = None, timeout: int = 5000) -> bool:
    """
    Confirm that required UI elements are present on the page. 
    If not, navigate directly to template URL.
    """
    DIRECT_TEMPLATE_URL = "https://bgbpartnerportal.my.site.com/0064L00000DvLVS?srPos=0&srKp=006"
    
    try:
        # Check if required elements are present
        for sel in required_selectors:
            try:
                await page.wait_for_selector(sel, state="visible", timeout=timeout)
            except Exception:
                LOG.debug(f"[{job_id}] Required selector missing: {sel}")
                raise
        
        LOG.info(f"[{job_id}] ✅ Page state verified - all required elements present")
        return True
        
    except Exception as verify_error:
        LOG.warning(f"[{job_id}] Page state verification failed: {verify_error}")
        
        # Attempt recovery by navigating directly to template URL
        try:
            await broadcast(job_id, {
                "type": "warning", 
                "msg": "⚠️ Page state invalid, navigating to template..."
            })
            
            # Navigate directly to template
            LOG.info(f"[{job_id}] Attempting direct navigation to template URL")
            await page.goto(DIRECT_TEMPLATE_URL, wait_until="networkidle", timeout=15000)
            await asyncio.sleep(2.0)
            
            # Re-verify required selectors
            all_found = True
            for sel in required_selectors:
                try:
                    await page.wait_for_selector(sel, state="visible", timeout=timeout)
                except Exception as e:
                    LOG.warning(f"[{job_id}] Selector still missing after navigation: {sel} - {e}")
                    all_found = False
                    break
            
            if all_found:
                LOG.info(f"[{job_id}] ✅ Page state recovered after direct navigation")
                await broadcast(job_id, {
                    "type": "success", 
                    "msg": "✅ Page state recovered"
                })
                
                # Update template URL in job metadata
                if job_id in jobs:
                    jobs[job_id]['template_url'] = DIRECT_TEMPLATE_URL
                    await save_job_meta(job_id)
                
                return True
            else:
                LOG.error(f"[{job_id}] ❌ Page state could not be recovered")
                return False
                
        except Exception as nav_error:
            LOG.error(f"[{job_id}] Navigation recovery failed: {nav_error}")
            await broadcast(job_id, {
                "type": "error", 
                "msg": f"❌ Page recovery failed: {str(nav_error)}"
            })
            return False
        
        
@app.get("/credentials")
async def get_credentials_status():
    """Check if credentials are configured (never expose the actual password)"""
    user, password = load_credentials()
    return JSONResponse({
        "configured": bool(user and password),
        "username": user if user else None,
        "source": "file" if CREDENTIALS_FILE.exists() else "env"
    })

@app.post("/credentials")
async def save_credentials(request: Request):
    """Save portal credentials"""
    try:
        body = await request.json()
        user = body.get('username', '').strip()
        password = body.get('password', '').strip()
        
        if not user or not password:
            raise HTTPException(status_code=400, detail="Username and password required")
        
        with open(CREDENTIALS_FILE, 'w') as f:
            json.dump({'user': user, 'password': password}, f)
        
        # Update in-memory globals too
        global PORTAL_USER, PORTAL_PASS
        PORTAL_USER = user
        PORTAL_PASS = password
        
        return JSONResponse({"msg": "Credentials saved successfully"})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/credentials")
async def delete_credentials():
    """Remove saved credentials"""
    if CREDENTIALS_FILE.exists():
        CREDENTIALS_FILE.unlink()
    return JSONResponse({"msg": "Credentials cleared"})        



async def login_to_portal(page: Page, job_id: str, template_url: str = None) -> tuple[bool, str]:
    """
    Login to BGB portal and navigate directly to template page.
    Returns: (success: bool, final_template_url: str)
    """
    try:
        # Check cancellation BEFORE starting
        if should_cancel(job_id):
            LOG.info(f"[{job_id}] Login cancelled before starting")
            return False, None

        await broadcast(job_id, {"type": "status", "msg": "Navigating to login page..."})
        
        # Check if page is still valid
        if page.is_closed():
            LOG.warning(f"[{job_id}] Page closed during login")
            return False, None
        
        # Retry navigation on connection errors
        try:
            await retry_on_connection_error(
                page.goto("https://bgbpartnerportal.my.site.com/login", 
                         wait_until="networkidle", 
                         timeout=PAGE_LOAD_TIMEOUT),
                job_id,
                "login navigation"
            )
        except asyncio.CancelledError:
            LOG.info(f"[{job_id}] Login navigation cancelled")
            return False, None
        except Exception as e:
            if should_cancel(job_id):
                LOG.info(f"[{job_id}] Login cancelled during navigation")
                return False, None
            raise
        
        await asyncio.sleep(0.5)

        if should_cancel(job_id):
            LOG.info(f"[{job_id}] Login cancelled after navigation")
            return False, None

        # Check page is still alive
        if page.is_closed():
            LOG.warning(f"[{job_id}] Page closed after navigation")
            return False, None

        await broadcast(job_id, {"type": "status", "msg": "Filling login credentials..."})
        
        # Fill username
        username_filled = False
        for selector in ["input#username", "input[name='username']", "input[type='text']"]:
            if should_cancel(job_id) or page.is_closed():
                return False, None
            try:
                el = await page.query_selector(selector)
                if el and await el.is_visible():
                    await el.fill(PORTAL_USER or "")
                    username_filled = True
                    break
            except Exception as e:
                if should_cancel(job_id):
                    return False, None
                continue
                
        if not username_filled:
            await broadcast(job_id, {"type": "error", "msg": "Could not find username field"})
            return False, None

        await asyncio.sleep(0.3)
        
        if should_cancel(job_id) or page.is_closed():
            return False, None
        
        # Fill password
        password_filled = False
        for selector in ["input#password", "input[name='password']", "input[type='password']"]:
            if should_cancel(job_id) or page.is_closed():
                return False, None
            try:
                el = await page.query_selector(selector)
                if el and await el.is_visible():
                    await el.fill(PORTAL_PASS or "")
                    password_filled = True
                    break
            except Exception as e:
                if should_cancel(job_id):
                    return False, None
                continue
                
        if not password_filled:
            await broadcast(job_id, {"type": "error", "msg": "Could not find password field"})
            return False, None

        await asyncio.sleep(0.3)
        
        if should_cancel(job_id) or page.is_closed():
            return False, None

        await broadcast(job_id, {"type": "status", "msg": "Submitting login form..."})
        
        # Submit form
        submit_clicked = False
        for selector in ["button[type='submit']", "input[type='submit']"]:
            if should_cancel(job_id) or page.is_closed():
                return False, None
            try:
                el = await page.query_selector(selector)
                if el and await el.is_visible():
                    await el.click()
                    submit_clicked = True
                    break
            except Exception as e:
                if should_cancel(job_id):
                    return False, None
                continue
                
        if not submit_clicked:
            if should_cancel(job_id) or page.is_closed():
                return False, None
            try:
                await page.keyboard.press("Enter")
            except Exception:
                pass

        await broadcast(job_id, {"type": "status", "msg": "Waiting for login to complete..."})
        
        if should_cancel(job_id) or page.is_closed():
            return False, None
        
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except PlaywrightTimeout:
            if should_cancel(job_id):
                return False, None
            pass
        except Exception as e:
            if should_cancel(job_id):
                return False, None
            LOG.debug(f"Wait for load state error: {e}")
            
        await asyncio.sleep(1)

        if should_cancel(job_id) or page.is_closed():
            return False, None

        current_url = page.url
        page_content = await page.content()
        
        if "login" in current_url.lower() and "username" in page_content.lower():
            await broadcast(job_id, {"type": "error", "msg": "Login failed - still on login page"})
            return False, None
            
        if "recaptcha" in page_content.lower():
            await broadcast(job_id, {"type": "error", "msg": "CAPTCHA detected - manual intervention required"})
            return False, None

        await broadcast(job_id, {"type": "success", "msg": "✅ Login successful!"})
        
        # ✅ NEW: Navigate directly to template URL
        DIRECT_TEMPLATE_URL = "https://bgbpartnerportal.my.site.com/0064L00000DvLVS?srPos=0&srKp=006"
        
        await broadcast(job_id, {"type": "status", "msg": "📋 Navigating to template page..."})
        
        try:
            await page.goto(DIRECT_TEMPLATE_URL, wait_until="networkidle", timeout=15000)
            await asyncio.sleep(2.0)  # Wait for page to fully load
            
            # Verify we're on the right page by checking for "New Quote Item" button
            try:
                await page.wait_for_selector(
                    "input[value='New Quote Item'], button:has-text('New Quote Item')",
                    state="visible",
                    timeout=5000
                )
                await broadcast(job_id, {"type": "success", "msg": "✅ Template page loaded!"})
                return True, DIRECT_TEMPLATE_URL
                
            except Exception:
                LOG.warning(f"[{job_id}] Template page loaded but button not found")
                # Still return success - we're on the right page
                return True, DIRECT_TEMPLATE_URL
                
        except Exception as e:
            LOG.error(f"[{job_id}] Failed to navigate to template: {e}")
            await broadcast(job_id, {"type": "error", "msg": f"❌ Failed to load template page: {str(e)}"})
            return False, None

    except asyncio.CancelledError:
        LOG.info(f"[{job_id}] Login cancelled via CancelledError")
        return False, None
    except Exception as e:
        if should_cancel(job_id):
            LOG.info(f"[{job_id}] Login cancelled during exception handling")
            return False, None
        
        error_msg = str(e).lower()
        if any(phrase in error_msg for phrase in [
            "target page, context or browser has been closed",
            "browser has been closed",
            "context closed"
        ]):
            LOG.info(f"[{job_id}] Login detected browser closure")
            return False, None
            
        LOG.error("Login exception: %s", e, exc_info=True)
        await broadcast(job_id, {"type": "error", "msg": f"Login exception: {str(e)}"})
        return False, None


# IMPROVED NEW QUOTE ITEM CLICK WITH RETRIES
async def click_new_quote_item(page: Page, job_id: str, max_attempts: int = 3, timeout_per_attempt: int = 15) -> bool:
    """Click New Quote Item button with timeout per attempt"""
    
    for attempt in range(max_attempts):
        if should_cancel(job_id):
            return False
            
        try:
            await broadcast(job_id, {"type": "status", "msg": f"🔘 Looking for New Quote Item (attempt {attempt + 1}/{max_attempts})..."})
            
            start_time = time.time()
            
            # Strategy 1: Direct selector match with timeout
            new_quote_selectors = [
                "input[value='New Quote Item']",
                "button:has-text('New Quote Item')",
                "input[name*='newQuote']",
                "button[name*='newQuote']",
                "*[title='New Quote Item']"
            ]
            
            for sel in new_quote_selectors:
                if time.time() - start_time > timeout_per_attempt:
                    break
                    
                try:
                    el = await page.wait_for_selector(sel, state="visible", timeout=3000)
                    if el:
                        await el.click()
                        await asyncio.sleep(0.5)
                        await broadcast(job_id, {"type": "success", "msg": "✅ Clicked New Quote Item"})
                        return True
                except:
                    continue
            
            # Strategy 2: Text search with timeout
            if time.time() - start_time <= timeout_per_attempt:
                try:
                    all_clickable = await page.query_selector_all("input, button, a")
                    for elem in all_clickable[:50]:  # Limit search to first 50 elements
                        if time.time() - start_time > timeout_per_attempt:
                            break
                            
                        try:
                            if not await elem.is_visible():
                                continue
                            
                            value = await elem.get_attribute("value") or ""
                            text = await elem.inner_text() if await elem.evaluate("el => el.tagName !== 'INPUT'") else ""
                            title = await elem.get_attribute("title") or ""
                            
                            if any("new quote" in s.lower() for s in [value, text, title]):
                                await elem.click()
                                await asyncio.sleep(0.5)
                                await broadcast(job_id, {"type": "success", "msg": "✅ Clicked New Quote Item (text search)"})
                                return True
                        except:
                            continue
                except Exception as e:
                    LOG.debug(f"Text search failed: {e}")
            
            # If we've exhausted time for this attempt, try refresh if not last attempt
            if attempt < max_attempts - 1:
                await broadcast(job_id, {"type": "warning", "msg": "⚠️ Button not found, refreshing page..."})
                await refresh_page_safe(page, job_id)
                continue
            
        except Exception as e:
            LOG.warning(f"New Quote Item click attempt {attempt + 1} failed: {e}")
            if attempt < max_attempts - 1:
                await exponential_backoff(attempt)
                continue
    
    return False


async def wait_for_meter_results(page: Page, job_id: str, meter: str, kind: str, max_wait_seconds: int = 90) -> tuple[bool, dict]:
    """
    Wait for results with explicit timeout and enhanced validation for Gas meters
    """
    # Gas meters often take longer to load
    if kind == "Gas":
        max_wait_seconds = 120  # 2 minutes for Gas
    
    start_time = time.time()
    found_results = False
    
    iteration = 0
    last_progress = time.time()
    
    while time.time() - start_time < max_wait_seconds:
        if should_cancel(job_id):
            raise asyncio.CancelledError()
            
        iteration += 1
        
        try:
            body = await page.inner_text("body", timeout=3000)
            if body and len(body) > 200000:
                body = body[:200000]
            body_lower = (body or "").lower()

            # Check for "Future contract already agreed"
            if "future contract already agreed" in body_lower:
                await broadcast(job_id, {
                    "type": "warning",
                    "msg": f"⚠️ Future contract already agreed for {meter}"
                })
                return False, {
                    "meter": meter,
                    "kind": kind,
                    "not_found": True,
                    "error": "Future contract already agreed for meter point"
                }

            # Check for "No site details found"
            if "no site details found" in body_lower or "no site details found for meter point" in body_lower:
                LOG.info(f"[{job_id}] No site details found for {meter}")
                
                # Try to extract AQ for Gas even if unfound
                aq_value = None
                if kind == "Gas":
                    try:
                        aq_selectors = SELECTOR_MAP.get("Transportation AQ", [])
                        for sel in aq_selectors[:5]:
                            try:
                                el = await page.query_selector(sel)
                                if el and await el.is_visible():
                                    val = await el.input_value() or await el.get_attribute("value")
                                    if val and str(val).strip():
                                        try:
                                            aq_numeric = float(str(val).strip().replace(",", ""))
                                            if aq_numeric > 0:
                                                aq_value = str(val).strip()
                                                LOG.info(f"[{job_id}] Found AQ > 0 for unfound meter: {aq_value}")
                                                break
                                        except ValueError:
                                            continue
                            except:
                                continue
                    except Exception as e:
                        LOG.debug(f"[{job_id}] AQ extraction for unfound failed: {e}")

                return False, {
                    "meter": meter,
                    "kind": kind,
                    "not_found": True,
                    "with_british_gas": False,
                    "Transportation AQ": aq_value
                }
            
            # Check for other not found messages
            not_found_phrases = [
                "no records found", "not found", "no results",
                "meter not registered", "meter could not be found",
                "meter point not found", "unable to find meter"
            ]
            if any(phrase in body_lower for phrase in not_found_phrases):
                return False, {
                    "meter": meter,
                    "kind": kind,
                    "error": "Meter not found with BGB",
                    "not_found": True
                }

            # Check for successful data load
            has_postcode = bool(re.search(r"\b[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[ABD-HJLNP-UW-Z]{2}\b", body or "", re.I))
            has_consumption = bool(re.search(r"\d+\.?\d*\s*(?:kwh|units)", body or "", re.I))
            
            # For Gas, also check for AQ or MPRN presence
            has_gas_indicators = False
            if kind == "Gas":
                has_gas_indicators = bool(
                    re.search(r"transportation|annual\s+quantity|mprn", body_lower) or
                    re.search(r"\d{6,10}", body or "")  # MPRN format
                )

            # Check for visible input fields
            found_input = False
            check_fields = []
            
            if kind == "Gas":
                check_fields = [
                    "Site Postcode", "Gas Consumption (kWh)", 
                    "Transportation AQ", "MPRN", "Proposed Start Date (Gas)"
                ]
            else:
                check_fields = [
                    "Site Postcode", "Electricity Consumption (kWh)",
                    "MPAN Topline", "Energization Status"
                ]
            
            for field_name in check_fields:
                if field_name not in SELECTOR_MAP:
                    continue
                for sel in SELECTOR_MAP[field_name][:3]:
                    try:
                        el = await page.query_selector(sel)
                        if el and await el.is_visible():
                            # For Gas, verify field has value
                            if kind == "Gas":
                                try:
                                    val = await el.input_value() or await el.get_attribute("value")
                                    if val and str(val).strip():
                                        found_input = True
                                        LOG.debug(f"[{job_id}] Found {field_name} with value")
                                        break
                                except:
                                    pass
                            else:
                                found_input = True
                                break
                    except:
                        continue
                if found_input:
                    break

            # Success criteria (more lenient for Gas)
            if kind == "Gas":
                if has_postcode or has_consumption or has_gas_indicators or found_input:
                    found_results = True
                    await broadcast(job_id, {"type": "success", "msg": f"✅ Gas data loaded for {meter}"})
                    return True, {}
            else:
                if has_postcode or has_consumption or found_input:
                    found_results = True
                    await broadcast(job_id, {"type": "success", "msg": f"✅ Electricity data loaded for {meter}"})
                    return True, {}

            # Progress indicator (less frequent for Gas)
            if iteration % 15 == 0:
                elapsed = int(time.time() - start_time)
                await broadcast(job_id, {
                    "type": "status", 
                    "msg": f"⏳ Waiting for {kind} data... ({elapsed}s)"
                })
                LOG.debug(f"[{job_id}] Still waiting for {kind} results: iteration {iteration}, elapsed {elapsed}s")

        except Exception as e:
            LOG.debug(f"Results check error: {e}")

        await asyncio.sleep(1)
    
    # Timeout reached
    LOG.warning(f"[{job_id}] ⏰ Timeout waiting for {kind} results for meter {meter} after {max_wait_seconds}s")
    return False, {
        "meter": meter,
        "kind": kind,
        "error": f"Timeout waiting for results after {max_wait_seconds}s",
        "not_found": True
    }



def clean_value(v: Optional[Any]) -> Optional[str]:
    """Clean and sanitize field values"""
    if v is None:
        return None
    s = str(v)
    s = re.sub(r'<[^>]+>', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    if len(s) > 2000:
        s = s[:2000] + '...'
    return s if s != '' else None



# Add this new function after clean_value() (around line 1450)

async def extract_field_with_retry(page: Page, field_name: str, selectors: List[str], 
                                   job_id: str, max_attempts: int = 3) -> Optional[str]:
    """
    Enhanced field extraction with retry logic and label-based fallback.
    Tries harder to find critical fields like Energization Status.
    """
    found_value = None
    
    for attempt in range(max_attempts):
        # Try all selectors
        for sel in selectors:
            try:
                el = await page.query_selector(sel)
                if not el:
                    continue
                    
                # Check visibility
                is_visible = await el.is_visible()
                if not is_visible:
                    continue
                
                # Try to get value
                try:
                    val = await el.input_value()
                    if val and str(val).strip():
                        found_value = str(val).strip()
                except:
                    try:
                        val = await el.get_attribute("value")
                        if val and str(val).strip():
                            found_value = str(val).strip()
                    except:
                        pass
                
                # Validate the value isn't just the field name
                if found_value and found_value != field_name:
                    LOG.debug(f"[{job_id}] Found {field_name}: {found_value} using selector: {sel}")
                    return clean_value(found_value)
                    
            except Exception as e:
                LOG.debug(f"[{job_id}] Selector {sel} failed for {field_name}: {e}")
                continue
        
        # If no value found and not last attempt, try label-based search
        if not found_value and attempt < max_attempts - 1:
            try:
                # Look for labels containing the field name keywords
                keywords = []
                if "Energization" in field_name:
                    keywords = ["energization", "energisation", "status"]
                elif "Transportation" in field_name or "AQ" in field_name:
                    keywords = ["transportation", "aq", "annual quantity"]
                elif "Consumption" in field_name:
                    keywords = ["consumption", "usage", "kwh"]
                
                if keywords:
                    for keyword in keywords:
                        # Find labels with this keyword
                        labels = await page.query_selector_all(
                            f"label:has-text('{keyword}'), span:has-text('{keyword}'), "
                            f"td:has-text('{keyword}'), th:has-text('{keyword}')"
                        )
                        
                        for label in labels[:5]:  # Check first 5 matches
                            try:
                                # Get the 'for' attribute
                                label_for = await label.get_attribute('for')
                                if label_for:
                                    input_el = await page.query_selector(f"input#{label_for}")
                                    if input_el and await input_el.is_visible():
                                        val = await input_el.input_value() or await input_el.get_attribute("value")
                                        if val and str(val).strip():
                                            found_value = str(val).strip()
                                            LOG.info(f"[{job_id}] Found {field_name} via label search: {found_value}")
                                            return clean_value(found_value)
                                
                                # Try sibling/next input
                                parent = await label.evaluate_handle("el => el.parentElement")
                                if parent:
                                    inputs = await parent.as_element().query_selector_all("input[type='text']")
                                    for inp in inputs:
                                        if await inp.is_visible():
                                            val = await inp.input_value() or await inp.get_attribute("value")
                                            if val and str(val).strip():
                                                found_value = str(val).strip()
                                                LOG.info(f"[{job_id}] Found {field_name} via parent search: {found_value}")
                                                return clean_value(found_value)
                            except:
                                continue
                                
            except Exception as e:
                LOG.debug(f"[{job_id}] Label-based search failed for {field_name}: {e}")
        
        # Wait before retry
        if not found_value and attempt < max_attempts - 1:
            await asyncio.sleep(0.5)
    
    return None



def try_parse_date(s: Optional[str]) -> Optional[datetime]:
    """Parse date string with multiple format attempts"""
    if not s:
        return None
    s = s.strip().replace('.', '/').replace('-', '/').strip()
    
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    
    m = re.search(r"(\d{1,2})\D+(\d{1,2})\D+(\d{2,4})", s)
    if m:
        d, mo, y = m.groups()
        y = y if len(y) == 4 else ('20' + y if len(y) == 2 else y)
        try:
            return datetime.strptime(f"{d}/{mo}/{y}", "%d/%m/%Y")
        except Exception:
            pass
    return None



# 2. ADD PAGE REFRESH ON FAILURES
async def refresh_page_safe(page: Page, job_id: str) -> bool:
    """Safely refresh page and wait for stability"""
    try:
        await broadcast(job_id, {"type": "status", "msg": "🔄 Refreshing page..."})
        await page.reload(wait_until="networkidle", timeout=PAGE_LOAD_TIMEOUT)
        await asyncio.sleep(2.0)  # Extra wait after refresh
        return True
    except Exception as e:
        LOG.warning(f"Page refresh failed: {e}")
        return False


# 3. IMPROVED METER INPUT DETECTION WITH RETRY
async def find_and_fill_meter_input(page: Page, meter: str, kind: str, job_id: str, max_attempts: int = 3) -> bool:
    """Find and fill meter input with improved Gas detection"""
    
    if kind == "Gas":
        selectors = [
            "input.MPR__c",
            "input[class*='MPR__c']",
            "input[id*='j_id787:j_id790']",
            "input.MPRN__c",
            "input[class*='MPRN']",
            "input[name*='MPRN']",
            "input[id*='MPRN']",  # NEW
            "input[placeholder*='MPRN']",
            "input[type='text'][id*='gas' i][id*='meter' i]",  # NEW
            "input[type='text'][name*='gas' i]",  # NEW
        ]
    else:
        selectors = [
            "input.MPAN__c",
            "input[class*='MPAN__c']", 
            "input[id*='j_id358']",
            "input[class*='MPAN']",
            "input[name*='MPAN']",
            "input[id*='MPAN']",  # NEW
            "input[placeholder*='MPAN']",
            "input[type='text'][id*='elec' i][id*='meter' i]",  # NEW
        ]
    
    for attempt in range(max_attempts):
        try:
            # Wait longer for Gas meters (they seem slower)
            await asyncio.sleep(1.5 if kind == "Gas" else 1.0)
            
            # Try each selector
            for selector in selectors:
                try:
                    el = await page.wait_for_selector(selector, state="attached", timeout=3000)
                    if not el:
                        continue
                    
                    is_visible = await el.is_visible()
                    if not is_visible:
                        LOG.debug(f"[{job_id}] Selector {selector} found but not visible")
                        continue
                    
                    # Focus and clear
                    await el.click()
                    await asyncio.sleep(0.5)  # Increased delay for Gas
                    
                    # Multiple clear methods
                    await el.fill("")
                    await asyncio.sleep(0.3)
                    
                    await page.keyboard.press("Control+A")
                    await page.keyboard.press("Backspace")
                    await asyncio.sleep(0.3)
                    
                    # Fill meter value
                    await el.fill(meter)
                    await asyncio.sleep(0.7 if kind == "Gas" else 0.5)  # Extra delay for Gas
                    
                    # Verify
                    filled_value = await el.input_value()
                    if filled_value and filled_value.strip() == meter.strip():
                        LOG.info(f"[{job_id}] ✅ Successfully filled {kind} meter: {meter} using selector: {selector}")
                        await broadcast(job_id, {
                            "type": "success", 
                            "msg": f"✅ Filled {kind} meter input: {meter}"
                        })
                        return True
                    else:
                        LOG.warning(f"[{job_id}] Fill verification failed. Expected: {meter}, Got: {filled_value}")
                        
                except Exception as e:
                    LOG.debug(f"[{job_id}] Selector {selector} failed: {str(e)}")
                    continue
            
            # Label-based detection (especially important for Gas)
            if attempt < max_attempts - 1:
                LOG.info(f"[{job_id}] Standard selectors failed, trying label-based detection for {kind}...")
                try:
                    label_text = "MPRN" if kind == "Gas" else "MPAN"
                    labels = await page.query_selector_all(
                        f"label:has-text('{label_text}'), span:has-text('{label_text}'), "
                        f"td:has-text('{label_text}'), th:has-text('{label_text}')"
                    )
                    
                    for label in labels:
                        try:
                            label_for = await label.get_attribute('for')
                            if label_for:
                                input_el = await page.query_selector(f"input#{label_for}")
                                if input_el and await input_el.is_visible():
                                    await input_el.click()
                                    await asyncio.sleep(0.5)
                                    await input_el.fill("")
                                    await asyncio.sleep(0.3)
                                    await input_el.fill(meter)
                                    await asyncio.sleep(0.7)
                                    
                                    filled_value = await input_el.input_value()
                                    if filled_value and filled_value.strip() == meter.strip():
                                        LOG.info(f"[{job_id}] ✅ Filled using label-based detection")
                                        return True
                        except Exception:
                            continue
                except Exception as e:
                    LOG.debug(f"[{job_id}] Label-based detection failed: {e}")
            
            # Retry with refresh
            if attempt < max_attempts - 1:
                await broadcast(job_id, {
                    "type": "warning", 
                    "msg": f"⚠️ {kind} meter input not found, refreshing... (attempt {attempt + 1}/{max_attempts})"
                })
                await refresh_page_safe(page, job_id)
                await asyncio.sleep(2.5 if kind == "Gas" else 2.0)  # Longer wait for Gas
                continue
            
        except Exception as e:
            LOG.warning(f"[{job_id}] Meter input attempt {attempt + 1} failed: {e}")
            if attempt < max_attempts - 1:
                await refresh_page_safe(page, job_id)
                await asyncio.sleep(2.5)
                continue
    
    LOG.error(f"[{job_id}] ❌ Failed to fill {kind} meter input after {max_attempts} attempts")
    return False


# 4. IMPROVED NAVIGATION WITH VERIFICATION
async def navigate_back_to_template(page: Page, job_id: str, template_url: str, max_attempts: int = 3) -> bool:
    """Navigate back to template URL - simplified since we use direct URL"""
    
    # Use hardcoded template URL
    TEMPLATE_URL = "https://bgbpartnerportal.my.site.com/0064L00000DvLVS?srPos=0&srKp=006"
    
    for attempt in range(max_attempts):
        if should_cancel(job_id):
            LOG.info(f"[{job_id}] Navigation cancelled before attempt {attempt + 1}")
            raise asyncio.CancelledError()
            
        try:
            LOG.info(f"[{job_id}] Navigation attempt {attempt + 1}/{max_attempts}")
            
            # Direct navigation to template URL
            await asyncio.wait_for(
                page.goto(TEMPLATE_URL, wait_until="networkidle", timeout=15000),
                timeout=20.0
            )
            
            await asyncio.sleep(1.5)
            
            # Verify button presence
            try:
                await page.wait_for_selector(
                    "input[value='New Quote Item'], button:has-text('New Quote Item')",
                    state="visible",
                    timeout=5000
                )
                LOG.info(f"[{job_id}] ✅ Navigation successful")
                return True
            except:
                LOG.warning(f"[{job_id}] Page loaded but button not found")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(2.0)
                    continue
                
        except asyncio.CancelledError:
            raise
        except Exception as e:
            LOG.warning(f"[{job_id}] Navigation attempt {attempt + 1} failed: {e}")
            if attempt < max_attempts - 1:
                await asyncio.sleep(2.0)
                continue
    
    return False



async def process_single_meter_with_retry(page: Page, meter: str, job_id: str, 
                                         is_first_meter: bool = False, 
                                         template_url: str = None) -> Dict[str, Any]:
    """Process meter with exponential backoff on failures"""
    for attempt in range(METER_RETRIES):
        try:
            if attempt > 0:
                await broadcast(job_id, {
                    "type": "info",
                    "msg": f"🔄 Retry {attempt + 1}/{METER_RETRIES} for meter {meter}"
                })
                await exponential_backoff(attempt)
            
            result = await process_single_meter_async(
                page, meter, job_id, is_first_meter, template_url
            )
            
            # If successful or not found (valid result), return
            if not result.get("error") or result.get("not_found"):
                return result
                
            # If error and more retries available, continue
            if attempt < METER_RETRIES - 1:
                continue
            else:
                return result
                
        except Exception as e:
            if attempt < METER_RETRIES - 1:
                LOG.warning(f"Meter {meter} attempt {attempt + 1} failed: {e}")
                continue
            else:
                return {
                    "meter": meter,
                    "kind": detect_meter_type(meter),
                    "error": f"Failed after {METER_RETRIES} attempts: {str(e)}"
                }


async def process_single_meter_async(page: Page, meter: str, job_id: str, 
                                    is_first_meter: bool = False, 
                                    template_url: str = None) -> Dict[str, Any]:
    """
    Process a single meter - ENHANCED AQ EXTRACTION FOR GAS METERS
    """
    if should_cancel(job_id):
        raise asyncio.CancelledError()

    await broadcast(job_id, {"type": "status", "msg": f"🔍 Processing meter: {meter}"})
    LOG.info(f"[Job {job_id}] Starting to process meter: {meter}")
    
    kind = detect_meter_type(meter)

    try:
        # STEP 1: Navigate back (if not first meter)
        if not is_first_meter:
            if should_cancel(job_id):
                raise asyncio.CancelledError()
            
            await broadcast(job_id, {"type": "status", "msg": f"🔙 Navigating back to template..."})
            
            # Use safe_page_operation for navigation with retry
            async def nav_operation():
                return await navigate_back_to_template(page, job_id, template_url, max_attempts=3)
            
            try:
                success = await safe_page_operation(
                    page, 
                    nav_operation, 
                    job_id, 
                    "navigate_back", 
                    max_retries=2  # Outer retry wrapper
                )
            except Exception as e:
                LOG.error(f"[{job_id}] Navigation failed with error: {e}")
                return {
                    "meter": meter, 
                    "kind": kind, 
                    "error": f"Navigation failed: {str(e)}", 
                    "not_found": True
                }
            
            if not success:
                LOG.error(f"[{job_id}] Failed to navigate back after all retries")
                return {
                    "meter": meter, 
                    "kind": kind, 
                    "error": "Failed to navigate back after retries", 
                    "not_found": True
                }

        if should_cancel(job_id):
            raise asyncio.CancelledError()

        # STEP 2: Click New Quote Item with timeout
        await broadcast(job_id, {"type": "status", "msg": f"🔘 Looking for New Quote Item button..."})
        if not await click_new_quote_item(page, job_id, max_attempts=2):
            return {"meter": meter, "kind": kind, "error": "Could not find New Quote Item button", "not_found": True}

        if should_cancel(job_id):
            raise asyncio.CancelledError()

        # STEP 3: Select meter type dropdown
        await broadcast(job_id, {"type": "status", "msg": f"📋 Selecting {kind} meter type..."})
        await asyncio.sleep(1.0)
        selection_success = False
        try:
            select_elements = await page.query_selector_all("select")
            for select in select_elements:
                if should_cancel(job_id):
                    raise asyncio.CancelledError()
                try:
                    if not await select.is_visible():
                        continue
                    options = await select.query_selector_all("option")
                    for option in options:
                        option_text = (await option.inner_text() or "").strip()
                        if kind.lower() in option_text.lower():
                            option_value = await option.get_attribute("value") or ""
                            if option_value:
                                await select.select_option(value=option_value)
                            else:
                                await select.select_option(label=option_text)
                            selection_success = True
                            await asyncio.sleep(0.5)
                            break
                except Exception:
                    continue
                if selection_success:
                    break
        except Exception as e:
            LOG.warning(f"Dropdown selection failed: {e}")

        await asyncio.sleep(0.5)

        if should_cancel(job_id):
            raise asyncio.CancelledError()

        # STEP 4: Click Continue
        await broadcast(job_id, {"type": "status", "msg": f"▶️ Clicking Continue..."})
        continue_clicked = False
        continue_selectors = [
            "input[value='Continue']",
            "button:has-text('Continue')"
        ]
        for sel in continue_selectors:
            try:
                el = await page.wait_for_selector(sel, state="visible", timeout=5000)
                if el:
                    await el.click()
                    continue_clicked = True
                    await asyncio.sleep(0.5)
                    break
            except:
                continue

        if not continue_clicked:
            try:
                await page.keyboard.press("Enter")
                await asyncio.sleep(0.5)
            except:
                pass

        if should_cancel(job_id):
            raise asyncio.CancelledError()

        # STEP 5: Fill meter number WITH RETRY
        await broadcast(job_id, {"type": "status", "msg": f"✍️ Filling meter number..."})
        await asyncio.sleep(1.0)
        meter_filled = await find_and_fill_meter_input(page, meter, kind, job_id, max_attempts=3)
        if not meter_filled:
            return {"meter": meter, "kind": kind, "error": f"Could not find {kind} meter input after retries", "not_found": True}

        if should_cancel(job_id):
            raise asyncio.CancelledError()

        # STEP 6: Click search (multiple fallbacks)
        await broadcast(job_id, {"type": "status", "msg": f"🔍 Searching meter data..."})
        await asyncio.sleep(0.5)
        try:
            search_clicked = False
            try:
                img = await page.wait_for_selector("img[title*='MPAN'], img[title*='MPRN'], img[title*='Search']", 
                                                   state="visible", timeout=3000)
                if img:
                    try:
                        parent = await img.evaluate_handle("el => el.closest('a') || el.parentElement")
                        parent_el = parent.as_element() if parent else None
                        if parent_el:
                            await parent_el.click()
                        else:
                            await img.click()
                        search_clicked = True
                    except Exception:
                        await img.click()
                        search_clicked = True
            except:
                pass

            if not search_clicked:
                try:
                    await page.click("a[onclick*='getRelDetails']", timeout=2000)
                    search_clicked = True
                except:
                    pass

            if not search_clicked:
                await page.keyboard.press("Enter")

            await asyncio.sleep(2.0)
        except Exception as e:
            LOG.warning(f"Search click failed: {e}")
            try:
                await page.keyboard.press("Enter")
                await asyncio.sleep(2.0)
            except:
                pass

        if should_cancel(job_id):
            raise asyncio.CancelledError()

        # STEP 7: Wait for results with ENHANCED AQ EXTRACTION
        await broadcast(job_id, {"type": "status", "msg": f"⏳ Waiting for results..."})
        found_results, error_data = await wait_for_meter_results(page, job_id, meter, kind, max_wait_seconds=90)
        
        # If error_data returned (unfound/error cases)
        if error_data:
            return error_data

        # STEP 8: Extract fields if found
        if found_results:
            if should_cancel(job_id):
                raise asyncio.CancelledError()

            await broadcast(job_id, {"type": "status", "msg": f"📝 Extracting data..."})
            await asyncio.sleep(1.0)
            
            # Define fields to extract based on meter type
            if kind == "Gas":
                fields_to_extract = [
                    "Gas Consumption (kWh)",
                    "MPRN",
                    "Proposed Start Date (Gas)",
                    "Gas Backdate",
                    "Gas Meter Serial No",
                    "Transportation AQ",  # ← ALWAYS extract for Gas
                    "Meter Type",
                    "Site Building Name",
                    "Site Street No",
                    "Site Street 1",
                    "Site Street 2",
                    "Site Town",
                    "Site County",
                    "Site Postcode"
                ]
            else:  # Electricity
                fields_to_extract = [
                    "Electricity Consumption (kWh)",
                    "MPAN Topline",
                    "Proposed Start Date (Elec)",
                    "Meter Serial No",
                    "Energization Status",
                    "Meter Type",
                    "Site Building Name",
                    "Site Street No",
                    "Site Street 1",
                    "Site Street 2",
                    "Site Town",
                    "Site County",
                    "Site Postcode"
                ]

            fields = {key: None for key in SELECTOR_MAP.keys()}

            # Extract ALL fields (including AQ for Gas)
            for field_name in fields_to_extract:
                if should_cancel(job_id):
                    raise asyncio.CancelledError()

                if field_name not in SELECTOR_MAP:
                    continue

                sel_list = SELECTOR_MAP[field_name]
                
                # Use enhanced extraction for critical fields
                if field_name in ["Energization Status", "Transportation AQ", "Gas Consumption (kWh)", "Proposed Start Date (Gas)"]:
                    found_value = await extract_field_with_retry(
                        page, field_name, sel_list, job_id, max_attempts=3
                    )
                    if found_value:
                        fields[field_name] = found_value
                        LOG.info(f"[{job_id}] ✅ Enhanced extraction: {field_name} = {found_value}")
                    else:
                        LOG.warning(f"[{job_id}] ⚠️ Could not extract {field_name} after retries")
                    continue

                # Standard extraction for other fields
                found_value = None
                for sel in sel_list:
                    try:
                        el = await page.query_selector(sel)
                        if not el or not await el.is_visible():
                            continue
                        try:
                            val = await el.input_value()
                            if val and str(val).strip():
                                found_value = str(val).strip()
                        except:
                            try:
                                val = await el.get_attribute("value")
                                if val and str(val).strip():
                                    found_value = str(val).strip()
                            except:
                                pass

                        if found_value and found_value != field_name:
                            fields[field_name] = clean_value(found_value)
                            break
                    except:
                        continue

            # ✅ CRITICAL: For Gas meters, validate and clean AQ
            if kind == "Gas":
                aq_value = fields.get("Transportation AQ")
                if aq_value:
                    try:
                        # Validate numeric value
                        aq_numeric = float(str(aq_value).replace(",", "").strip())
                        LOG.info(f"[{job_id}] Gas AQ extracted: {aq_numeric}")
                        
                        # Keep the value regardless of threshold for now
                        # The save_final_results function will filter > 30,000 for export
                        fields["Transportation AQ"] = clean_value(str(aq_value))
                    except ValueError:
                        LOG.warning(f"[{job_id}] Invalid AQ format: {aq_value}")
                        fields["Transportation AQ"] = None
                else:
                    LOG.debug(f"[{job_id}] No AQ found for Gas meter {meter}")

            # Address deduplication (same as before)
            address_priority = [
                "Site Building Name", "Site Street No", "Site Street 1",
                "Site Street 2", "Site Town", "Site Postcode"
            ]

            seen_addr_values = set()
            for addr_field in address_priority:
                val = fields.get(addr_field)
                if val and str(val).strip() != "":
                    norm = str(val).strip()
                    if norm in seen_addr_values:
                        fields[addr_field] = None
                    else:
                        seen_addr_values.add(norm)

            fields["meter"] = meter
            fields["kind"] = kind
            fields["not_found"] = False

            return fields

        return {"meter": meter, "kind": kind, "skip_meter": True, "unfound": True, "error": "Failed to extract data"}

    except asyncio.CancelledError:
        LOG.info(f"Processing cancelled for meter: {meter}")
        raise
    except Exception as e:
        LOG.error(f"Meter processing error: {e}", exc_info=True)
        await broadcast(job_id, {"type": "error", "msg": f"❌ Error processing {meter}: {str(e)[:100]}"})
        return {"meter": meter, "kind": kind, "skip_meter": True, "unfound": True, "error": str(e)}


@dataclass
class WorkerState:
    """Track state of a single worker/browser instance"""
    worker_id: int
    playwright_ctx: Any = None
    browser: Any = None
    context: Any = None
    page: Any = None
    is_busy: bool = False
    current_meter: Optional[str] = None
    processed_count: int = 0
    last_activity: datetime = None
    heartbeat_task: Optional[asyncio.Task] = None
    page_lock: asyncio.Lock = None

    def __post_init__(self):
        if self.page_lock is None:
            self.page_lock = asyncio.Lock()
        if self.last_activity is None:
            self.last_activity = datetime.now()


class WorkerPool:
    """Manage pool of concurrent browser workers"""
    
    def __init__(self, job_id: str, size: int = 5):
        self.job_id = job_id
        self.size = size
        self.workers: List[WorkerState] = []
        self.pool_lock = asyncio.Lock()
        
    async def initialize(self):
        """Initialize all workers in the pool"""
        for worker_id in range(self.size):
            worker = WorkerState(
                worker_id=worker_id,
                last_activity=datetime.now()
            )
            self.workers.append(worker)
            
        LOG.info(f"[{self.job_id}] Initialized worker pool with {self.size} workers")
    
    async def get_available_worker(self) -> Optional[WorkerState]:
        """Get first available (non-busy) worker"""
        async with self.pool_lock:
            for worker in self.workers:
                if not worker.is_busy:
                    worker.is_busy = True
                    worker.last_activity = datetime.now()
                    return worker
        return None
    
    async def wait_for_available_worker(self, timeout: float = 300) -> WorkerState:
        start_time = time.time()
        while time.time() - start_time < timeout:
            if should_cancel(self.job_id):
                raise asyncio.CancelledError()
            
            worker = await self.get_available_worker()
            if worker:
                return worker
            
            # Sleep WITHOUT holding any locks
            await asyncio.sleep(0.1)  # Faster polling
        
        raise TimeoutError(f"No worker available after {timeout}s")
    
    def release_worker(self, worker: WorkerState):
        """Release worker back to pool"""
        worker.is_busy = False
        worker.current_meter = None
        worker.last_activity = datetime.now()
    
    async def ensure_worker_browser_alive(self, worker: WorkerState, template_url: str = None) -> WorkerState:
        """Ensure worker's browser is alive, recreate if needed"""
        try:
            if worker.page and not worker.page.is_closed():
                await asyncio.wait_for(worker.page.evaluate("() => true"), timeout=2.0)
                return worker
        except Exception as e:
            LOG.warning(f"[{self.job_id}] Worker {worker.worker_id} browser unhealthy: {e}, recreating...")
            await self._recreate_worker_browser(worker, template_url)
        
        return worker
    
    async def _recreate_worker_browser(self, worker: WorkerState, template_url: str = None):
        """Recreate browser for a specific worker"""
        # Stop heartbeat
        if worker.heartbeat_task and not worker.heartbeat_task.done():
            worker.heartbeat_task.cancel()
            try:
                await worker.heartbeat_task
            except:
                pass
        
        # Close existing resources
        await self._safe_close_worker_resources(worker)
        await asyncio.sleep(1.0)
        
        # Recreate
        worker.playwright_ctx = await async_playwright().start()
        worker.browser = await worker.playwright_ctx.chromium.launch(
            headless=BROWSER_HEADLESS,
            args=['--start-maximized', '--disable-blink-features=AutomationControlled']
                    if not BROWSER_HEADLESS else ['--disable-blink-features=AutomationControlled']
        )
        worker.context = await worker.browser.new_context(
            viewport={'width': 1920, 'height': 1080} if not BROWSER_HEADLESS else None,
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        worker.page = await worker.context.new_page()
        worker.page.set_default_timeout(PAGE_LOAD_TIMEOUT)
        
        # Restore session
        try:
            await _restore_session_to_context(self.job_id, worker.context, worker.page)
        except Exception:
            pass
        
        # Re-login if needed
        login_ok = False
        nav_ok = False

        if template_url:
            try:
                await worker.page.goto(template_url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
                await asyncio.sleep(1.0)
                try:
                    await worker.page.wait_for_selector("input[value='New Quote Item'], button:has-text('New Quote Item')", state="visible", timeout=5000)
                    nav_ok = True
                except:
                    pass
            except:
                pass

        if not nav_ok:
            # ✅ UPDATED: Use new login signature
            login_ok, new_template = await login_to_portal(worker.page, self.job_id, template_url)
            
            if login_ok and new_template:
                template_url = new_template
                nav_ok = True

        if not login_ok and not nav_ok:
            raise Exception(f"Worker {worker.worker_id} failed to re-login and navigate")
        
        # Restart heartbeat
        worker.heartbeat_task = asyncio.create_task(
            self._worker_heartbeat(worker)
        )
        
        LOG.info(f"[{self.job_id}] Worker {worker.worker_id} browser recreated")
    
    async def _worker_heartbeat(self, worker: WorkerState):
        """Monitor worker's page health"""
        last_response = time.time()
        
        while True:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                
                if should_cancel(self.job_id):
                    break
                
                if not worker.is_busy or not worker.page:
                    continue
                
                if worker.page.is_closed():
                    LOG.warning(f"[{self.job_id}] Worker {worker.worker_id} page closed")
                    break
                
                # Try non-blocking lock acquisition
                try:
                    await asyncio.wait_for(worker.page_lock.acquire(), timeout=0.8)
                    got_lock = True
                except asyncio.TimeoutError:
                    got_lock = False
                
                if not got_lock:
                    LOG.debug(f"[{self.job_id}] Worker {worker.worker_id} heartbeat skipped (lock busy)")
                    continue
                
                try:
                    await asyncio.wait_for(worker.page.evaluate("() => true"), timeout=3.0)
                    last_response = time.time()
                    worker.last_activity = datetime.now()
                except asyncio.TimeoutError:
                    elapsed = time.time() - last_response
                    if elapsed > HEARTBEAT_TIMEOUT:
                        LOG.error(f"Worker {worker.worker_id} heartbeat timeout ({elapsed}s)")
                except Exception as e:
                    LOG.debug(f"Worker {worker.worker_id} heartbeat error: {e}")
                finally:
                    try:
                        worker.page_lock.release()
                    except:
                        pass
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                LOG.debug(f"Worker {worker.worker_id} heartbeat exception: {e}")
        
        LOG.info(f"[{self.job_id}] Worker {worker.worker_id} heartbeat stopped")
    
    async def _safe_close_worker_resources(self, worker: WorkerState):
        """Safely close worker's browser resources"""
        for resource, closer in [
            (worker.page, lambda: worker.page.close() if worker.page else None),
            (worker.context, lambda: worker.context.close() if worker.context else None),
            (worker.browser, lambda: worker.browser.close() if worker.browser else None),
            (worker.playwright_ctx, lambda: worker.playwright_ctx.stop() if worker.playwright_ctx else None)
        ]:
            if resource:
                try:
                    await asyncio.wait_for(closer(), timeout=2.0)
                except Exception:
                    pass
    
    async def cleanup_all(self):
        """Cleanup all workers"""
        LOG.info(f"[{self.job_id}] Cleaning up {len(self.workers)} workers...")
        
        # Stop all heartbeats first
        for worker in self.workers:
            if worker.heartbeat_task and not worker.heartbeat_task.done():
                worker.heartbeat_task.cancel()
        
        # Wait for heartbeats to stop
        await asyncio.sleep(0.5)
        
        # Close all resources
        tasks = []
        for worker in self.workers:
            tasks.append(self._safe_close_worker_resources(worker))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        self.workers.clear()
        LOG.info(f"[{self.job_id}] Worker pool cleaned up")


async def initialize_worker(worker: WorkerState, job_id: str) -> str:
    """Initialize a worker with browser and login"""
    try:
        LOG.info(f"[{job_id}] Initializing worker {worker.worker_id}...")
        
        # Launch browser
        worker.playwright_ctx = await async_playwright().start()
        worker.browser = await worker.playwright_ctx.chromium.launch(
            headless=BROWSER_HEADLESS,
            args=['--start-maximized', '--disable-blink-features=AutomationControlled']
                    if not BROWSER_HEADLESS else ['--disable-blink-features=AutomationControlled']
        )
        worker.context = await worker.browser.new_context(
            viewport={'width': 1920, 'height': 1080} if not BROWSER_HEADLESS else None,
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        worker.page = await worker.context.new_page()
        worker.page.set_default_timeout(PAGE_LOAD_TIMEOUT)
        
        # Restore session if available
        try:
            await _restore_session_to_context(job_id, worker.context, worker.page)
        except:
            pass
        
        # ✅ UPDATED: Login and get template URL
        login_ok, template_url = await login_to_portal(worker.page, job_id)
        
        if not login_ok:
            raise Exception(f"Worker {worker.worker_id} login failed")
        
        # Save session
        try:
            await _save_session_to_meta(job_id, worker.context, worker.page)
        except:
            pass
        
        # Start heartbeat
        worker.heartbeat_task = asyncio.create_task(
            WorkerPool(job_id, 1)._worker_heartbeat(worker)
        )
        
        LOG.info(f"[{job_id}] Worker {worker.worker_id} initialized successfully")
        return template_url
        
    except Exception as e:
        LOG.error(f"Worker {worker.worker_id} initialization failed: {e}")
        raise


async def run_job_async(job_id: str, input_path: str, output_path: str, 
                        meters_list: List[str], concurrency: int, state: JobState, 
                        attempt_number: int, skip_count: int = 0):
    """
    CONCURRENT VERSION - Process meters using multiple browser workers in parallel
    WITH PROPER SKIP HANDLING
    
    Args:
        job_id: Unique job identifier
        input_path: Path to merged input Excel file
        output_path: Path to output Excel file
        meters_list: FULL list of meters (not pre-sliced)
        concurrency: Number of concurrent workers
        state: JobState instance for checkpoint management
        attempt_number: Current attempt number (for retries)
        skip_count: Number of meters to skip from start (0 = process all)
    """
    
    # Ensure cancellation lock exists
    if job_id not in cancellation_locks:
        cancellation_locks[job_id] = threading.Lock()

    # ==================== VALIDATE AND APPLY SKIP ====================
    original_total = len(meters_list)
    
    # ⭐ DEBUG LOGGING
    LOG.info(f"[{job_id}] ⭐⭐⭐ run_job_async called with:")
    LOG.info(f"[{job_id}]   - meters_list length: {len(meters_list)}")
    LOG.info(f"[{job_id}]   - skip_count parameter: {skip_count}")
    LOG.info(f"[{job_id}]   - attempt_number: {attempt_number}")
    
    if original_total == 0:
        LOG.error(f"[{job_id}] Meters list is empty!")
        await broadcast(job_id, {"type": "error", "msg": "❌ No meters to process"})
        jobs[job_id]["status"] = "failed"
        return "failed"
    
    # Normalize skip_count to valid range [0, total-1]
    skip_count = max(0, min(skip_count, original_total - 1))
    
    LOG.info(f"[{job_id}] Normalized skip_count: {skip_count} (was: {skip_count})")
    
    # Create the working meters list (sliced if skip > 0)
    if skip_count > 0:
        meters_to_process_list = meters_list[skip_count:]
        LOG.info(f"[{job_id}] 🚀 SKIP ACTIVE: Skipping first {skip_count} meters")
        LOG.info(f"[{job_id}] Original total: {original_total}, Processing from index {skip_count}")
        LOG.info(f"[{job_id}] Meters remaining to process: {len(meters_to_process_list)}")
        
        await broadcast(job_id, {
            "type": "info",
            "msg": f"🚀 Skipping first {skip_count} meters, starting from meter #{skip_count + 1}/{original_total}"
        })
    else:
        meters_to_process_list = meters_list
        LOG.info(f"[{job_id}] Processing all {original_total} meters (no skip)")
    
    remaining_count = len(meters_to_process_list)
    # ==============================================================

    # Initialize job state with proper counts
    jobs[job_id] = {
        "status": "running",
        "cancel_requested": False,
        "is_stopping": False,
        "progress": int((skip_count / original_total) * 100),
        "total": original_total,  # Always use original total
        "log": [],
        "attempt": attempt_number,
        "processed_meters": [],
        "processed": skip_count,  # Start from skip count
        "success": 0,
        "failed": 0,
        "unfound": 0,
        "active_workers": 0,
        "skipped_count": skip_count,
        "meters_remaining": remaining_count,
    }

    # Initialize worker pool
    worker_pool = WorkerPool(job_id, size=concurrency)
    await worker_pool.initialize()

    # Results tracked in-memory by meter
    results_map: Dict[str, Dict[str, Any]] = {}
    unfound_meters: Dict[str, Dict[str, Any]] = {}
    processed_meters_set: Set[str] = set()

    async def save_checkpoint_safe():
        """Safe checkpoint save: persist current results_map and processed set"""
        try:
            recs = [dict(v) for v in results_map.values()]
            for r in recs:
                r.pop("_status", None)

            unfound_recs = [dict(v) for v in unfound_meters.values()]
            all_recs = recs + unfound_recs

            await save_final_results(all_recs, output_path, job_id)
            # Save checkpoint with absolute index (processed count includes skip)
            state.save_checkpoint(list(processed_meters_set), len(processed_meters_set) + skip_count)
            return True
        except Exception as e:
            LOG.warning(f"[{job_id}] Checkpoint save failed: {e}")
            return False

    try:
        total = original_total  # Always use original total for progress calculations
        
        if remaining_count == 0:
            await broadcast(job_id, {"type": "error", "msg": "❌ No meters remaining to process after skip"})
            jobs[job_id]["status"] = "finished"
            return "success"

        LOG.info(f"[{job_id}] Starting processing: {remaining_count} meters with {concurrency} workers (total: {total}, skipped: {skip_count})")

        # Load checkpoint / existing results
        checkpoint = state.load_checkpoint()
        success_count = 0
        failed_count = 0
        unfound_count = 0

        UNFOUND_PHRASES = [
            "not with bg", "not with british gas", "no site details",
            "no site details found", "not found", "no data",
            "meter not registered", "meter could not be found",
            "future contract already agreed"
        ]

        def is_unfound_error(err_text: str) -> bool:
            if not err_text:
                return False
            err_l = err_text.lower()
            return any(phrase in err_l for phrase in UNFOUND_PHRASES)

        # Load existing results if output exists
        if Path(output_path).exists():
            try:
                LOG.info(f"[{job_id}] Loading existing results from {output_path}...")
                existing_df = pd.read_excel(output_path, sheet_name="Combined", engine="openpyxl", dtype=str)
                existing_results = existing_df.to_dict('records')

                for rec in existing_results:
                    # Clean up unwanted fields
                    rec.pop('_contract_start', None)
                    rec.pop('_contract_start_parsed', None)
                    rec.pop('Contract_Start_Year', None)
                    rec.pop('Unnamed: 0', None)
                    rec.pop('Status', None)  # Remove status column from Combined sheet

                    meter_id = rec.get("meter") or rec.get("Meter Number")
                    if not meter_id:
                        continue

                    # Check if unfound based on address fields
                    address_fields = [
                        "Site Building Name", "Site Street No", "Site Street 1",
                        "Site Street 2", "Site Town", "Site Postcode"
                    ]
                    all_empty = all(
                        not rec.get(field) or str(rec.get(field)).strip() in ['', 'None', 'nan']
                        for field in address_fields
                    )
                    err = str(rec.get("error") or "")

                    if all_empty or is_unfound_error(err):
                        status = "unfound"
                        unfound_count += 1
                        unfound_meters[meter_id] = {
                            "meter": meter_id,
                            "kind": rec.get("kind", detect_meter_type(meter_id)),
                            "error": err or "Meter not found or not with British Gas",
                            "Transportation AQ": rec.get("Transportation AQ") or rec.get("Annualised AQ")
                        }
                    elif rec.get("error") and str(rec.get("error")).strip() not in ['', 'None', 'nan']:
                        status = "failed"
                        failed_count += 1
                        results_map[meter_id] = {**rec, "_status": "failed"}
                    else:
                        status = "success"
                        success_count += 1
                        results_map[meter_id] = {**rec, "_status": "success"}
                    
                    processed_meters_set.add(meter_id)

                jobs[job_id]['success'] = success_count
                jobs[job_id]['failed'] = failed_count
                jobs[job_id]['unfound'] = unfound_count

                LOG.info(f"[{job_id}] Loaded {len(existing_results)} existing results: "
                        f"✅ {success_count}, ❌ {failed_count}, ⚠️ {unfound_count}")
                        
                await broadcast(job_id, {
                    "type": "info",
                    "msg": f"📂 Loaded {len(existing_results)} existing rows (✅ {success_count}, ❌ {failed_count}, ⚠️ {unfound_count})"
                })
            except Exception as e:
                LOG.warning(f"[{job_id}] Could not load existing results: {e}")
                results_map = {}
                success_count = failed_count = unfound_count = 0

        start_time = time.time()
        # Processed count includes skip + already processed
        processed = skip_count + len(processed_meters_set)

        # Initialize all workers
        await broadcast(job_id, {"type": "status", "msg": f"🚀 Launching {concurrency} browser workers..."})
        
        template_url = None
        initialization_tasks = []
        
        for worker in worker_pool.workers:
            initialization_tasks.append(initialize_worker(worker, job_id))
        
        # Wait for all workers to initialize
        init_results = await asyncio.gather(*initialization_tasks, return_exceptions=True)
        
        # Check for failures
        failed_workers = [i for i, r in enumerate(init_results) if isinstance(r, Exception)]
        if len(failed_workers) == len(init_results):
            await broadcast(job_id, {"type": "error", "msg": "❌ All workers failed to initialize"})
            await worker_pool.cleanup_all()
            jobs[job_id]["status"] = "failed"
            return "failed"
        
        # Get template URL from first successful worker
        for result in init_results:
            if isinstance(result, str) and result.startswith("http"):
                template_url = result
                break
        
        if not template_url:
            await broadcast(job_id, {"type": "error", "msg": "❌ Failed to get template URL"})
            await worker_pool.cleanup_all()
            jobs[job_id]["status"] = "failed"
            return "failed"
        
        jobs[job_id]['template_url'] = template_url
        await save_job_meta(job_id)
        
        successful_workers = len([r for r in init_results if not isinstance(r, Exception)])
        await broadcast(job_id, {
            "type": "success", 
            "msg": f"✅ {successful_workers}/{concurrency} workers ready!" + 
                   (f" (Starting from meter #{skip_count + 1})" if skip_count > 0 else "")
        })

        if should_cancel(job_id):
            raise asyncio.CancelledError()

        # Create queue of meters to process (using sliced list with original indices)
        meters_to_process = [
            (idx + skip_count, meter)  # idx is position in sliced list, add skip_count for original index
            for idx, meter in enumerate(meters_to_process_list)
            if meter not in processed_meters_set
        ]
        
        total_to_process = len(meters_to_process)
        LOG.info(f"[{job_id}] {total_to_process} meters remaining to process (after skip and existing results)")

        if total_to_process == 0:
            LOG.info(f"[{job_id}] All meters already processed, finishing...")
            await broadcast(job_id, {"type": "success", "msg": "✅ All meters already processed!"})
            
            # Save final results and cleanup
            final_records = []
            for m, rec in results_map.items():
                r = dict(rec)
                r.pop("_status", None)
                final_records.append(r)
            for m, rec in unfound_meters.items():
                final_records.append(dict(rec))

            elec_count, gas_count, saved_success, saved_failed, saved_unfound = await save_final_results(
                final_records, output_path, job_id
            )
            
            await worker_pool.cleanup_all()
            jobs[job_id]["status"] = "finished"
            jobs[job_id]["progress"] = 100
            
            return "success"

        # Semaphore to limit concurrent tasks
        semaphore = asyncio.Semaphore(concurrency)
        
        async def process_meter_with_worker(original_idx: int, meter: str):
            """
            Process single meter using an available worker
            
            Args:
                original_idx: Index in the ORIGINAL full meters list (already includes skip offset)
                meter: Meter number to process
            """
            async with semaphore:
                if should_cancel(job_id):
                    return None
                
                worker = None
                try:
                    # Get available worker
                    worker = await worker_pool.wait_for_available_worker(timeout=300)
                    worker.current_meter = meter
                    
                    # Update active workers count
                    active = sum(1 for w in worker_pool.workers if w.is_busy)
                    jobs[job_id]['active_workers'] = active
                    
                    # Ensure worker browser is healthy
                    worker = await worker_pool.ensure_worker_browser_alive(worker, template_url)
                    
                    # Process meter with connection recovery
                    max_connection_retries = 3
                    result = None
                    
                    for conn_attempt in range(max_connection_retries):
                        try:
                            # Check browser health before processing
                            try:
                                await asyncio.wait_for(worker.page.evaluate("() => true"), timeout=2.0)
                            except Exception:
                                worker = await worker_pool.ensure_worker_browser_alive(worker, template_url)
                            
                            if should_cancel(job_id):
                                raise asyncio.CancelledError()
                            
                            # Process the meter
                            result = await process_single_meter_with_retry(
                                worker.page, 
                                meter, 
                                job_id,
                                is_first_meter=False,
                                template_url=template_url
                            )
                            
                            worker.processed_count += 1
                            break
                            
                        except asyncio.CancelledError:
                            raise
                        except Exception as e:
                            error_msg = str(e).lower()
                            is_connection_error = any(phrase in error_msg for phrase in [
                                "target closed", "connection closed", "disconnected",
                                "navigation failed", "context closed", "browser closed"
                            ])
                            
                            if is_connection_error and conn_attempt < max_connection_retries - 1:
                                LOG.warning(f"[{job_id}] Worker {worker.worker_id} connection error for {meter} (attempt {conn_attempt + 1}): {e}")
                                await exponential_backoff(conn_attempt)
                                worker = await worker_pool.ensure_worker_browser_alive(worker, template_url)
                                continue
                            else:
                                result = {
                                    "meter": meter,
                                    "kind": detect_meter_type(meter),
                                    "error": f"Error after {conn_attempt + 1} attempts: {str(e)}"
                                }
                                break
                    
                    if not result:
                        result = {
                            "meter": meter,
                            "kind": detect_meter_type(meter),
                            "skip_meter": True,
                            "unfound": True,
                            "error": "Unknown processing failure"
                        }
                    
                    return (original_idx, meter, result)
                    
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    LOG.error(f"[{job_id}] Worker processing error for {meter}: {e}")
                    return (original_idx, meter, {
                        "meter": meter,
                        "kind": detect_meter_type(meter),
                        "error": str(e),
                        "skip_meter": True,
                        "unfound": True
                    })
                finally:
                    if worker:
                        worker_pool.release_worker(worker)
                        active = sum(1 for w in worker_pool.workers if w.is_busy)
                        jobs[job_id]['active_workers'] = active
        
        # Process all meters concurrently
        tasks = [
            process_meter_with_worker(original_idx, meter) 
            for original_idx, meter in meters_to_process
        ]
        
        completed = 0
        
        # Process with progress tracking
        for coro in asyncio.as_completed(tasks):
            if should_cancel(job_id):
                LOG.info(f"[{job_id}] Cancellation detected during processing")
                break
            
            try:
                result = await coro
                if result is None:
                    continue
                
                original_idx, meter, meter_result = result
                
                # Helper: has valid data?
                def has_valid_data(res):
                    address_fields = [
                        "Site Building Name", "Site Street No", "Site Street 1",
                        "Site Street 2", "Site Town", "Site Postcode"
                    ]
                    has_address = any(
                        res.get(field) and str(res.get(field)).strip() not in ['', 'None', 'nan']
                        for field in address_fields
                    )
                    consumption_fields = [
                        "Electricity Consumption (kWh)", "Gas Consumption (kWh)"
                    ]
                    has_consumption = any(
                        res.get(field) and str(res.get(field)).strip() not in ['', 'None', 'nan']
                        for field in consumption_fields
                    )
                    return has_address or has_consumption

                # Determine status
                def determine_status(res):
                    if res.get("skip_meter") or res.get("unfound"):
                        return "unfound"
                    if has_valid_data(res):
                        return "success"
                    err_text = str(res.get("error") or "")
                    if is_unfound_error(err_text) or err_text.strip() == "":
                        return "unfound"
                    return "failed"

                new_status = determine_status(meter_result)
                old_entry = results_map.get(meter)
                old_status = old_entry.get("_status") if old_entry else None

                # Handle unfound separately
                if new_status == "unfound":
                    if old_status == "success":
                        success_count = max(0, success_count - 1)
                        results_map.pop(meter, None)
                    elif old_status == "failed":
                        failed_count = max(0, failed_count - 1)
                        results_map.pop(meter, None)

                    unfound_count += 1
                    unfound_meters[meter] = {
                        "meter": meter,
                        "kind": detect_meter_type(meter),
                        "error": meter_result.get("error", "Meter not found or not with British Gas"),
                        "Transportation AQ": meter_result.get("Transportation AQ") if detect_meter_type(meter) == "Gas" else None
                    }

                else:
                    # Normalize selectors
                    for k in SELECTOR_MAP.keys():
                        if k not in meter_result:
                            meter_result[k] = None

                    meter_result.setdefault("meter", meter)
                    meter_result.setdefault("kind", detect_meter_type(meter))

                    if new_status == "success":
                        if old_status == "success":
                            results_map[meter] = {**meter_result, "_status": "success"}
                        else:
                            if old_status == "failed":
                                failed_count = max(0, failed_count - 1)
                            elif old_status == "unfound":
                                unfound_count = max(0, unfound_count - 1)
                            success_count += 1
                            results_map[meter] = {**meter_result, "_status": "success"}

                    elif new_status == "failed":
                        if old_status == "success":
                            success_count = max(0, success_count - 1)
                        elif old_status == "unfound":
                            unfound_count = max(0, unfound_count - 1)
                        if old_status != "failed":
                            failed_count += 1
                        results_map[meter] = {**meter_result, "_status": "failed"}

                completed += 1
                processed += 1
                processed_meters_set.add(meter)

                jobs[job_id]['processed_meters'] = list(processed_meters_set)
                jobs[job_id]['processed'] = processed
                jobs[job_id]['success'] = success_count
                jobs[job_id]['failed'] = failed_count
                jobs[job_id]['unfound'] = unfound_count

                # Calculate stats
                elapsed = max(0.0001, time.time() - start_time)
                denom = max(1, completed)
                avg_time_seconds = elapsed / denom
                remaining = total_to_process - completed
                eta_seconds = int(avg_time_seconds * remaining)
                progress = int((processed / total) * 100)  # Use original total
                jobs[job_id]["progress"] = progress
                
                active_workers = sum(1 for w in worker_pool.workers if w.is_busy)

                # Save to DB
                stored_rec = results_map.get(meter)
                if stored_rec and stored_rec.get("_status") in ("success", "failed"):
                    rec_for_db = dict(stored_rec)
                    rec_for_db.pop("_status", None)
                    try:
                        save_result_db(job_id, rec_for_db)
                    except Exception:
                        LOG.debug(f"[{job_id}] save_result_db failed for meter {meter}", exc_info=True)

                # Broadcast progress
                broadcast_row = dict(stored_rec) if stored_rec else None
                if broadcast_row:
                    broadcast_row.pop("_status", None)

                await broadcast(job_id, {
                    "type": "progress",
                    "msg": f"✅ {completed}/{total_to_process} remaining (🤖 {active_workers} active) | Total: {processed}/{total} ({progress}%)",
                    "progress": progress,
                    "row": broadcast_row,
                    "meta": {
                        "total": total,
                        "processed": processed,
                        "skipped": skip_count,
                        "success": success_count,
                        "failed": failed_count,
                        "unfound": unfound_count,
                        "remaining": remaining,
                        "completed_this_run": completed,
                        "total_this_run": total_to_process,
                        "avg_time_seconds": round(avg_time_seconds, 3),
                        "eta_seconds": eta_seconds,
                        "attempt": attempt_number,
                        "active_workers": active_workers
                    }
                })

                # Periodic checkpoint
                if completed % (INCREMENTAL_SAVE_EVERY * concurrency) == 0:
                    try:
                        # Save session from first available worker
                        for worker in worker_pool.workers:
                            if worker.context and worker.page:
                                try:
                                    await _save_session_to_meta(job_id, worker.context, worker.page)
                                    break
                                except:
                                    pass
                    except:
                        pass
                    await save_checkpoint_safe()
            
            except asyncio.CancelledError:
                LOG.info(f"[{job_id}] Task completion cancelled")
                raise
            except Exception as e:
                LOG.error(f"Task completion error: {e}", exc_info=True)

        # Final save and finish
        LOG.info(f"[{job_id}] All meters processed, saving final results...")
        
        final_records = []
        for m, rec in results_map.items():
            r = dict(rec)
            r.pop("_status", None)
            final_records.append(r)

        for m, rec in unfound_meters.items():
            final_records.append(dict(rec))

        elec_count, gas_count, saved_success, saved_failed, saved_unfound = await save_final_results(
            final_records, output_path, job_id
        )
        
        LOG.info(f"[{job_id}] Save operation returned - Success: {saved_success}, Failed: {saved_failed}, Unfound: {saved_unfound}")

        final_success = saved_success
        final_failed = saved_failed
        final_unfound = saved_unfound

        total_processed_for_rate = final_success + final_failed
        if total_processed_for_rate == 0:
            success_rate = 0.0
        else:
            success_rate = (final_success / total_processed_for_rate) * 100

        state.save_checkpoint(list(processed_meters_set), processed)

        # Save session before cleanup
        try:
            for worker in worker_pool.workers:
                if worker.context and worker.page:
                    try:
                        await _save_session_to_meta(job_id, worker.context, worker.page)
                        break
                    except:
                        pass
        except:
            pass

        # Cleanup worker pool
        await worker_pool.cleanup_all()

        jobs[job_id]["status"] = "finished"
        jobs[job_id]["progress"] = 100
        jobs[job_id]["success"] = final_success
        jobs[job_id]["failed"] = final_failed
        jobs[job_id]["unfound"] = final_unfound
        jobs[job_id]["active_workers"] = 0

        end_time = time.time()
        total_time_minutes = round((end_time - start_time) / 60.0, 2)

        await broadcast(job_id, {
            "type": "done",
            "msg": f"🎉 Job completed in {total_time_minutes} minutes with {concurrency} workers!",
            "output_file": str(output_path),
            "total_time": total_time_minutes,
            "meta": {
                "total": total,
                "processed": processed,
                "skipped": skip_count,
                "success": final_success,
                "failed": final_failed,
                "unfound": final_unfound,
                "success_rate_percent": round(success_rate, 2),
                "elec_exported": elec_count,
                "gas_exported": gas_count,
                "workers_used": concurrency
            }
        })

        return "success"

    except asyncio.CancelledError:
        LOG.info(f"Job {job_id} cancelled")
        jobs[job_id]["status"] = "cancelling"
        await broadcast(job_id, {"type": "status", "msg": "Cancellation detected - saving progress..."})

        await save_checkpoint_safe()
        await worker_pool.cleanup_all()

        jobs[job_id]["status"] = "cancelled"
        jobs[job_id]["active_workers"] = 0
        end_time = time.time()
        total_time_minutes = round((end_time - start_time) / 60.0, 2)

        await broadcast(job_id, {
            "type": "done",
            "msg": f"Job cancelled - Progress saved. Resume by restarting.",
            "output_file": str(output_path),
            "total_time": total_time_minutes,
            "meta": {
                "total": total,
                "processed": processed,
                "skipped": skip_count,
                "success": success_count,
                "failed": failed_count,
                "unfound": unfound_count,
                "can_resume": True
            }
        })
        return "cancelled"

    except Exception as e:
        err_msg = str(e) or ""
        cancelled_indicators = [
            "Target page, context or browser has been closed",
            "Browser has been closed",
            "Context closed",
            "Navigation failed because the browser has disconnected"
        ]

        if jobs.get(job_id, {}).get('cancel_requested') or any(ind in err_msg for ind in cancelled_indicators):
            LOG.info(f"Job {job_id} encountered closure-related error -> treating as cancelled")
            await save_checkpoint_safe()
            await worker_pool.cleanup_all()
            jobs[job_id]["status"] = "cancelled"
            jobs[job_id]["active_workers"] = 0
            await broadcast(job_id, {
                "type": "done",
                "msg": "Job cancelled - Progress saved.",
                "can_resume": True
            })
            return "cancelled"

        LOG.error(f"Unexpected error: {e}", exc_info=True)
        await broadcast(job_id, {"type": "error", "msg": f"Unexpected error: {err_msg}"})
        await save_checkpoint_safe()
        await worker_pool.cleanup_all()
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["active_workers"] = 0
        return "failed"


async def run_job_async_with_retry(job_id: str, input_path: str, output_path: str, 
                                    meters_list: List[str], concurrency: int = CONCURRENT_WORKERS,
                                    skip_meters: int = 0):  # ← ADD skip_meters parameter with default
    """Main job runner with retry logic - FIXED skip handling"""
    state = JobState(job_id)
    checkpoint = state.load_checkpoint()
    attempt_number = 1
    actual_skip = 0  # ← Initialize here
    
    # Determine starting point - checkpoint takes priority over skip
    if checkpoint:
        # RESUME MODE: Continue from checkpoint (ignore skip parameter on resume)
        resume_index = checkpoint.get('last_index', 0)
        attempt_number = checkpoint.get("attempt_number", 1) + 1
        actual_skip = resume_index  # ← Use checkpoint index
        
        await broadcast(job_id, {
            "type": "status", 
            "msg": f"🔄 Resuming from meter {resume_index + 1}/{len(meters_list)}"
        })
        
        LOG.info(f"[{job_id}] Resume mode: Starting from index {resume_index}")
        
    else:
        # FRESH START: Apply skip parameter
        actual_skip = max(0, min(skip_meters, len(meters_list) - 1)) if len(meters_list) > 0 else 0
        
        if actual_skip > 0:
            await broadcast(job_id, {
                "type": "info", 
                "msg": f"🚀 Starting with {actual_skip} meter skip"
            })
            LOG.info(f"[{job_id}] Fresh start with skip: Skipping first {actual_skip} meters")
    
    for attempt in range(attempt_number, MAX_JOB_RETRIES + 1):
        try:
            await broadcast(job_id, {
                "type": "status", 
                "msg": f"🚀 Attempt {attempt}/{MAX_JOB_RETRIES} with {concurrency} workers"
            })
            
            save_job_attempt(job_id, attempt, "started")

            # CRITICAL: Pass full meters_list and actual_skip (not skip_meters)
            result = await run_job_async(
                job_id=job_id,
                input_path=input_path,
                output_path=output_path,
                meters_list=meters_list,  # Full list
                concurrency=concurrency,
                state=state,
                attempt_number=attempt,
                skip_count=actual_skip  # ← Use calculated actual_skip
            )

            if result == "success":
                processed = jobs.get(job_id, {}).get("processed", 0)
                total = jobs.get(job_id, {}).get("total", 0)
                save_job_attempt(job_id, attempt, "completed", meters_processed=processed)

                if processed >= total:
                    state.clear()
                return

            if result == "cancelled":
                save_job_attempt(job_id, attempt, "cancelled", meters_processed=jobs.get(job_id, {}).get("processed", 0))
                return

            if result == "failed":
                save_job_attempt(job_id, attempt, "failed", meters_processed=jobs.get(job_id, {}).get("processed", 0))

        except asyncio.CancelledError:
            save_job_attempt(job_id, attempt, "cancelled", meters_processed=jobs.get(job_id, {}).get("processed", 0))
            raise

        except Exception as e:
            error_msg = str(e)
            LOG.error(f"Attempt {attempt} failed: {error_msg}", exc_info=True)
            save_job_attempt(job_id, attempt, "failed", error_message=error_msg)

            if attempt < MAX_JOB_RETRIES:
                await broadcast(job_id, {
                    "type": "warning",
                    "msg": f"⚠️ Retry in {RETRY_DELAY_SECONDS}s... ({MAX_JOB_RETRIES - attempt} left)"
                })
                await asyncio.sleep(RETRY_DELAY_SECONDS)
                continue
            else:
                jobs.setdefault(job_id, {})["status"] = "failed"
                await broadcast(job_id, {
                    "type": "error",
                    "msg": f"❌ Job failed after {MAX_JOB_RETRIES} attempts"
                })
                return
            

# ==================== FIXED UNFOUND DETECTION ====================

def is_unfound_meter(row: Dict[str, Any]) -> bool:
    """
    Unified unfound detection logic.
    A meter is unfound if:
    1. Has explicit not_found flag, OR
    2. Has unfound error message, OR  
    3. Missing ALL critical address fields (Building Name, Street 1, Town, Postcode)
    
    BUT: Gas meters with valid AQ are still included in export (just marked unfound)
    """
    # Check explicit flag
    if row.get("not_found") is True:
        return True
    
    # Check for unfound error messages
    UNFOUND_PHRASES = [
        "not with bg", "not with british gas", "no site details",
        "no site details found", "not found", "no data",
        "meter not registered", "meter could not be found",
        "future contract already agreed"
    ]
    
    error_text = str(row.get("error") or "").lower()
    if error_text and any(phrase in error_text for phrase in UNFOUND_PHRASES):
        return True
    
    # Check if missing ALL critical address fields
    critical_address_fields = [
        "Site Building Name",
        "Site Street 1", 
        "Site Town",
        "Site Postcode"
    ]
    
    has_any_critical_address = any(
        row.get(field) and str(row.get(field)).strip() not in ['', 'None', 'none', 'nan']
        for field in critical_address_fields
    )
    
    return not has_any_critical_address


# ==================== FIXED SAVE RESULTS ====================

async def save_final_results(results: List[Dict[str, Any]], output_path: str, job_id: str):
    """
    Save results with FIXED unfound handling and PROPER EMPTY VALUE HANDLING.
    - Never replace empty usage/AQ with meter numbers
    - Create separate sheets for different meter types
    - Keep unfound meters for export
    """
    try:
        ordered_columns = ["meter", "kind"] + list(SELECTOR_MAP.keys()) + ["error"]

        # Deduplicate by meter
        seen_meters: Dict[str, Dict[str, Any]] = {}
        for rec in results:
            meter_id = rec.get("meter")
            if meter_id:
                seen_meters[meter_id] = rec
        unique_results = list(seen_meters.values())

        df_all = pd.DataFrame(unique_results)

        # Ensure required columns exist
        for c in ordered_columns:
            if c not in df_all.columns:
                df_all[c] = None

        # ✅ FIXED: Determine status using unified logic
        statuses = []
        for _, row in df_all.iterrows():
            row_dict = row.to_dict()
            
            if is_unfound_meter(row_dict):
                statuses.append("unfound")
            else:
                # Has address data - check for other errors
                err = str(row_dict.get("error") or "").strip()
                has_error = err and err.lower() not in ['none', '', 'nan']
                
                if has_error:
                    statuses.append("failed")
                else:
                    statuses.append("success")
                    
        df_all["status"] = statuses

        # ✅ FIXED: Export success AND unfound (exclude only failed)
        df_for_export = df_all[df_all["status"].isin(["success", "unfound"])].copy()
        
        LOG.info(f"[{job_id}] Export breakdown: "
                 f"success={len(df_all[df_all['status']=='success'])}, "
                 f"failed={len(df_all[df_all['status']=='failed'])}, "
                 f"unfound={len(df_all[df_all['status']=='unfound'])}")

        # Build unified output (12 columns + Status)
        df_final = pd.DataFrame()
        
        # Columns 1-6: Address fields (keep empty if no data)
        df_final["Site Building Name"] = df_for_export.get("Site Building Name", "").fillna("").astype(str).replace("nan", "")
        df_final["Site Street No"] = df_for_export.get("Site Street No", "").fillna("").astype(str).replace("nan", "")
        df_final["Site Street 1"] = df_for_export.get("Site Street 1", "").fillna("").astype(str).replace("nan", "")
        df_final["Site Street 2"] = df_for_export.get("Site Street 2", "").fillna("").astype(str).replace("nan", "")
        df_final["Site Town"] = df_for_export.get("Site Town", "").fillna("").astype(str).replace("nan", "")
        df_final["Site Postcode"] = df_for_export.get("Site Postcode", "").fillna("").astype(str).replace("nan", "")
        
        # Column 7: MPAN Topline (Electricity only)
        def get_mpan(row):
            if row.get("kind") == "Electricity":
                val = row.get("MPAN Topline", "")
                if val and str(val).strip() and str(val).lower() not in ["nan", "none", ""]:
                    return str(val).strip()
            return ""
        
        df_final["MPAN Topline"] = df_for_export.apply(get_mpan, axis=1)
        
        # Column 8: Meter Number (KEEP AS IS - never replace!)
        df_final["Meter Number"] = df_for_export.get("meter", "").fillna("").astype(str).replace("nan", "")
        
        # ✅ FIXED: Column 9: Usage (consumption, LEAVE EMPTY IF NO DATA!)
        def get_usage(row):
            """Extract usage WITHOUT replacing empty with meter number"""
            if row.get("kind") == "Electricity":
                val = row.get("Electricity Consumption (kWh)", "")
            else:
                val = row.get("Gas Consumption (kWh)", "")
            
            # Return empty if no valid data
            if not val or str(val).strip() in ["nan", "none", "", "None"]:
                return ""
            
            return str(val).strip()
        
        df_final["Usage"] = df_for_export.apply(get_usage, axis=1)
        
        # Column 10: Proposed Start Date
        def get_start_date(row):
            if row.get("kind") == "Electricity":
                val = row.get("Proposed Start Date (Elec)", "")
            else:
                val = row.get("Proposed Start Date (Gas)", "")
            
            if not val or str(val).strip() in ["nan", "none", "", "None"]:
                return ""
            
            return str(val).strip()
        
        df_final["Proposed Start Date"] = df_for_export.apply(get_start_date, axis=1)
        
        # Column 11: Energization Status (Electricity only, LEAVE EMPTY IF NO DATA!)
        def get_energization(row):
            if row.get("kind") == "Electricity":
                val = row.get("Energization Status", "")
                if val and str(val).strip() and str(val).lower() not in ["nan", "none", ""]:
                    return str(val).strip()
            return ""
        
        df_final["Energization Status"] = df_for_export.apply(get_energization, axis=1)
        
        # ✅ FIXED: Column 12: Annualised AQ (Gas only, LEAVE EMPTY IF NO DATA!)
        def filter_aq_final(row):
            """Include AQ for Gas meters only if value exists and is numeric - NEVER replace with meter number!"""
            if row.get("kind") != "Gas":
                return ""
            
            val = row.get("Transportation AQ") or row.get("Annualised AQ")
            
            # Return empty if no valid data
            if pd.isna(val) or val == "" or val == "None" or val is None:
                return ""
            
            val_str = str(val).strip()
            
            # Skip if it's just "nan" or "none"
            if val_str.lower() in ["nan", "none", ""]:
                return ""
            
            try:
                aq_numeric = float(val_str.replace(",", ""))
                # Return the original formatted value (with commas if present)
                return val_str
            except (ValueError, TypeError):
                # Invalid numeric value - leave empty
                return ""

        df_final["Annualised AQ"] = df_for_export.apply(filter_aq_final, axis=1)
        
        # Status column for reference
        df_final["Status"] = df_for_export["status"].apply(lambda s: s.capitalize())

        # Save to Excel with MULTIPLE SHEETS
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = Path(output_path)
        
        with pd.ExcelWriter(str(output_path), engine="openpyxl") as writer:
            # Sheet 1: Combined (all records)
            df_final.to_excel(writer, sheet_name="Combined", index=False)
            
            # Sheet 2: Electricity only
            df_electricity = df_final[
                (df_for_export["kind"] == "Electricity") | 
                (df_final["MPAN Topline"] != "")
            ].copy()
            df_electricity.to_excel(writer, sheet_name="Electricity", index=False)
            
            # Sheet 3: Gas only
            df_gas = df_final[
                (df_for_export["kind"] == "Gas") | 
                (df_final["Annualised AQ"] != "")
            ].copy()
            df_gas.to_excel(writer, sheet_name="Gas", index=False)
            
            # Sheet 4: Unfound only
            df_unfound = df_final[df_for_export["status"] == "unfound"].copy()
            df_unfound.to_excel(writer, sheet_name="Unfound", index=False)

        LOG.info(f"[{job_id}] Excel file saved with 4 sheets: Combined, Electricity, Gas, Unfound")

        # Summary counts
        success_count = int((df_for_export["status"] == "success").sum())
        unfound_count = int((df_for_export["status"] == "unfound").sum())
        failed_count = int((df_all["status"] == "failed").sum())
        
        # Count by type (only count non-blank key fields)
        elec_count = len(df_electricity)
        gas_count = len(df_gas)
        
        # Count unfound gas with AQ
        unfound_gas_with_aq = int(
            ((df_for_export["status"] == "unfound") & 
             (df_final["Annualised AQ"] != "")).sum()
        )

        await broadcast(job_id, {
            "type": "status",
            "msg": f"💾 Results saved: {len(df_final)} total records — "
                   f"✅ {success_count} found, "
                   f"⚠️ {unfound_count} unfound ({unfound_gas_with_aq} gas with AQ), "
                   f"❌ {failed_count} failed — "
                   f"⚡ {elec_count} electricity, 🔥 {gas_count} gas"
        })

        return elec_count, gas_count, success_count, failed_count, unfound_count

    except Exception as e:
        LOG.warning(f"Save results failed: {e}", exc_info=True)
        return 0, 0, 0, 0, 0



# ==================== API ENDPOINTS ====================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    template_path = TEMPLATE_DIR / "dashboard.html"
    with open(template_path, "r", encoding="utf-8") as f:
        content = f.read()
    return HTMLResponse(content=content)



def extract_meters_from_dataframe(df: pd.DataFrame, filename: str = "") -> List[str]:
    """
    Extract meter numbers from a DataFrame using multiple strategies.
    Returns a list of meter strings found in this specific DataFrame.
    """
    meters = []
    
    # Strategy 1: Look for MPAN/MPRN specific column (exact match priority)
    exact_match_columns = ["MPAN/MPRN", "MPAN / MPRN", "Mpan/Mprn", "Mpan / Mpr", "mpxn", "MPXN"]
    
    for exact_col in exact_match_columns:
        if exact_col in df.columns:
            LOG.info(f"  Found exact match column: '{exact_col}'")
            try:
                col_meters = df[exact_col].astype(str).str.strip().tolist()
                col_meters = [m for m in col_meters if m and m.lower() not in ['nan', 'none', '', 'null']]
                meters.extend(col_meters)
                LOG.info(f"  ✅ Extracted {len(col_meters)} meters from '{exact_col}'")
                return meters  # Return immediately if exact column found
            except Exception as e:
                LOG.warning(f"  Failed to extract from '{exact_col}': {e}")
    
    # Strategy 2: Look for meter-specific columns (fuzzy match)
    meter_keywords = ["meter", "mpan", "mprn", "MPAN/MPR"]  # Removed generic "number", "ref", "id"
    meter_columns = []
    
    for col in df.columns:
        col_lower = str(col).lower().strip()
        # Exact keyword match or contains mpan/mprn
        if col_lower in meter_keywords or any(kw in col_lower for kw in ["mpan", "mprn"]):
            meter_columns.append(col)
            LOG.debug(f"  Found potential meter column: '{col}'")
    
    # Extract from matching columns
    for meter_col in meter_columns:
        try:
            col_meters = df[meter_col].astype(str).str.strip().tolist()
            col_meters = [m for m in col_meters if m and m.lower() not in ['nan', 'none', '', 'null']]
            
            # Validate each meter looks like a number
            valid_meters = []
            for m in col_meters:
                if is_meter_number(m):
                    valid_meters.append(m)
                else:
                    LOG.debug(f"    Skipping invalid meter format: {m}")
            
            meters.extend(valid_meters)
            LOG.info(f"  Extracted {len(valid_meters)} valid meters from '{meter_col}'")
        except Exception as e:
            LOG.debug(f"  Failed to extract from column '{meter_col}': {e}")
    
    # Strategy 3: If no meter columns found, scan all cells
    if not meters:
        LOG.info(f"  No meter columns in {filename}, scanning all cells...")
        
        seen = set()
        
        # FIRST: Check column headers (they might be meter numbers!)
        for col in df.columns:
            if is_meter_number(col):
                meter_str = str(col).strip()
                if meter_str not in seen:
                    meters.append(meter_str)
                    seen.add(meter_str)
                    LOG.debug(f"    Found meter in header: {meter_str}")
        
        # THEN: Scan all cell values
        for col in df.columns:
            for val in df[col]:
                if is_meter_number(val):
                    meter_str = str(val).strip()
                    if meter_str not in seen:
                        meters.append(meter_str)
                        seen.add(meter_str)
    
    return meters


def extract_meters_from_excel_all_sheets(file_path: Path, filename: str = "") -> List[str]:
    """
    Extract meters from ALL sheets in an Excel file.
    Returns combined list of unique meters from all sheets.
    """
    all_meters = []
    
    try:
        # Get all sheet names
        xl_file = pd.ExcelFile(file_path, engine="openpyxl")
        sheet_names = xl_file.sheet_names
        
        LOG.info(f"📄 File '{filename}' contains {len(sheet_names)} sheet(s): {sheet_names}")
        
        # Process each sheet
        for sheet_name in sheet_names:
            try:
                LOG.info(f"  📊 Processing sheet: '{sheet_name}'")
                
                # Read sheet
                df_sheet = pd.read_excel(file_path, sheet_name=sheet_name, engine="openpyxl", dtype=str)
                df_sheet = df_sheet.fillna("")
                df_sheet = df_sheet.map(lambda x: str(x).strip() if x else "")
                
                LOG.info(f"     {len(df_sheet)} rows, {len(df_sheet.columns)} columns")
                
                # Extract meters from this sheet
                sheet_meters = extract_meters_from_dataframe(df_sheet, f"{filename}[{sheet_name}]")
                
                if sheet_meters:
                    LOG.info(f"     ✅ Found {len(sheet_meters)} meters in sheet '{sheet_name}'")
                    all_meters.extend(sheet_meters)
                else:
                    LOG.info(f"     ⚠️ No meters found in sheet '{sheet_name}'")
                    
            except Exception as e:
                LOG.warning(f"  ⚠️ Failed to read sheet '{sheet_name}': {e}")
                continue
        
        return all_meters
        
    except Exception as e:
        LOG.error(f"Failed to read Excel file '{filename}': {e}")
        return []
    
    
    
def is_meter_number(val) -> bool:
    """Check if value looks like a meter number (MPAN or MPRN)"""
    if pd.isna(val) or val in ['', 'nan', 'none', None]:
        return False
    
    val_str = str(val).strip()
    
    # Remove common text patterns that aren't meters
    if any(char.isalpha() for char in val_str):
        # Contains letters - probably not a pure meter number
        # Exception: scientific notation like "1.80004E+12"
        if 'e+' not in val_str.lower() and 'e-' not in val_str.lower():
            return False
    
    # Extract just digits
    digits = re.sub(r"\D", "", val_str)
    
    # MPAN = 13 digits, MPRN = 6-10 digits
    digit_count = len(digits)
    
    if digit_count == 13 or (6 <= digit_count <= 10):
        return True
    
    # Check for scientific notation (Excel format for large numbers)
    if 'e+' in val_str.lower():
        try:
            # Convert scientific notation to integer
            num = float(val_str)
            num_str = f"{num:.0f}"
            digits_from_sci = re.sub(r"\D", "", num_str)
            return len(digits_from_sci) == 13 or (6 <= len(digits_from_sci) <= 10)
        except:
            return False
    
    return False    


@app.post("/start")
async def start_job(
    files: List[UploadFile] = File(...), 
    skip_meters: int = 0,  # ← Optional from frontend (defaults to 0)
    selected_sheets: str = ""
):
    """Process multiple files with optional meter skip and sheet selection"""
    if not PORTAL_USER or not PORTAL_PASS:
        raise HTTPException(status_code=400, detail="Credentials not configured")

    job_id = str(uuid.uuid4())
    
    # ⭐ HARDCODED SKIP (Backend only)
    # Override frontend parameter with hardcoded value
    HARDCODED_SKIP_METERS = 0  # ← Set your skip value here
    ENABLE_HARDCODED_SKIP = False   # ← Toggle this to enable/disable
    
    # Use hardcoded skip if enabled, otherwise use frontend parameter
    actual_skip = HARDCODED_SKIP_METERS if ENABLE_HARDCODED_SKIP else skip_meters
    
    # ⭐ DEBUG LOGGING (after job_id is created)
    LOG.info(f"[{job_id}] /start endpoint called")
    LOG.info(f"[{job_id}] Frontend skip_meters parameter: {skip_meters}")
    LOG.info(f"[{job_id}] ENABLE_HARDCODED_SKIP: {ENABLE_HARDCODED_SKIP}")
    LOG.info(f"[{job_id}] HARDCODED_SKIP_METERS: {HARDCODED_SKIP_METERS}")
    LOG.info(f"[{job_id}] Actual skip to use: {actual_skip}")
    
    merged_input_path = UPLOAD_DIR / f"{job_id}_merged_input.xlsx"
    output_path = OUTPUT_DIR / f"{job_id}_output.xlsx"

    try:
        frames = []
        file_count = 0
        all_meters_from_files = []
        sheet_count = 0
        
        # Parse selected sheets BEFORE processing files
        sheets_to_process = None
        if selected_sheets and selected_sheets.strip() and selected_sheets.lower() != "all":
            sheets_to_process = [s.strip() for s in selected_sheets.split(",") if s.strip()]
            LOG.info(f"[{job_id}] Sheet filter active: {sheets_to_process}")
        else:
            LOG.info(f"[{job_id}] Processing ALL sheets (no filter applied)")
        
        LOG.info(f"[{job_id}] Processing {len(files)} uploaded file(s)...")
        
        for idx, file in enumerate(files):
            content = await file.read()
            tmp_path = UPLOAD_DIR / f"{job_id}_part_{idx}.xlsx"
            
            with open(tmp_path, "wb") as f:
                f.write(content)
            
            try:
                # Get all sheet names
                xl_file = pd.ExcelFile(tmp_path, engine="openpyxl")
                all_sheet_names = xl_file.sheet_names
                
                # Determine which sheets to process based on selection
                if sheets_to_process is None:
                    # No filter - process ALL sheets
                    target_sheets = all_sheet_names
                    LOG.info(f"  📄 {file.filename}: Processing ALL {len(all_sheet_names)} sheets")
                else:
                    # Filter applied - process ONLY selected sheets that exist in this file
                    target_sheets = [s for s in sheets_to_process if s in all_sheet_names]
                    skipped_sheets = [s for s in sheets_to_process if s not in all_sheet_names]
                    
                    if skipped_sheets:
                        LOG.warning(f"  ⚠️ {file.filename}: Sheets not found: {skipped_sheets}")
                    
                    if not target_sheets:
                        LOG.warning(f"  ⚠️ {file.filename}: None of the selected sheets exist in this file, skipping")
                        try:
                            tmp_path.unlink()
                        except:
                            pass
                        continue
                    
                    LOG.info(f"  📄 {file.filename}: Processing {len(target_sheets)}/{len(all_sheet_names)} selected sheets: {target_sheets}")
                
                sheet_count += len(target_sheets)
                
                # Process only the target sheets
                for sheet_name in target_sheets:
                    try:
                        LOG.info(f"    📊 Processing sheet: '{sheet_name}'")
                        
                        # Read sheet
                        df_sheet = pd.read_excel(tmp_path, sheet_name=sheet_name, engine="openpyxl", dtype=str)
                        df_sheet = df_sheet.fillna("")
                        df_sheet = df_sheet.map(lambda x: str(x).strip() if x else "")
                        
                        LOG.info(f"       {len(df_sheet)} rows, {len(df_sheet.columns)} columns")
                        
                        # Extract meters from this sheet
                        sheet_meters = extract_meters_from_dataframe(df_sheet, f"{file.filename}[{sheet_name}]")
                        
                        if sheet_meters:
                            LOG.info(f"       ✅ Found {len(sheet_meters)} meters")
                            all_meters_from_files.extend(sheet_meters)
                        else:
                            LOG.info(f"       ⚠️ No meters found")
                        
                        # Add to frames for merging
                        frames.append(df_sheet)
                        
                    except Exception as e:
                        LOG.warning(f"    ⚠️ Failed to read sheet '{sheet_name}': {e}")
                        continue
                
                file_count += 1
                
                try:
                    tmp_path.unlink()
                except:
                    pass
                    
            except Exception as e:
                LOG.warning(f"Failed to process file {file.filename}: {e}")
                continue

        if not frames:
            raise HTTPException(status_code=400, detail="No valid Excel sheets found to process")

        # Merge all dataframes
        LOG.info(f"[{job_id}] Merging {len(frames)} sheet(s) from {file_count} file(s)...")
        df_merged = pd.concat(frames, ignore_index=True, sort=False)
        df_merged = df_merged.fillna("")
        
        LOG.info(f"[{job_id}] Merged result: {len(df_merged)} total rows")
        
        # Save merged file
        df_merged.to_excel(merged_input_path, index=False, engine="openpyxl")

        # Deduplicate meters
        LOG.info(f"[{job_id}] Total meters collected before deduplication: {len(all_meters_from_files)}")
        
        unique_meters = []
        seen = set()
        duplicates_removed = 0
        
        for m in all_meters_from_files:
            if m and m not in seen:
                unique_meters.append(m)
                seen.add(m)
            elif m:
                duplicates_removed += 1
        
        meters = unique_meters
        total_meters = len(meters)
        
        LOG.info(f"[{job_id}] Removed {duplicates_removed} duplicate meters")
        LOG.info(f"[{job_id}] Final unique meter count: {total_meters} (from {sheet_count} sheet(s) in {file_count} file(s))")
        
        # ⭐ Validate and normalize actual_skip parameter
        actual_skip = max(0, min(actual_skip, total_meters - 1)) if total_meters > 0 else 0
        
        if actual_skip > 0:
            LOG.info(f"[{job_id}] ⚡ Skip parameter set: Will skip first {actual_skip} meters")
            LOG.info(f"[{job_id}] Processing will start from meter #{actual_skip + 1}")
        
        # Log meter type breakdown
        elec_meters = [m for m in meters if detect_meter_type(m) == "Electricity"]
        gas_meters = [m for m in meters if detect_meter_type(m) == "Gas"]
        LOG.info(f"[{job_id}] Meter breakdown: {len(elec_meters)} Electricity, {len(gas_meters)} Gas")

        if total_meters == 0:
            raise HTTPException(
                status_code=400, 
                detail=f"No meters found in {sheet_count} sheet(s) from {file_count} file(s)"
            )

        # Save meters list
        save_meters_list(job_id, meters)
        LOG.info(f"[{job_id}] Saved full meters list to persistent storage")

    except HTTPException:
        raise
    except Exception as e:
        LOG.exception(f"[{job_id}] Failed preparing input: {e}")
        raise HTTPException(status_code=500, detail=f"Failed preparing input: {str(e)}")

    # Initialize job state
    initial_progress = int((actual_skip / total_meters) * 100) if total_meters > 0 and actual_skip > 0 else 0
    
    jobs[job_id] = {
        "status": "queued",
        "progress": initial_progress,
        "total": total_meters,
        "log": [],
        "processed": actual_skip,
        "success": 0,
        "failed": 0,
        "unfound": 0,
        "files_uploaded": file_count,
        "sheets_processed": sheet_count,
        "skip_meters": actual_skip,  # ⭐ Use actual_skip here
        "skipped_count": actual_skip,
        "meters_remaining": total_meters - actual_skip,
        "selected_sheets": selected_sheets if sheets_to_process else "all"
    }
    await save_job_meta(job_id)
    
    ws_queues[job_id] = []

    LOG.info(f"[{job_id}] ⭐ Job initialized - Total: {total_meters}, Skip: {actual_skip}, Remaining: {total_meters - actual_skip}")

    # Start the job with actual_skip
    task = asyncio.create_task(
        run_job_async_with_retry(
            job_id, 
            str(merged_input_path), 
            str(output_path),
            meters_list=meters,
            concurrency=CONCURRENT_WORKERS,
            skip_meters=actual_skip  # ⭐ Pass actual_skip
        )
    )
    jobs[job_id]['task'] = task

    sheet_info = f" ({sheet_count} sheet(s))" if sheets_to_process else f" ({sheet_count} sheet(s), all)"
    skip_info = f" - Skipping first {actual_skip} meters" if actual_skip > 0 else ""
    
    return JSONResponse({
        "job_id": job_id, 
        "msg": f"Job started with {file_count} file(s){sheet_info}{skip_info}",
        "total_meters": total_meters,
        "skip_meters": actual_skip,  # ⭐ Return actual skip in response
        "meters_remaining": total_meters - actual_skip,
        "files_processed": file_count,
        "sheets_processed": sheet_count,
        "sheets_selected": sheets_to_process if sheets_to_process else "all",
        "meter_breakdown": {
            "electricity": len(elec_meters),
            "gas": len(gas_meters)
        }
    })
    
    
@app.post("/preview_sheets")
async def preview_sheets(files: List[UploadFile] = File(...)):
    """
    Preview available sheets in uploaded Excel files before starting job.
    Returns sheet names, row counts, and detected meters per sheet.
    """
    try:
        temp_job_id = str(uuid.uuid4())
        files_info = []
        
        for idx, file in enumerate(files):
            content = await file.read()
            tmp_path = UPLOAD_DIR / f"{temp_job_id}_preview_{idx}.xlsx"
            
            with open(tmp_path, "wb") as f:
                f.write(content)
            
            try:
                xl_file = pd.ExcelFile(tmp_path, engine="openpyxl")
                sheet_names = xl_file.sheet_names
                
                sheets_info = []
                
                for sheet_name in sheet_names:
                    try:
                        # Read sheet
                        df_sheet = pd.read_excel(tmp_path, sheet_name=sheet_name, engine="openpyxl", dtype=str)
                        df_sheet = df_sheet.fillna("")
                        
                        # Extract meters
                        sheet_meters = extract_meters_from_dataframe(df_sheet, f"{file.filename}[{sheet_name}]")
                        
                        # Get column preview (convert to strings to avoid datetime serialization issues)
                        columns = [str(col) for col in list(df_sheet.columns)[:10]]  # First 10 columns
                        
                        sheets_info.append({
                            "name": str(sheet_name),  # Ensure string
                            "rows": int(len(df_sheet)),
                            "columns": int(len(df_sheet.columns)),
                            "column_preview": columns,
                            "meters_found": int(len(sheet_meters)),
                            "sample_meters": [str(m) for m in sheet_meters[:5]] if sheet_meters else []
                        })
                        
                    except Exception as e:
                        sheets_info.append({
                            "name": str(sheet_name),
                            "error": f"Failed to read sheet: {str(e)}"
                        })
                
                files_info.append({
                    "filename": str(file.filename),
                    "sheets": sheets_info,
                    "total_sheets": int(len(sheet_names))
                })
                
                # Clean up temp file
                try:
                    tmp_path.unlink()
                except:
                    pass
                    
            except Exception as e:
                files_info.append({
                    "filename": str(file.filename),
                    "error": f"Failed to read file: {str(e)}"
                })
                try:
                    tmp_path.unlink()
                except:
                    pass
        
        # Use json.dumps with default serializer to handle any remaining non-serializable objects
        return JSONResponse(
            content=json.loads(json.dumps({
                "files": files_info,
                "total_files": len(files)
            }, default=str))
        )
        
    except Exception as e:
        LOG.error(f"Sheet preview failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to preview sheets: {str(e)}")  


# REPLACE the /resume endpoint to properly handle checkpoints (around line 2600)
@app.post("/resume/{job_id}")
async def resume_job(job_id: str):
    """Resume job from checkpoint - FIXED to start from actual checkpoint, not from 0"""
    try:
        LOG.info(f"[{job_id}] Resume request received")
        
        input_path = UPLOAD_DIR / f"{job_id}_merged_input.xlsx"
        output_path = OUTPUT_DIR / f"{job_id}_output.xlsx"

        # Check if input file exists
        if not input_path.exists():
            error_msg = f"Job input file not found: {input_path}"
            LOG.error(f"[{job_id}] {error_msg}")
            raise HTTPException(status_code=404, detail=error_msg)

        LOG.info(f"[{job_id}] Input file found: {input_path}")

        # Check if job is already running
        if job_id in jobs:
            existing = jobs[job_id]
            task = existing.get('task')
            if task and not task.done():
                status = existing.get('status')
                if status not in ['cancelled', 'failed', 'finished']:
                    error_msg = f"Job is already {status}"
                    LOG.warning(f"[{job_id}] {error_msg}")
                    raise HTTPException(status_code=400, detail=error_msg)

        # Clear any lingering flags
        if job_id in jobs:
            LOG.info(f"[{job_id}] Clearing lingering job flags")
            jobs[job_id].pop('cancel_requested', None)
            jobs[job_id].pop('is_stopping', None)
            jobs[job_id].pop('task', None)

        # Load state/checkpoint
        state = JobState(job_id)
        checkpoint = state.load_checkpoint()
        
        if not checkpoint:
            error_msg = "No checkpoint found - cannot resume"
            LOG.error(f"[{job_id}] {error_msg}")
            raise HTTPException(status_code=400, detail=error_msg)
        
        checkpoint_index = checkpoint.get('last_index', 0)
        processed_count = checkpoint.get('last_index', 0)
        LOG.info(f"[{job_id}] Checkpoint found: {processed_count} meters already processed, resuming from index {checkpoint_index}")

        # LOAD METERS FROM PERSISTED FILE
        LOG.info(f"[{job_id}] Loading meters list from file...")
        meters_list = load_meters_list(job_id)
        
        if not meters_list:
            LOG.error(f"[{job_id}] Meters list file not found!")
            
            # Try to load from job meta as fallback
            if job_id in jobs and 'meters_list' in jobs[job_id]:
                meters_list = jobs[job_id]['meters_list']
                LOG.info(f"[{job_id}] Recovered {len(meters_list)} meters from job meta")
            else:
                # Last resort: try to reconstruct from input file
                LOG.warning(f"[{job_id}] Attempting to reconstruct meters from input file...")
                try:
                    df_input = pd.read_excel(input_path, engine="openpyxl", dtype=str)
                    meters_list = extract_meters_from_dataframe(df_input, "merged_input")
                    
                    if meters_list:
                        # Save for future resumes
                        save_meters_list(job_id, meters_list)
                        LOG.info(f"[{job_id}] Reconstructed and saved {len(meters_list)} meters")
                    else:
                        error_msg = "Cannot resume: no meters found in input file"
                        LOG.error(f"[{job_id}] {error_msg}")
                        raise HTTPException(status_code=400, detail=error_msg)
                except Exception as e:
                    error_msg = f"Cannot resume: failed to reconstruct meters list: {str(e)}"
                    LOG.error(f"[{job_id}] {error_msg}")
                    raise HTTPException(status_code=400, detail=error_msg)
        
        total_meters = len(meters_list)
        LOG.info(f"[{job_id}] Successfully loaded {total_meters} total meters for resume")

        # Initialize websocket queue
        if job_id not in ws_queues:
            ws_queues[job_id] = []

        # Reset job state - CRITICAL: Don't reset processed count
        LOG.info(f"[{job_id}] Initializing job state for resume...")
        jobs[job_id] = {
            "status": "queued",
            "progress": int((processed_count / total_meters) * 100) if total_meters > 0 else 0,
            "total": total_meters,
            "log": [],
            "processed": processed_count,  # PRESERVE checkpoint progress
            "processed_meters": checkpoint.get("processed_meters", []),
            "cancel_requested": False,
            "is_stopping": False,
            "success": 0,
            "failed": 0,
            "unfound": 0,
        }
        await save_job_meta(job_id)

        # Start new task - DO NOT pass skip_meters, let checkpoint handle it
        LOG.info(f"[{job_id}] Starting resume task from checkpoint...")
        task = asyncio.create_task(
            run_job_async_with_retry(
                job_id, 
                str(input_path), 
                str(output_path),
                meters_list=meters_list,
                skip_meters=0  # ← IMPORTANT: Don't apply skip on resume, checkpoint handles it
            )
        )
        jobs[job_id]['task'] = task

        await broadcast(job_id, {
            "type": "success",
            "msg": f"🔁 Resuming from meter {checkpoint_index + 1}/{total_meters}"
        })

        LOG.info(f"[{job_id}] Resume successful - task started from checkpoint {checkpoint_index}")
        
        return JSONResponse({
            "msg": "Resume started",
            "job_id": job_id,
            "resume_from": checkpoint_index,
            "total_meters": total_meters,
            "already_processed": processed_count
        })
        
    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Failed to resume job: {str(e)}"
        LOG.error(f"[{job_id}] {error_msg}", exc_info=True)
        
        # Broadcast error to websocket
        try:
            await broadcast(job_id, {
                "type": "error",
                "msg": f"❌ Resume failed: {str(e)}"
            })
        except:
            pass
        
        raise HTTPException(status_code=500, detail=error_msg)


# Also add a new endpoint to check resume status
@app.get("/check_resume/{job_id}")
async def check_resume(job_id: str):
    """Check if a job can be resumed with detailed diagnostics"""
    input_path = UPLOAD_DIR / f"{job_id}_merged_input.xlsx"
    output_path = OUTPUT_DIR / f"{job_id}_output.xlsx"
    meters_path = _meters_list_path(job_id)

    result = {
        "can_resume": False,
        "input_exists": input_path.exists(),
        "output_exists": output_path.exists(),
        "meters_file_exists": meters_path.exists(),
        "checkpoint_exists": False,
        "processed_meters": 0,
        "total_meters": 0,
        "reason": None,
        "details": {}
    }

    # Check input file
    if not input_path.exists():
        result["reason"] = "Input file not found"
        return JSONResponse(result)

    # Check meters file
    meters_list = load_meters_list(job_id)
    if not meters_list:
        result["reason"] = "Meters list not found - will attempt reconstruction"
        result["details"]["meters_status"] = "missing"
        
        # Try to reconstruct
        try:
            df_input = pd.read_excel(input_path, engine="openpyxl", dtype=str)
            meters_list = extract_meters_from_dataframe(df_input, "merged_input")
            if meters_list:
                result["total_meters"] = len(meters_list)
                result["details"]["meters_status"] = "can_reconstruct"
                result["can_resume"] = True
            else:
                result["reason"] = "No meters found in input file"
                return JSONResponse(result)
        except Exception as e:
            result["reason"] = f"Failed to read input file: {str(e)}"
            return JSONResponse(result)
    else:
        result["total_meters"] = len(meters_list)
        result["details"]["meters_status"] = "loaded"

    # Check checkpoint
    state = JobState(job_id)
    checkpoint = state.load_checkpoint()
    
    if checkpoint:
        result["checkpoint_exists"] = True
        result["processed_meters"] = checkpoint.get("last_index", 0)
        result["can_resume"] = result["processed_meters"] < result["total_meters"]
        if not result["can_resume"]:
            result["reason"] = "Job already completed"
    elif output_path.exists():
        # Try to create checkpoint from output
        try:
            df_output = pd.read_excel(output_path, sheet_name="All", engine="openpyxl", dtype=str)
            processed_count = len(df_output)
            result["processed_meters"] = processed_count
            result["can_resume"] = processed_count < result["total_meters"]
            result["details"]["checkpoint_status"] = "can_create_from_output"
            if not result["can_resume"]:
                result["reason"] = "Job already completed"
        except Exception as e:
            result["reason"] = f"Failed to inspect output file: {str(e)}"
            result["can_resume"] = False
    else:
        result["can_resume"] = True
        result["details"]["checkpoint_status"] = "will_start_fresh"

    return JSONResponse(result)


# Update the frontend error handler to show more details
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Custom error handler with more details"""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "detail": exc.detail,
            "status_code": exc.status_code,
            "path": str(request.url)
        }
    )


@app.post("/stop/{job_id}")
async def stop_job_endpoint(job_id: str):
    """Stop job with proper cleanup for concurrent workers"""
    if job_id not in jobs:
        if _meta_path(job_id).exists():
            try:
                with open(_meta_path(job_id), 'r', encoding='utf-8') as f:
                    jobs[job_id] = json.load(f)
            except Exception:
                raise HTTPException(status_code=404, detail="Job not found")
        else:
            raise HTTPException(status_code=404, detail="Job not found")

    if job_id not in cancellation_locks:
        cancellation_locks[job_id] = threading.Lock()

    job = jobs[job_id]

    # Check if already stopping
    with cancellation_locks[job_id]:
        if job.get("is_stopping"):
            return JSONResponse({"msg": "Stop already in progress", "job_id": job_id})

        job['cancel_requested'] = True
        job['is_stopping'] = True
        job['status'] = 'cancelling'

    LOG.info(f"🛑 Stop requested for job {job_id}")
    
    await save_job_meta(job_id)
    
    try:
        await broadcast(job_id, {"type": "status", "msg": "🛑 Stop requested - cleaning up workers..."})
    except:
        pass

    await asyncio.sleep(0.5)

    # Cancel the main task (which will cleanup worker pool)
    task = job.get('task')
    if task and not task.done():
        LOG.info(f"[{job_id}] Cancelling main task...")
        task.cancel()
        
        try:
            await asyncio.wait_for(task, timeout=15.0)
            LOG.info(f"[{job_id}] Task cancelled successfully")
        except asyncio.TimeoutError:
            LOG.warning(f"Task {job_id} did not stop within timeout")
        except asyncio.CancelledError:
            LOG.info(f"Task {job_id} raised CancelledError (expected)")
        except Exception as e:
            error_msg = str(e).lower()
            if not any(phrase in error_msg for phrase in [
                "target page, context or browser has been closed",
                "browser has been closed",
                "context closed"
            ]):
                LOG.debug(f"Task cancellation error: {e}")

    # Save checkpoint
    try:
        processed_meters = job.get('processed_meters', [])
        processed_count = job.get('processed', len(processed_meters))

        if processed_meters:
            state = JobState(job_id)
            state.save_checkpoint(list(processed_meters), processed_count)
            LOG.info(f"[{job_id}] Checkpoint saved: {len(processed_meters)} meters")
            
            try:
                await broadcast(job_id, {
                    "type": "success",
                    "msg": f"💾 Progress saved ({len(processed_meters)} meters processed)"
                })
            except:
                pass
    except Exception as e:
        LOG.warning(f"Checkpoint save failed: {e}")

    # Update final status
    with cancellation_locks[job_id]:
        job['status'] = 'cancelled'
        job['cancel_requested'] = False
        job['is_stopping'] = False
        job['active_workers'] = 0
    
    await save_job_meta(job_id)

    LOG.info(f"[{job_id}] Stop complete - status: cancelled")
    
    try:
        await broadcast(job_id, {
            "type": "done",
            "msg": "✅ Job cancelled - Progress saved. Resume by restarting.",
            "meta": {
                "can_resume": True,
                "total": job.get('total', 0),
                "processed": processed_count,
                "success": job.get('success', 0),
                "failed": job.get('failed', 0),
                "unfound": job.get('unfound', 0)
            }
        })
    except Exception as e:
        LOG.debug(f"Failed to broadcast stop completion: {e}")

    return JSONResponse({
        "msg": "Job stopped successfully",
        "job_id": job_id,
        "status": "cancelled",
        "can_resume": True,
        "processed": processed_count
    })



@app.post("/append/{job_id}")
async def append_to_job(job_id: str, file: UploadFile = File(...)):
    """Append additional Excel file to existing job"""
    if job_id not in jobs:
        # allow append if persisted input exists
        merged_input_path = UPLOAD_DIR / f"{job_id}_merged_input.xlsx"
        if not merged_input_path.exists():
            raise HTTPException(status_code=404, detail="Job not found")
        else:
            jobs.setdefault(job_id, {})

    merged_input_path = UPLOAD_DIR / f"{job_id}_merged_input.xlsx"
    if not merged_input_path.exists():
        raise HTTPException(status_code=404, detail="Merged input for this job not found")

    try:
        content = await file.read()
        tmp_path = UPLOAD_DIR / f"{job_id}_append_tmp.xlsx"
        with open(tmp_path, "wb") as f:
            f.write(content)
        df_new = pd.read_excel(tmp_path, engine="openpyxl", dtype=str)
        df_new = df_new.fillna("")

        df_existing = pd.read_excel(merged_input_path, engine="openpyxl", dtype=str)
        df_merged = pd.concat([df_existing, df_new], ignore_index=True, sort=False)
        df_merged.to_excel(merged_input_path, index=False, engine="openpyxl")

        return JSONResponse({"msg": "Appended to job input", "job_id": job_id})
    except Exception as e:
        LOG.exception("Append failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Append failed: {str(e)}")





@app.get("/preview/{job_id}/electricity")
async def preview_electricity(job_id: str):
    """Preview electricity meters data as JSON"""
    path = OUTPUT_DIR / f"{job_id}_output_electricity.csv"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Electricity CSV not found")

    try:
        df = pd.read_csv(path)
        preview_data = df.head(50).to_dict(orient="records")
        return JSONResponse({
            "total_rows": len(df),
            "preview_rows": len(preview_data),
            "columns": list(df.columns),
            "data": preview_data
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading CSV: {str(e)}")


@app.get("/preview/{job_id}/gas")
async def preview_gas(job_id: str):
    """Preview gas meters data as JSON"""
    path = OUTPUT_DIR / f"{job_id}_output_gas.csv"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Gas CSV not found")

    try:
        df = pd.read_csv(path)
        preview_data = df.head(50).to_dict(orient="records")
        return JSONResponse({
            "total_rows": len(df),
            "preview_rows": len(preview_data),
            "columns": list(df.columns),
            "data": preview_data
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading CSV: {str(e)}")
    
    
# Add this endpoint to your app_async.py (around line 2700, after other preview endpoints)

@app.get("/preview/{job_id}/combined")
async def preview_combined(job_id: str):
    """
    Preview combined data (all meters including unfound) as JSON
    Returns separate arrays for electricity, gas, and unfound
    """
    path = OUTPUT_DIR / f"{job_id}_output.xlsx"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Output file not found")

    try:
        # Read from Combined sheet
        df = pd.read_excel(path, sheet_name="Combined", engine="openpyxl", dtype=str)
        
        # Separate by type
        electricity_rows = []
        gas_rows = []
        unfound_rows = []
        
        def is_unfound(row_dict):
            """Check if row is unfound (no address data)"""
            address_fields = ["Site Building Name", "Site Street 1", "Site Town", "Site Postcode"]
            return not any(
                row_dict.get(f) and str(row_dict.get(f)).strip() and 
                str(row_dict.get(f)).lower() not in ['none', 'nan', '']
                for f in address_fields
            )
        
        def is_gas(row_dict):
            """Check if row is Gas meter"""
            # Check Annualised AQ column (Gas-specific)
            has_aq = (
                row_dict.get("Annualised AQ") and 
                str(row_dict.get("Annualised AQ")).strip() and
                str(row_dict.get("Annualised AQ")).lower() not in ['none', 'nan', '']
            )
            
            # Check meter number format (MPRN = 6-10 digits)
            meter = str(row_dict.get("Meter Number", ""))
            digits = len([c for c in meter if c.isdigit()])
            is_mprn_format = 6 <= digits <= 10
            
            return has_aq or is_mprn_format
        
        def is_electricity(row_dict):
            """Check if row is Electricity meter"""
            # Check Energization Status column (Electricity-specific)
            has_energization = (
                row_dict.get("Energization Status") and 
                str(row_dict.get("Energization Status")).strip() and
                str(row_dict.get("Energization Status")).lower() not in ['none', 'nan', '']
            )
            
            # Check meter number format (MPAN = 13 digits)
            meter = str(row_dict.get("Meter Number", ""))
            digits = len([c for c in meter if c.isdigit()])
            is_mpan_format = digits == 13
            
            return has_energization or is_mpan_format
        
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            
            if is_unfound(row_dict):
                # Add kind field for frontend
                meter = str(row_dict.get("Meter Number", ""))
                digits = len([c for c in meter if c.isdigit()])
                row_dict["kind"] = "Gas" if (6 <= digits <= 10) else "Electricity"
                unfound_rows.append(row_dict)
            elif is_gas(row_dict):
                row_dict["kind"] = "Gas"
                gas_rows.append(row_dict)
            elif is_electricity(row_dict):
                row_dict["kind"] = "Electricity"
                electricity_rows.append(row_dict)
            else:
                # Fallback: try to determine by meter number
                meter = str(row_dict.get("Meter Number", ""))
                digits = len([c for c in meter if c.isdigit()])
                if digits == 13:
                    row_dict["kind"] = "Electricity"
                    electricity_rows.append(row_dict)
                elif 6 <= digits <= 10:
                    row_dict["kind"] = "Gas"
                    gas_rows.append(row_dict)
        
        # Limit preview
        preview_limit = 100
        
        return JSONResponse({
            "total_rows": len(df),
            "electricity_count": len(electricity_rows),
            "gas_count": len(gas_rows),
            "unfound_count": len(unfound_rows),
            "preview_rows": min(len(df), preview_limit),
            "columns": list(df.columns),
            "data": {
                "electricity": electricity_rows[:preview_limit],
                "gas": gas_rows[:preview_limit],
                "unfound": unfound_rows[:preview_limit]
            }
        })
    except Exception as e:
        LOG.error(f"Preview combined error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error reading file: {str(e)}")  
    
    
@app.get("/preview/{job_id}/unfound")
async def preview_unfound(job_id: str):
    """Preview unfound meters data as JSON - includes Gas meters with AQ"""
    path = OUTPUT_DIR / f"{job_id}_output.xlsx"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Output file not found")

    try:
        df = pd.read_excel(path, sheet_name="Combined", engine="openpyxl", dtype=str)
        
        # Filter unfound (empty address)
        unfound_rows = []
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            is_unfound = (
                row_dict.get("Status") == "Unfound" or
                not any(row_dict.get(f) and str(row_dict.get(f)).strip() 
                       for f in ["Site Building Name", "Site Street 1", "Site Town", "Site Postcode"])
            )
            if is_unfound:
                unfound_rows.append(row_dict)
        
        preview_data = unfound_rows[:50]
        
        return JSONResponse({
            "total_rows": len(unfound_rows),
            "preview_rows": len(preview_data),
            "columns": list(df.columns),
            "data": preview_data
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading CSV: {str(e)}")
    


@app.get("/stats/{job_id}")
async def get_job_stats(job_id: str):
    """Get job statistics including meter type breakdown"""
    elec_path = OUTPUT_DIR / f"{job_id}_output_electricity.csv"
    gas_path = OUTPUT_DIR / f"{job_id}_output_gas.csv"
    unfound_path = OUTPUT_DIR / f"{job_id}_output_unfound.csv"

    stats = {
        "job_id": job_id,
        "elec_count": 0,
        "gas_count": 0,
        "unfound_count": 0,
        "total_count": 0
    }

    try:
        if elec_path.exists():
            df_elec = pd.read_csv(elec_path)
            stats["elec_count"] = len(df_elec)

        if gas_path.exists():
            df_gas = pd.read_csv(gas_path)
            stats["gas_count"] = len(df_gas)

        if unfound_path.exists():
            df_un = pd.read_csv(unfound_path)
            stats["unfound_count"] = len(df_un)

        stats["total_count"] = stats["elec_count"] + stats["gas_count"] + stats["unfound_count"]

        return JSONResponse(stats)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading stats: {str(e)}")


@app.get("/download/{job_id}")
async def download_output(job_id: str):
    """Download full Excel file with all sheets (Combined, Electricity, Gas, Unfound)"""
    path = OUTPUT_DIR / f"{job_id}_output.xlsx"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Output not ready")
    
    LOG.info(f"Downloading full Excel: {path}")
    return FileResponse(str(path), filename=f"{job_id}_all_data.xlsx")


@app.get("/download/{job_id}/electricity")
async def download_electricity_csv(job_id: str):
    """Download Electricity sheet as CSV"""
    xlsx_path = OUTPUT_DIR / f"{job_id}_output.xlsx"
    
    if not xlsx_path.exists():
        raise HTTPException(status_code=404, detail="Output file not found")
    
    try:
        # Read Electricity sheet
        df_elec = pd.read_excel(xlsx_path, sheet_name="Electricity", engine="openpyxl", dtype=str)
        
        if len(df_elec) == 0:
            raise HTTPException(status_code=404, detail="No electricity data found")
        
        # Save to CSV
        csv_path = OUTPUT_DIR / f"{job_id}_electricity.csv"
        df_elec.to_csv(csv_path, index=False)
        
        LOG.info(f"Downloading electricity CSV: {len(df_elec)} rows")
        return FileResponse(str(csv_path), filename=f"{job_id}_electricity.csv")
        
    except HTTPException:
        raise
    except Exception as e:
        LOG.error(f"Error downloading electricity CSV: {e}")
        raise HTTPException(status_code=500, detail=f"Error reading electricity data: {str(e)}")


@app.get("/download/{job_id}/gas")
async def download_gas_csv(job_id: str):
    """Download Gas sheet as CSV"""
    xlsx_path = OUTPUT_DIR / f"{job_id}_output.xlsx"
    
    if not xlsx_path.exists():
        raise HTTPException(status_code=404, detail="Output file not found")
    
    try:
        # Read Gas sheet
        df_gas = pd.read_excel(xlsx_path, sheet_name="Gas", engine="openpyxl", dtype=str)
        
        if len(df_gas) == 0:
            raise HTTPException(status_code=404, detail="No gas data found")
        
        # Save to CSV
        csv_path = OUTPUT_DIR / f"{job_id}_gas.csv"
        df_gas.to_csv(csv_path, index=False)
        
        LOG.info(f"Downloading gas CSV: {len(df_gas)} rows")
        return FileResponse(str(csv_path), filename=f"{job_id}_gas.csv")
        
    except HTTPException:
        raise
    except Exception as e:
        LOG.error(f"Error downloading gas CSV: {e}")
        raise HTTPException(status_code=500, detail=f"Error reading gas data: {str(e)}")


@app.get("/download/{job_id}/unfound")
async def download_unfound_csv(job_id: str):
    """Download Unfound sheet as CSV"""
    xlsx_path = OUTPUT_DIR / f"{job_id}_output.xlsx"
    
    if not xlsx_path.exists():
        raise HTTPException(status_code=404, detail="Output file not found")
    
    try:
        # Read Unfound sheet
        df_unfound = pd.read_excel(xlsx_path, sheet_name="Unfound", engine="openpyxl", dtype=str)
        
        if len(df_unfound) == 0:
            raise HTTPException(status_code=404, detail="No unfound data found")
        
        # Save to CSV
        csv_path = OUTPUT_DIR / f"{job_id}_unfound.csv"
        df_unfound.to_csv(csv_path, index=False)
        
        LOG.info(f"Downloading unfound CSV: {len(df_unfound)} rows")
        return FileResponse(str(csv_path), filename=f"{job_id}_unfound.csv")
        
    except HTTPException:
        raise
    except Exception as e:
        LOG.error(f"Error downloading unfound CSV: {e}")
        raise HTTPException(status_code=500, detail=f"Error reading unfound data: {str(e)}")


@app.post("/delete/{job_id}")
async def delete_job(job_id: str):
    """Delete a job's persisted metadata and files"""
    # Remove persisted meta
    delete_persisted_job(job_id)

    # Remove meters list file
    try:
        meters_path = _meters_list_path(job_id)
        if meters_path.exists():
            meters_path.unlink()
            LOG.info(f"[{job_id}] Deleted meters list file")
    except Exception as e:
        LOG.warning(f"Failed to delete meters list for {job_id}: {e}")

    # Remove common files if present
    patterns = [
        UPLOAD_DIR / f"final_{job_id}.xlsx",
        OUTPUT_DIR / f"{job_id}_output.xlsx",
        OUTPUT_DIR / f"{job_id}_output_electricity.csv",
        OUTPUT_DIR / f"{job_id}_output_gas.csv",
        OUTPUT_DIR / f"{job_id}_output_unfound.csv",
    ]
    for p in patterns:
        try:
            if p.exists():
                p.unlink()
        except Exception:
            LOG.debug(f"Failed to delete {p}")

    # Remove in-memory job and cancel running task if present
    job = jobs.pop(job_id, None)
    if job:
        task = job.get("task")
        if task and not task.done():
            try:
                task.cancel()
            except Exception:
                pass

    # Notify websockets and clear queue
    if job_id in ws_queues:
        try:
            for ws in list(ws_queues[job_id]):
                try:
                    await ws.send_json({"type": "info", "msg": "Job deleted"})
                except Exception:
                    pass
        except Exception:
            pass
        ws_queues.pop(job_id, None)

    # Clear any state files
    try:
        state = JobState(job_id)
        state.clear()
    except Exception:
        pass

    return JSONResponse({"msg": "Deleted job resources", "job_id": job_id})


@app.websocket("/ws/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    await websocket.accept()
    if job_id not in ws_queues:
        ws_queues[job_id] = []
    ws_queues[job_id].append(websocket)

    # try to send meta either from in-memory or persisted meta
    meta = None
    if job_id in jobs:
        meta = {
            "status": jobs[job_id].get("status"),
            "progress": jobs[job_id].get("progress"),
            "total": jobs[job_id].get("total"),
            "processed": jobs[job_id].get("processed", 0)
        }
    else:
        p = _meta_path(job_id)
        if p.exists():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    stored = json.load(f)
                meta = {
                    "status": stored.get("status"),
                    "progress": stored.get("progress", 0),
                    "total": stored.get("total", 0),
                    "processed": stored.get("processed", 0)
                }
                jobs.setdefault(job_id, {}).update(stored)
            except Exception:
                meta = None

    if meta:
        try:
            await websocket.send_json({"type": "status", "msg": "connected", "meta": meta})
        except Exception:
            pass

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        try:
            ws_queues[job_id].remove(websocket)
        except Exception:
            pass


# Run: uvicorn app_async:app --host 0.0.0.0 --port 8000 --reload
