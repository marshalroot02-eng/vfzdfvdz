"""
DSJ Punjab FIR Search Automation - Fleet Runner
Registers with the backend, claims FIRs atomically, scrapes DSJ website,
and reports results back via the fleet bot API.
"""

import asyncio
import subprocess
import sys
import os
from datetime import datetime

os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8')

# Auto-install dependencies
def ensure_import(import_name, pip_name=None):
    try:
        __import__(import_name)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", pip_name or import_name])

ensure_import("camoufox", "camoufox[geoip]")
ensure_import("playwright", "playwright")
ensure_import("httpx")

subprocess.run([sys.executable, "-m", "playwright", "install", "firefox"], check=True, capture_output=True)
subprocess.run([sys.executable, "-m", "camoufox", "fetch"], check=True, capture_output=True)

import httpx

# ─── Configuration from environment ───
API_URL = os.environ.get("API_URL", "http://localhost:4000/api")
API_KEY = os.environ.get("API_KEY", "")
SESSION_ID = int(os.environ.get("SESSION_ID", "0"))
REPO_ID = int(os.environ.get("REPO_ID", "0"))

CLAIM_BATCH_SIZE = 10
HEARTBEAT_INTERVAL = 60
DATE_WINDOW_PAST_DAYS = 15
NAVIGATION_TIMEOUT_MS = 18000
NAVIGATION_RETRIES = 2
BLOCKED_RESOURCE_TYPES = {"image", "media", "font", "stylesheet"}

headers = {"Content-Type": "application/json"}
if API_KEY:
    headers["x-api-key"] = API_KEY

runner_id = 0  # Set after registration


# ─── API helpers ───
async def api_get(client: httpx.AsyncClient, path: str):
    r = await client.get(f"{API_URL}{path}", headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

async def api_post(client: httpx.AsyncClient, path: str, data: dict):
    r = await client.post(f"{API_URL}{path}", headers=headers, json=data, timeout=30)
    r.raise_for_status()
    return r.json()


# ─── Fleet API ───
async def register_runner(client, ip=""):
    return await api_post(client, "/fleet/bot/register", {
        "repoId": REPO_ID, "sessionId": SESSION_ID, "ipAddress": ip,
    })

async def send_heartbeat(client, **kwargs):
    return await api_post(client, "/fleet/bot/heartbeat", {
        "runnerId": runner_id, **kwargs,
    })

async def claim_firs(client):
    return await api_post(client, "/fleet/bot/claim", {
        "runnerId": runner_id, "sessionId": SESSION_ID, "batchSize": CLAIM_BATCH_SIZE,
    })

async def complete_fir(client, fir_id, status, results=None):
    return await api_post(client, "/fleet/bot/complete", {
        "runnerId": runner_id, "firRecordId": fir_id,
        "status": status, "results": results or [],
    })


# ─── Browser helpers ───
async def configure_lightweight_page(page):
    async def route_handler(route):
        if route.request.resource_type in BLOCKED_RESOURCE_TYPES:
            await route.abort()
            return
        await route.continue_()
    await page.route("**/*", route_handler)


async def create_browser(proxy_config=None):
    from camoufox.async_api import AsyncCamoufox
    kwargs = {"headless": True}
    if proxy_config:
        kwargs["proxy"] = proxy_config
    print(f"[LAUNCH] Camoufox (proxy={'yes' if proxy_config else 'no'})")
    browser = await AsyncCamoufox(**kwargs).__aenter__()
    page = await browser.new_page()
    await configure_lightweight_page(page)
    return browser, page


def parse_proxy(proxy_str):
    if not proxy_str:
        return None
    parts = proxy_str.strip().split(":")
    if len(parts) < 2:
        return None
    config = {"server": f"http://{parts[0]}:{parts[1]}"}
    if len(parts) >= 4:
        config["username"] = parts[2]
        config["password"] = parts[3]
    return config


# ─── Scraping core ───
async def scrape_search_results(page):
    try:
        await page.wait_for_selector("table#dt_casesearch tbody tr", timeout=10000)
    except Exception:
        return [], []

    rows_data = await page.evaluate("""
        () => {
            const rows = [];
            const table = document.querySelector('table#dt_casesearch tbody');
            if (!table) return rows;
            table.querySelectorAll('tr').forEach(tr => {
                const tds = tr.querySelectorAll('td');
                if (tds.length >= 6) {
                    const clean = t => t.trim().replace(/\\s+/g,' ');
                    const caseLinkEl = tds[0].querySelector('a') || tr.querySelector('a');
                    let caseLink = '';
                    if (caseLinkEl) {
                        const href = caseLinkEl.getAttribute('href');
                        if (href) {
                            try { caseLink = new URL(href, window.location.origin).href; }
                            catch { caseLink = href; }
                        }
                    }
                    rows.push({
                        caseNo: clean(tds[0].textContent),
                        caseLink, caseTitle: clean(tds[1].textContent),
                        category: clean(tds[2].textContent), judge: clean(tds[3].textContent),
                        stage: clean(tds[4].textContent), hearingDate: clean(tds[5].textContent),
                    });
                }
            });
            return rows;
        }
    """)

    today = datetime.now().date()
    matching, all_rows = [], []

    for row in rows_data:
        is_match = False
        if 'pre arrest' in row['category'].lower():
            valid_keywords = ['confirm', 'accept', 'compromise', 'acquitted']
            if any(k in row['stage'].lower() for k in valid_keywords):
                try:
                    hearing = datetime.strptime(row['hearingDate'].strip(), '%d-%m-%Y').date()
                    if (hearing - today).days >= -DATE_WINDOW_PAST_DAYS:
                        is_match = True
                except Exception:
                    pass
        row['matchedFilters'] = is_match
        all_rows.append(row)
        if is_match:
            matching.append(row)

    return matching, all_rows


async def process_record(page, record):
    district = record['district']
    ps = record['policeStation']
    fir_number = record['firNumber']
    year = record['year']

    search_url = (
        f"https://dsj.punjab.gov.pk/search?"
        f"district_id={district['dsjValue']}&case_title=&case_no=&court_id="
        f"&casecate_id=0&fir_no={fir_number}&fir_year={year}"
        f"&policestation_id={ps['dsjValue']}"
    )

    print(f"  [SEARCH] FIR {fir_number}/{year} ({district['name']})")

    for attempt in range(1, NAVIGATION_RETRIES + 1):
        try:
            await page.goto(search_url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT_MS)
            await asyncio.sleep(0.3)
            break
        except Exception as e:
            print(f"    [RETRY] {attempt}/{NAVIGATION_RETRIES}: {e}")
            if attempt == NAVIGATION_RETRIES:
                return "FAILED", []
            await asyncio.sleep(1 * attempt)

    matching, all_rows = await scrape_search_results(page)
    print(f"    [RESULT] {len(all_rows)} rows, {len(matching)} matches")

    results = [{
        "caseNo": r['caseNo'], "caseLink": r['caseLink'], "caseTitle": r['caseTitle'],
        "category": r['category'], "judge": r['judge'], "stage": r['stage'],
        "hearingDate": r['hearingDate'], "matchedFilters": r['matchedFilters'],
    } for r in all_rows]

    if matching:
        return "COMPLETED", results
    elif all_rows:
        return "NO_MATCH", results
    return "NO_MATCH", []


# ─── Heartbeat background task ───
async def heartbeat_loop(client, counters):
    while True:
        try:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            await send_heartbeat(client, **counters)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"  [HB ERROR] {e}")


# ─── Main ───
async def main():
    global runner_id
    print(f"[START] Fleet Runner | API: {API_URL} | Session: {SESSION_ID} | Repo: {REPO_ID}")

    # Claim a proxy (round-robin, least-recently-used)
    proxy_config = None
    async with httpx.AsyncClient() as client:
        try:
            p = await api_post(client, "/proxies/claim", {})
            if p and p.get('host'):
                ps = f"{p['host']}:{p['port']}"
                if p.get('username'):
                    ps += f":{p['username']}:{p['password']}"
                proxy_config = parse_proxy(ps)
                print(f"[PROXY] {p['host']}:{p['port']}")
            else:
                print("[WARN] No proxy assigned - running without proxy")
        except Exception as e:
            print(f"[WARN] No proxies available: {e}")

    browser = None
    try:
        async with httpx.AsyncClient() as client:
            # Get external IP
            ip = ""
            try:
                r = await client.get("https://api.ipify.org", timeout=5)
                ip = r.text.strip()
            except:
                pass
            print(f"[IP] {ip}")

            # Register
            reg = await register_runner(client, ip)
            runner_id = reg['runnerId']
            print(f"[REGISTERED] Runner #{runner_id}")

            # Launch browser
            browser, page = await create_browser(proxy_config)

            counters = {"processedCount": 0, "successCount": 0, "failedCount": 0, "matchCount": 0}
            hb_task = asyncio.create_task(heartbeat_loop(client, counters))

            try:
                while True:
                    resp = await claim_firs(client)
                    records = resp.get('records', [])
                    if not records:
                        print("[DONE] No more records")
                        break

                    print(f"[CLAIMED] {len(records)} FIRs")

                    for record in records:
                        try:
                            status, results = await process_record(page, record)
                            await complete_fir(client, record['id'], status, results)
                            counters['processedCount'] += 1
                            if status in ('COMPLETED', 'NO_MATCH'):
                                counters['successCount'] += 1
                                counters['matchCount'] += sum(1 for r in results if r.get('matchedFilters'))
                            else:
                                counters['failedCount'] += 1
                        except Exception as e:
                            print(f"  [ERROR] FIR {record['id']}: {e}")
                            try:
                                await complete_fir(client, record['id'], 'FAILED')
                            except:
                                pass
                            counters['processedCount'] += 1
                            counters['failedCount'] += 1

                        await asyncio.sleep(0.2)

                await send_heartbeat(client, status='COMPLETED', **counters)
                print(f"[COMPLETE] P={counters['processedCount']} S={counters['successCount']} "
                      f"F={counters['failedCount']} M={counters['matchCount']}")

            finally:
                hb_task.cancel()
                try:
                    await hb_task
                except asyncio.CancelledError:
                    pass

    except Exception as e:
        print(f"[FATAL] {e}")
        try:
            async with httpx.AsyncClient() as client:
                await send_heartbeat(client, status='FAILED')
        except:
            pass
    finally:
        if browser:
            try:
                await browser.__aexit__(None, None, None)
            except:
                pass


if __name__ == "__main__":
    asyncio.run(main())
