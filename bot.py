"""Scheduled sync job."""
import datetime as dt
import json
import os
import random
import time

import requests
from nectar import Hive

API = "https://api.hive.blog"
STATE_FILE = "sync_state.json"


def env(name, default=""):
    return os.environ.get(name) or default


ACCOUNT = env("HIVE_ACCOUNT").strip().lstrip("@").lower()
POSTING_KEY = env("HIVE_POSTING_KEY")
DRY_RUN = env("DRY_RUN", "true").lower() != "false"
MAX_PER_RUN = int(env("MAX_PER_RUN", "25"))
MAX_PER_DAY = int(env("MAX_PER_DAY", "200"))
MIN_RC_PCT = float(env("MIN_RC_PCT", "20"))        # stop when RC mana falls below this %
LOOKBACK_BLOCKS = int(env("LOOKBACK_BLOCKS", "1200"))   # first run: about 1 hour
SCAN_BLOCKS_MAX = int(env("SCAN_BLOCKS_MAX", "3000"))   # per run (3 sec per block)
QUEUE_MAX = int(env("QUEUE_MAX", "1000"))
CREATE_OPS = {"account_create", "account_create_with_delegation", "create_claimed_account"}


def rpc(method, params):
    last = None
    for attempt in range(3):
        try:
            r = requests.post(API, json={"jsonrpc": "2.0", "method": method,
                                         "params": params, "id": 1}, timeout=30)
            r.raise_for_status()
            j = r.json()
            if "error" in j:
                raise RuntimeError(j["error"])
            return j["result"]
        except requests.exceptions.HTTPError as e:
            last = e
            status = e.response.status_code if e.response is not None else 0
            print(f"API HTTP {status} on {method} (attempt {attempt + 1}/3)")
            if status not in (429, 500, 502, 503, 504):
                raise
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last = e
            print(f"API connection issue on {method} (attempt {attempt + 1}/3)")
        if attempt < 2:
            time.sleep(2 ** attempt * 2)
    raise last


def load_state():
    base = {"last_block": 0, "queue": [], "day": {}}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as fh:
            base.update(json.load(fh))
    return base


def save_state(state):
    with open(STATE_FILE, "w") as fh:
        json.dump(state, fh)


def new_accounts_in(blocks):
    """Yield account names created in the given blocks (handles both op formats)."""
    for b in blocks:
        for tx in b.get("transactions", []):
            for op in tx.get("operations", []):
                if isinstance(op, dict):
                    typ, val = op.get("type", ""), op.get("value", {})
                else:
                    typ, val = op[0], op[1]
                if typ.replace("_operation", "") in CREATE_OPS and val.get("new_account_name"):
                    yield val["new_account_name"].lower()


def scan_new_accounts(state):
    props = rpc("condenser_api.get_dynamic_global_properties", [])
    safe_head = int(props["last_irreversible_block_num"])
    start = state["last_block"] + 1 if state["last_block"] else safe_head - LOOKBACK_BLOCKS
    end = min(safe_head, start + SCAN_BLOCKS_MAX - 1)
    found, n = [], start
    while n <= end:
        count = min(100, end - n + 1)
        try:
            res = rpc("block_api.get_block_range", {"starting_block_num": n, "count": count})
        except Exception as e:
            print("scan stopped:", type(e).__name__)
            break
        blocks = res.get("blocks", [])
        found.extend(new_accounts_in(blocks))
        n += count
        time.sleep(0.3)
    state["last_block"] = n - 1
    queued = set(state["queue"])
    for a in found:
        if a not in queued and a != ACCOUNT:
            state["queue"].append(a)
            queued.add(a)
    state["queue"] = state["queue"][-QUEUE_MAX:]
    print(f"scanned blocks {start}-{n - 1}, new accounts found: {len(found)}, queue: {len(state['queue'])}")


def rc_ok():
    r = rpc("rc_api.find_rc_accounts", {"accounts": [ACCOUNT]})["rc_accounts"][0]
    mx = int(r["max_rc"])
    if mx <= 0:
        return False
    m = r["rc_manabar"]
    regen = (time.time() - int(m["last_update_time"])) * mx / 432000  # 5 day regeneration
    cur = min(mx, int(m["current_mana"]) + regen)
    pct = cur / mx * 100
    print(f"RC: {pct:.0f}%")
    return pct >= MIN_RC_PCT


def already_following(name):
    rel = rpc("bridge.get_relationship_between_accounts", [ACCOUNT, name])
    return bool(rel.get("follows"))


def main():
    if not ACCOUNT:
        raise RuntimeError("HIVE_ACCOUNT is missing.")
    if not DRY_RUN and not POSTING_KEY:
        raise RuntimeError("HIVE_POSTING_KEY is required when DRY_RUN=false.")

    state = load_state()
    today = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    state["day"] = {today: state["day"].get(today, 0)}

    scan_new_accounts(state)

    budget = min(MAX_PER_RUN, MAX_PER_DAY - state["day"][today])
    if budget <= 0:
        print("daily limit reached")
        save_state(state)
        return
    if not rc_ok():
        print("RC too low, not following this run")
        save_state(state)
        return

    hive = None if DRY_RUN else Hive(node=[API], keys=[POSTING_KEY])
    done, failures = 0, 0
    for name in list(state["queue"]):
        if done >= budget or failures >= 3:
            break
        try:
            if already_following(name):
                state["queue"].remove(name)
                continue
            if DRY_RUN:
                print(f"[dry run] would follow {name}")
            else:
                hive.custom_json(
                    "follow",
                    ["follow", {"follower": ACCOUNT, "following": name, "what": ["blog"]}],
                    required_posting_auths=[ACCOUNT])
        except Exception as e:
            failures += 1
            print(f"follow failed: {type(e).__name__}: {str(e)[:150]}")
            continue
        failures = 0
        state["queue"].remove(name)
        state["day"][today] += 1
        done += 1
        if not DRY_RUN:
            time.sleep(random.randint(4, 9))

    print(f"followed={done}, left in queue={len(state['queue'])}")
    save_state(state)


if __name__ == "__main__":
    main()
