import socket

import time
import sys

import os

import datetime

import re

import fcntl

import atexit

import signal

import random

from threading import Lock

from concurrent.futures import ThreadPoolExecutor, as_completed



VIP = "10.0.0.100"

PORT = 5000

TIMEOUT_SEC = 40
#TIMEOUT_SEC = 20



#OUTPUT_DIR = "./results"

#os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_DIR = os.environ.get("RESULTS_BASE", "./results")
os.makedirs(OUTPUT_DIR, exist_ok=True)


RUN_ID = os.environ.get("RUN_ID") or datetime.datetime.now().strftime("%H%M%S")

CLIENT_ID = os.environ.get("CLIENT_ID") or os.environ.get("HOSTNAME") or "unknown"



RUN_DIR = os.path.join(OUTPUT_DIR, RUN_ID)

os.makedirs(RUN_DIR, exist_ok=True)



# Per-client files (avoid interleaving between clients)

status_file = os.path.join(RUN_DIR, f"status_{RUN_ID}_{CLIENT_ID}.txt")

times_file  = os.path.join(RUN_DIR, f"times_{RUN_ID}_{CLIENT_ID}.txt")



# Shared across clients

total_file  = os.path.join(RUN_DIR, f"total_{RUN_ID}.txt")



# Shared summary across clients

summary_all = os.path.join(RUN_DIR, f"clients_summary_{RUN_ID}.txt")

LOCK_FILE   = os.path.join(RUN_DIR, ".summary.lock")



# Concurrency limit: 0 => send all at once

MAX_IN_FLIGHT = int(os.environ.get("MAX_IN_FLIGHT", "0"))



# Random gap options:

# If GAP_MIN/GAP_MAX are set -> use uniform(GAP_MIN, GAP_MAX)

# Else -> use uniform(0, SEND_GAP_SEC)

SEND_GAP_SEC = float(os.environ.get("SEND_GAP_SEC", "0.1"))

GAP_MIN = os.environ.get("GAP_MIN", "")

GAP_MAX = os.environ.get("GAP_MAX", "")



gap_min = float(GAP_MIN) if GAP_MIN != "" else None

gap_max = float(GAP_MAX) if GAP_MAX != "" else None



# Shared state (protected by lock)

state_lock = Lock()

attempts = 0

ok_count = 0

fail_count = 0

sum_rtt_ms = 0.0



def parse_requests(s: str):

    s = (s or "").strip()

    if not s:

        return [("M", "1")]



    if "," in s or " " in s:

        tokens = [t.strip() for t in re.split(r"[,\s]+", s) if t.strip()]

        out = []

        for tok in tokens:

            m = re.match(r"^([MVP])(\d+)$", tok, re.IGNORECASE)

            if m:

                out.append((m.group(1).upper(), m.group(2)))

        return out if out else [("M", "1")]



    out = []

    for m in re.finditer(r"([MVP])(\d+)", s, re.IGNORECASE):

        out.append((m.group(1).upper(), m.group(2)))

    return out if out else [("M", "1")]



def write_shared_summary():

    with state_lock:

        local_attempts = attempts

        local_ok = ok_count

        local_fail = fail_count



    with open(LOCK_FILE, "a") as lf:

        fcntl.flock(lf, fcntl.LOCK_EX)



        data = {}

        if os.path.exists(summary_all):

            try:

                with open(summary_all, "r") as sf:

                    for line in sf:

                        line = line.strip()

                        if not line:

                            continue

                        parts = [p.strip() for p in line.split(",")]

                        c = parts[0]

                        vals = {}

                        for p in parts[1:]:

                            k, v = p.split("=")

                            vals[k.strip()] = int(v.strip())

                        data[c] = vals

            except Exception:

                data = {}



        data[CLIENT_ID] = {"attempts": local_attempts, "ok": local_ok, "fail": local_fail}



        with open(summary_all, "w") as sf:

            for c in sorted(data.keys()):

                v = data[c]

                sf.write(f"{c},attempts={v['attempts']},ok={v['ok']},fail={v['fail']}\n")

            sf.flush()



        fcntl.flock(lf, fcntl.LOCK_UN)



def on_exit(*_args):

    try:

        write_shared_summary()

    except Exception:

        pass



signal.signal(signal.SIGTERM, on_exit)

signal.signal(signal.SIGINT, on_exit)

atexit.register(on_exit)



def do_one_request(req_type: str, req_id: str):

    t0 = time.monotonic()

    ok = False

    resp_str = ""

    sock = None

    try:

        sock = socket.socket()

        sock.settimeout(TIMEOUT_SEC)

        sock.connect((VIP, PORT))

        sock.sendall(f"{req_type} {req_id}\n".encode())



        data = sock.recv(1024)

        if data:

            ok = True

            resp_str = data.decode(errors="ignore").strip()

        else:

            resp_str = "EMPTY_RESPONSE"

           

    except socket.timeout:

        resp_str = "TIMEOUT_ERROR"

        print(f"[{CLIENT_ID}] ⚠️ TIMEOUT: Server took too long to respond ({req_type}{req_id})")

       

    except ConnectionRefusedError:

        resp_str = "CONNECTION_REFUSED"

        # זו ההדפסה שביקשת - היא תופיע בטרמינל כשהשרת דוחה את החיבור

        print(f"[{CLIENT_ID}] ❌ CONNECTION REFUSED: Server/LoadBalancer rejected the connection! ({req_type}{req_id})")

       

    except Exception as e:

        ok = False

        resp_str = f"ERROR: {type(e).__name__}"

        print(f"[{CLIENT_ID}] ❗ Unexpected Error: {e}")

       

    finally:

        try:

            if sock:

                sock.close()

        except Exception:

            pass



    rtt_ms = (time.monotonic() - t0) * 1000.0

    return {"type": req_type, "id": req_id, "ok": ok, "rtt_ms": rtt_ms, "resp": resp_str}





def pick_gap():

    if gap_min is not None and gap_max is not None:

        lo = min(gap_min, gap_max)

        hi = max(gap_min, gap_max)

        return random.uniform(lo, hi)

    return random.uniform(0.0, max(0.0, SEND_GAP_SEC))



#REQUESTS_ENV = os.environ.get("REQUESTS", "M1V2P3")
if len(sys.argv) > 1:
    REQUESTS_ENV = sys.argv[1]
else:
    REQUESTS_ENV = os.environ.get("REQUESTS", "M1V2P3")

REQ_LIST = parse_requests(REQUESTS_ENV)

n = len(REQ_LIST)



workers = n if MAX_IN_FLIGHT <= 0 else max(1, min(MAX_IN_FLIGHT, n))

gap_mode = f"minmax({gap_min},{gap_max})" if (gap_min is not None and gap_max is not None) else f"0..{SEND_GAP_SEC}"



print(f"[{CLIENT_ID}] START concurrent (locks). REQUESTS='{REQUESTS_ENV}' parsed={REQ_LIST} workers={workers} gap={gap_mode}")



wall_start = time.monotonic()



with ThreadPoolExecutor(max_workers=workers) as ex:

    futures = []

    for i, (t, rid) in enumerate(REQ_LIST):

        if i > 0:

            g = pick_gap()

            ts_gap = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")



            # ✅ log the chosen gap into the status file

            # This means: "before starting request t+rid, we slept g seconds"

            with open(status_file, "a", buffering=1) as f:

                f.write(f"{ts_gap},{CLIENT_ID},GAP_BEFORE,req={t}{rid},gap_sec={g:.6f}\n")



            if g > 0:

                time.sleep(g)



        futures.append(ex.submit(do_one_request, t, rid))



    for fut in as_completed(futures):

        r = fut.result()

        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")



        with state_lock:

            attempts += 1

            sum_rtt_ms += r["rtt_ms"]

            if r["ok"]:

                ok_count += 1

            else:

                fail_count += 1



            #with open(status_file, "a", buffering=1) as f:

            #    f.write(

            #        f"{ts},{CLIENT_ID},RESULT,req={r['type']}{r['id']},"

            
            #        f"{'OK' if r['ok'] else 'FAIL'},rtt_ms={r['rtt_ms']:.3f}\n"

            #    )

            with open(status_file, "a", buffering=1) as f:
                f.write(
                    f"{ts},{CLIENT_ID},RESULT,req={r['type']}{r['id']},"
                    f"{'OK' if r['ok'] else 'FAIL'},rtt_ms={r['rtt_ms']:.3f},resp={r['resp']}\n"
                )



            with open(times_file, "a", buffering=1) as f:

                f.write(f"{r['rtt_ms']:.3f}\n")



wall_ms = (time.monotonic() - wall_start) * 1000.0



with state_lock:

    local_ok = ok_count

    local_fail = fail_count

    local_sum = sum_rtt_ms



print(f"[{CLIENT_ID}] DONE. requests={n} ok={local_ok} fail={local_fail}")

print(f"[{CLIENT_ID}] TOTAL_SUM_RTT_MS={local_sum:.3f}  WALL_MS={wall_ms:.3f}")



with open(LOCK_FILE, "a") as lf:

    fcntl.flock(lf, fcntl.LOCK_EX)

    with open(total_file, "a", buffering=1) as f:

        f.write(

            f"{CLIENT_ID},total_time_ms={local_sum:.3f},requests={n},ok={local_ok},fail={local_fail},"

            f"wall_ms={wall_ms:.3f},workers={workers},gap_mode={gap_mode}\n"

        )

    fcntl.flock(lf, fcntl.LOCK_UN)



write_shared_summary()