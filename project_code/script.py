import os
import subprocess
import time
import random
import shutil
import statistics as stats
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# ==========================================
# Experiment settings
# ==========================================
N_RUNS = 50

BASE_DIR = "/home/adan_alaa_mayas/final2"   # where alaa_topo.py, lb.py, client.py, server.py live
RESULTS_BASE_DIR = os.path.join(BASE_DIR, "results")  # client writes here via RESULTS_BASE
TOPO_FILE = os.path.join(BASE_DIR, "topo.py")
LB_FILE = os.path.join(BASE_DIR, "loadB.py")

# OpenFlow port: match what your Mininet RemoteController uses (Ryu default is 6653)
OF_PORT = "6653"

# Optional: gap inside each client (between requests) - only effective if client.py pick_gap() is enabled
#GAP_MIN = "0"
#GAP_MAX = "0"

GAP_MIN = "0.05"
GAP_MAX = "0.2"

# Safety timeouts
RYU_BOOT_SEC = 2
BETWEEN_RUNS_SEC = 1

# ==========================================
# Session output folder
# ==========================================
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
SIMULATION_DIR = os.path.join(os.getcwd(), f"simulation_{timestamp}")
os.makedirs(SIMULATION_DIR, exist_ok=True)
print(f"📂 All results for this session will be saved in: {SIMULATION_DIR}")

# ==========================================================
# Balanced workload generator
# - in each run choose k so that total per client is ~400-500
# - each client gets same counts of V/M/P, but randomized order
# ==========================================================
def generate_balanced_requests(k, seed=None):
    """
    Creates a workload string where counts of V, M, P are equal (k each).
    Total requests per client = 3k (k in [134..166] -> total in [402..498]).
    Order is randomized using seed.
    """
    items = []
    for j in range(1, k + 1):
        items.append(f"V{j}")
        items.append(f"M{j}")
        items.append(f"P{j}")

    rng = random.Random(seed)
    rng.shuffle(items)
    return "".join(items)

# ==========================================
# TCT calculation + archive status files
# ==========================================
def calculate_tct_and_archive(run_id):
    run_dir = os.path.join(RESULTS_BASE_DIR, run_id)
    if not os.path.exists(run_dir):
        print(f"⚠️ No run_dir found: {run_dir}")
        return None

    target_run_dir = os.path.join(SIMULATION_DIR, run_id)
    os.makedirs(target_run_dir, exist_ok=True)

    all_data = []
    for f in os.listdir(run_dir):
        if f.startswith("status_") and f.endswith(".txt"):
            src = os.path.join(run_dir, f)
            try:
                #df = pd.read_csv(src, names=["ts", "client", "type", "req", "status", "val"])
                df = pd.read_csv(src,header=None,names=["ts", "client", "type", "req", "status", "val1", "val2"],engine="python")
                df["ts"] = pd.to_datetime(df["ts"])
                all_data.append(df)
                shutil.move(src, os.path.join(target_run_dir, f))
            except Exception as e:
                print(f"⚠️ Error processing {f}: {e}")

    if not all_data:
        print(f"⚠️ No status files found for run_id={run_id}")
        return None

    combined = pd.concat(all_data, ignore_index=True)

    t_start = combined[combined["type"] == "GAP_BEFORE"]["ts"].min()
    if pd.isna(t_start):
        t_start = combined[combined["type"] == "RESULT"]["ts"].min()

    t_end = combined[combined["type"] == "RESULT"]["ts"].max()
    if pd.isna(t_start) or pd.isna(t_end):
        print(f"⚠️ Could not compute TCT for run_id={run_id} (missing timestamps)")
        return None

    return (t_end - t_start).total_seconds()

# ==========================================
# Run one Mininet + Ryu session
# topo argv: run_id algo_name req1 req2 req5
# ==========================================
def run_mininet_session(algo_name, run_id, req1, req2, req5):
    print(f"\n🚀 Launching session: {algo_name}, run_id={run_id}")
    print(f"   Workloads lengths: h1={len(req1)} | h2={len(req2)} | h5={len(req5)}")

    env = os.environ.copy()
    env["BASE_DIR"] = BASE_DIR
    env["RESULTS_BASE"] = RESULTS_BASE_DIR
    env["OF_PORT"] = OF_PORT

    #env["LB_ALGO"] = "hash" if algo_name == "IP-Hash" else "least"
    env["LB_ALGO"] = "IPHash" if algo_name == "IP-Hash" else "LeastConn"

    env["GAP_MIN"] = GAP_MIN
    env["GAP_MAX"] = GAP_MAX

    subprocess.run(["sudo", "mn", "-c"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["sudo", "pkill", "-f", "ryu-manager"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    ryu_cmd = [
        "sudo", "-E", "ryu-manager",
        "--ofp-tcp-listen-port", OF_PORT,
        LB_FILE
    ]
    ryu_proc = subprocess.Popen(
        ryu_cmd, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True
    )

    #ryu_proc = subprocess.Popen(ryu_cmd, env=env)

    time.sleep(RYU_BOOT_SEC)

    topo_cmd = [
        "sudo", "-E", "python3", TOPO_FILE,
        run_id, algo_name, req1, req2, req5
    ]
    subprocess.run(topo_cmd, env=env)

    try:
        ryu_proc.terminate()
        ryu_proc.wait(timeout=3)
    except Exception:
        ryu_proc.kill()

    time.sleep(BETWEEN_RUNS_SEC)

# ==========================================
# Main experiment loop (B)
# ==========================================
results = {"IP-Hash": [], "Least-Connections": []}
workloads_log = []

for i in range(1, N_RUNS + 1):
    print(f"\n--- Iteration {i} of {N_RUNS} ---")
    random.seed(i)

    # Choose k so total per client is ~400-500 (3k in [402..498])
    k = random.randint(134, 166)
    #k = random.randint(15, 20)


    # Same counts per client, but different random order per client (seed differs)
    req1 = generate_balanced_requests(k, seed=(i * 100 + 1))
    req2 = generate_balanced_requests(k, seed=(i * 100 + 2))
    req5 = generate_balanced_requests(k, seed=(i * 100 + 5))

    workloads_log.append({
        "iter": i,
        "k_per_type": k,
        "total_per_client": 3 * k,
        "h1": req1,
        "h2": req2,
        "h5": req5
    })

    run_id_hash = f"it{i}_hash"
    run_mininet_session("IP-Hash", run_id_hash, req1, req2, req5)
    tct_hash = calculate_tct_and_archive(run_id_hash)
    if tct_hash is not None:
        results["IP-Hash"].append(tct_hash)

    run_id_least = f"it{i}_least"
    run_mininet_session("Least-Connections", run_id_least, req1, req2, req5)
    tct_least = calculate_tct_and_archive(run_id_least)
    if tct_least is not None:
        results["Least-Connections"].append(tct_least)

pd.DataFrame(workloads_log).to_csv(os.path.join(SIMULATION_DIR, "workloads_used.csv"), index=False)

# ==========================================
# Summary + 3 plots
# ==========================================
if results["IP-Hash"] and results["Least-Connections"]:
    ip_vals = results["IP-Hash"]
    lc_vals = results["Least-Connections"]

    avg_hash = sum(ip_vals) / len(ip_vals)
    avg_least = sum(lc_vals) / len(lc_vals)

    std_hash = stats.stdev(ip_vals) if len(ip_vals) > 1 else 0.0
    std_least = stats.stdev(lc_vals) if len(lc_vals) > 1 else 0.0

    # ---------- Save summary ----------
    with open(os.path.join(SIMULATION_DIR, "summary.txt"), "w") as f:
        f.write(f"Simulation Date: {timestamp}\n")
        f.write(f"Number of Runs: {N_RUNS}\n")
        f.write(f"GAP_MIN={GAP_MIN}, GAP_MAX={GAP_MAX}\n")
        f.write(f"Average IP-Hash TCT: {avg_hash:.3f}s\n")
        f.write(f"Std IP-Hash TCT: {std_hash:.3f}s\n")
        f.write(f"Average Least-Connections TCT: {avg_least:.3f}s\n")
        f.write(f"Std Least-Connections TCT: {std_least:.3f}s\n")

    # ======================================================
    # Graph 1: Bar chart + std bars
    # ======================================================
    plt.figure(figsize=(8, 6))
    bars = plt.bar(
        ["IP-Hash", "Least-Connections"],
        [avg_hash, avg_least],
        yerr=[std_hash, std_least],
        capsize=6
    )
    plt.ylabel("Total Completion Time (sec)")
    plt.title(f"Graph 1: Average TCT over {N_RUNS} runs (avg ± std)")

    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval + 0.05, f"{yval:.2f}s", ha="center")

    graph1_path = os.path.join(SIMULATION_DIR, "graph1_avg_bar.png")
    plt.savefig(graph1_path)
    print(f"📊 Graph 1 saved to: {graph1_path}")
    plt.show()

    # ======================================================
    # Graph 2: Avg+Std + Boxplot
    # ======================================================
    plt.figure(figsize=(10, 6))

    plt.subplot(1, 2, 1)
    plt.bar(
        ["IP-Hash", "Least-Connections"],
        [avg_hash, avg_least],
        yerr=[std_hash, std_least],
        capsize=6
    )
    plt.ylabel("Total Completion Time (sec)")
    plt.title("Avg ± std")

    plt.subplot(1, 2, 2)
    plt.boxplot([ip_vals, lc_vals], labels=["IP-Hash", "Least-Connections"])
    plt.ylabel("Total Completion Time (sec)")
    plt.title("Boxplot (distribution)")

    plt.suptitle(f"Graph 2: Avg+Std + Boxplot over {N_RUNS} runs")
    plt.tight_layout(rect=[0, 0, 1, 0.93])

    graph2_path = os.path.join(SIMULATION_DIR, "graph2_avg_plus_boxplot.png")
    plt.savefig(graph2_path)
    print(f"📊 Graph 2 saved to: {graph2_path}")
    plt.show()

    # ======================================================
    # Graph 3: Avg+Std + Boxplot + Per-run plot
    # ======================================================
    plt.figure(figsize=(14, 5))

    plt.subplot(1, 3, 1)
    plt.bar(
        ["IP-Hash", "Least-Connections"],
        [avg_hash, avg_least],
        yerr=[std_hash, std_least],
        capsize=6
    )
    plt.ylabel("Total Completion Time (sec)")
    plt.title("Avg ± std")

    plt.subplot(1, 3, 2)
    plt.boxplot([ip_vals, lc_vals], labels=["IP-Hash", "Least-Connections"])
    plt.ylabel("Total Completion Time (sec)")
    plt.title("Boxplot")

    plt.subplot(1, 3, 3)
    runs_idx = list(range(1, min(len(ip_vals), len(lc_vals)) + 1))
    plt.plot(runs_idx, ip_vals[:len(runs_idx)], marker='o', label="IP-Hash")
    plt.plot(runs_idx, lc_vals[:len(runs_idx)], marker='o', label="Least-Connections")
    plt.xlabel("Run #")
    plt.ylabel("Total Completion Time (sec)")
    plt.title("Per-run TCT")
    plt.legend()

    plt.suptitle(f"Graph 3: Avg+Std + Boxplot + Per-run over {N_RUNS} runs")
    plt.tight_layout(rect=[0, 0, 1, 0.90])

    graph3_path = os.path.join(SIMULATION_DIR, "graph3_all.png")
    plt.savefig(graph3_path)
    print(f"📊 Graph 3 saved to: {graph3_path}")
    plt.show()

else:
    print("⚠️ Not enough data to plot. Check that status files are being created and archived.")