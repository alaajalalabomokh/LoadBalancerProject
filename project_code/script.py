import os
import subprocess
import time
import random
import shutil
import statistics as stats
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

N_RUNS = 1

BASE_DIR = "/home/adan_alaa_mayas/final2"
RESULTS_BASE_DIR = os.path.join(BASE_DIR, "results")
TOPO_FILE = os.path.join(BASE_DIR, "topo.py")
LB_FILE = os.path.join(BASE_DIR, "loadB.py")

OF_PORT = "6653"

GAP_MIN = "0"
GAP_MAX = "0"

RYU_BOOT_SEC = 2
BETWEEN_RUNS_SEC = 1

ALGORITHMS = [
    ("IP-Hash", "IPHash", "hash"),
    ("Least-Connections", "LeastConn", "least"),
    ("Least-Connections-Second-Tie", "LeastConnSecondTie", "least2"),
]

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
SIMULATION_DIR = os.path.join(os.getcwd(), f"simulation_{timestamp}")
os.makedirs(SIMULATION_DIR, exist_ok=True)
print(f"📂 All results for this session will be saved in: {SIMULATION_DIR}")


def generate_balanced_requests(k, seed=None):
    items = []
    for j in range(1, k + 1):
        items.append(f"V{j}")
        items.append(f"M{j}")
        items.append(f"P{j}")

    rng = random.Random(seed)
    rng.shuffle(items)
    return "".join(items)


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
                df = pd.read_csv(
                    src,
                    header=None,
                    names=["ts", "client", "type", "req", "status", "val1", "val2"],
                    engine="python"
                )
                df["ts"] = pd.to_datetime(df["ts"])
                all_data.append(df)
                shutil.move(src, os.path.join(target_run_dir, f))
            except Exception as e:
                print(f"⚠️ Error processing {f}: {e}")

    assign_src = os.path.join(run_dir, f"assignments_{run_id}.txt")
    if os.path.exists(assign_src):
        shutil.move(assign_src, os.path.join(target_run_dir, f"assignments_{run_id}.txt"))

    one_line_src = os.path.join(run_dir, "assignments_summary.txt")
    if os.path.exists(one_line_src):
        session_summary = os.path.join(SIMULATION_DIR, "all_assignments_summary.txt")
        with open(one_line_src, "r") as src_f, open(session_summary, "a") as dst_f:
            dst_f.write(src_f.read())
        os.remove(one_line_src)

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


def run_mininet_session(algo_name, lb_algo_env, run_id, req1, req2, req5):
    print(f"\n🚀 Launching session: {algo_name}, run_id={run_id}")
    print(f"   Workloads lengths: h1={len(req1)} | h2={len(req2)} | h5={len(req5)}")

    env = os.environ.copy()
    env["BASE_DIR"] = BASE_DIR
    env["RESULTS_BASE"] = RESULTS_BASE_DIR
    env["OF_PORT"] = OF_PORT
    env["LB_ALGO"] = lb_algo_env
    env["RUN_ID"] = run_id
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
        ryu_cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

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


results = {algo_name: [] for algo_name, _, _ in ALGORITHMS}
workloads_log = []

for i in range(1, N_RUNS + 1):
    print(f"\n--- Iteration {i} of {N_RUNS} ---")
    random.seed(i)

    k = random.randint(60, 75)

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

    for algo_name, lb_algo_env, suffix in ALGORITHMS:
        run_id = f"it{i}_{suffix}_{datetime.now().strftime('%H%M%S_%f')}"
        run_mininet_session(algo_name, lb_algo_env, run_id, req1, req2, req5)
        tct = calculate_tct_and_archive(run_id)
        if tct is not None:
            results[algo_name].append(tct)

pd.DataFrame(workloads_log).to_csv(
    os.path.join(SIMULATION_DIR, "workloads_used.csv"),
    index=False
)

summary_src = os.path.join(SIMULATION_DIR, "all_assignments_summary.txt")
pretty_dst = os.path.join(SIMULATION_DIR, "pretty_assignments_summary.txt")

if os.path.exists(summary_src):
    runs_map = {}

    with open(summary_src, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = {}
            for item in line.split(","):
                k, v = item.split("=", 1)
                parts[k.strip()] = v.strip()

            run_id = parts["run_id"]
            algo = parts["algorithm"]
            h3 = parts["h3"]
            h4 = parts["h4"]

            iter_num = run_id.split("_")[0].replace("it", "")
            runs_map.setdefault(iter_num, {})
            runs_map[iter_num][algo] = (h3, h4)

    with open(pretty_dst, "w") as f:
        for iter_num in sorted(runs_map, key=lambda x: int(x)):
            f.write(f"Run {iter_num}:\n")

            for algo in ["IPHash", "LeastConn", "LeastConnSecondTie"]:
                if algo in runs_map[iter_num]:
                    h3, h4 = runs_map[iter_num][algo]

                    if algo == "IPHash":
                        label = "IP-Hash"
                    elif algo == "LeastConn":
                        label = "Least-Connections"
                    else:
                        label = "Least-Connections-Second-Tie"

                    f.write(f"{label}: h3={h3}, h4={h4}\n")

            f.write("\n")

if all(len(results[name]) > 0 for name in results):
    algo_names = list(results.keys())
    avg_vals = [sum(results[name]) / len(results[name]) for name in algo_names]
    std_vals = [stats.stdev(results[name]) if len(results[name]) > 1 else 0.0 for name in algo_names]

    with open(os.path.join(SIMULATION_DIR, "summary.txt"), "w") as f:
        f.write(f"Simulation Date: {timestamp}\n")
        f.write(f"Number of Runs: {N_RUNS}\n")
        f.write(f"GAP_MIN={GAP_MIN}, GAP_MAX={GAP_MAX}\n")
        for name, avg, std in zip(algo_names, avg_vals, std_vals):
            f.write(f"Average {name} TCT: {avg:.3f}s\n")
            f.write(f"Std {name} TCT: {std:.3f}s\n")

    plt.figure(figsize=(9, 6))
    bars = plt.bar(algo_names, avg_vals, yerr=std_vals, capsize=6)
    plt.ylabel("Total Completion Time (sec)")
    plt.title(f"Graph 1: Average TCT over {N_RUNS} runs (avg ± std)")
    plt.xticks(rotation=10)

    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, yval + 0.05, f"{yval:.2f}s", ha="center")

    graph1_path = os.path.join(SIMULATION_DIR, "graph1_avg_bar.png")
    plt.savefig(graph1_path)
    print(f"📊 Graph 1 saved to: {graph1_path}")
    plt.show()

    plt.figure(figsize=(12, 6))

    plt.subplot(1, 2, 1)
    plt.bar(algo_names, avg_vals, yerr=std_vals, capsize=6)
    plt.ylabel("Total Completion Time (sec)")
    plt.title("Avg ± std")
    plt.xticks(rotation=10)

    plt.subplot(1, 2, 2)
    plt.boxplot([results[name] for name in algo_names], labels=algo_names)
    plt.ylabel("Total Completion Time (sec)")
    plt.title("Boxplot (distribution)")
    plt.xticks(rotation=10)

    plt.suptitle(f"Graph 2: Avg+Std + Boxplot over {N_RUNS} runs")
    plt.tight_layout(rect=[0, 0, 1, 0.93])

    graph2_path = os.path.join(SIMULATION_DIR, "graph2_avg_plus_boxplot.png")
    plt.savefig(graph2_path)
    print(f"📊 Graph 2 saved to: {graph2_path}")
    plt.show()

    plt.figure(figsize=(16, 5))

    plt.subplot(1, 3, 1)
    plt.bar(algo_names, avg_vals, yerr=std_vals, capsize=6)
    plt.ylabel("Total Completion Time (sec)")
    plt.title("Avg ± std")
    plt.xticks(rotation=10)

    plt.subplot(1, 3, 2)
    plt.boxplot([results[name] for name in algo_names], labels=algo_names)
    plt.ylabel("Total Completion Time (sec)")
    plt.title("Boxplot")
    plt.xticks(rotation=10)

    plt.subplot(1, 3, 3)
    for name in algo_names:
        runs_idx = list(range(1, len(results[name]) + 1))
        plt.plot(runs_idx, results[name], marker='o', label=name)

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