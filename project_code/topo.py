from mininet.topo import Topo
from mininet.net import Mininet
import sys
from mininet.node import RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel
from mininet.link import TCLink   # ✅ enable tc parameters
import time
import os
import datetime
from threading import Thread

VIP = "10.0.0.100"
PORT = 5000
BASE_DIR = "/home/adan_alaa_mayas/final2"
RESULTS_BASE = os.environ.get("RESULTS_BASE", os.path.join(BASE_DIR, "results"))
def stop_after_delay(net, delay=60):

    time.sleep(delay)
    print("\n*** Simulation finished after", delay, "seconds ***")
    net.stop()
    os._exit(0)

class ProjectTopo(Topo):
    def build(self):
        lb_switch = self.addSwitch('s1')
        # Clients
        client1 = self.addHost('h1', ip='10.0.0.1/24', mac='00:00:00:00:00:01')
        client2 = self.addHost('h2', ip='10.0.0.2/24', mac='00:00:00:00:00:02')
        client3 = self.addHost('h5', ip='10.0.0.5/24', mac='00:00:00:00:00:05')
        server1 = self.addHost('h3', ip='10.0.0.3/24', mac='00:00:00:00:00:03')
        server2 = self.addHost('h4', ip='10.0.0.4/24', mac='00:00:00:00:00:04')

        client_link = dict(bw=5, delay='5ms', loss=0, max_queue_size=500,use_htb=True)
        server_link_h3 = dict(bw=5, delay='7ms', loss=0, max_queue_size=500, use_htb=True)
        server_link_h4 = dict(bw=5, delay='7ms', loss=0, max_queue_size=500, use_htb=True)
        #server_link_h4 = dict(bw=5, delay='7ms', loss=0, max_queue_size=דד100, use_htb=True)

        self.addLink(server1, lb_switch, cls=TCLink, **server_link_h3)  # h3 (10.0.0.3)
        self.addLink(server2, lb_switch, cls=TCLink, **server_link_h4)  # h4 (10.0.0.4)
        self.addLink(client1, lb_switch, cls=TCLink, **client_link)
        self.addLink(client2, lb_switch, cls=TCLink, **client_link)
        self.addLink(client3, lb_switch, cls=TCLink, **client_link)

def run():
    print("*** Starting Mininet ***")
    if len(sys.argv) > 5:
        run_id = sys.argv[1]
        algo_name = sys.argv[2]
        req1 = sys.argv[3]
        req2 = sys.argv[4]
        req5 = sys.argv[5]
    else:
        run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        req1 = req2 = req5 = "V1,M1"

    print(f"*** RUN_ID = {run_id} ***")

    run_results_dir = os.path.join(RESULTS_BASE, run_id)
    os.makedirs(run_results_dir, exist_ok=True)
    print(f"*** RESULTS DIR = {run_results_dir} ***")

    print(f"*** Requests Received: H1:{req1} | H2:{req2} | H5:{req5} ***")

    topo = ProjectTopo()
    #net = Mininet(topo=topo, controller=RemoteController, link=TCLink)
    of_port = int(os.environ.get("OF_PORT", "6653"))
    net = Mininet(topo=topo,controller=lambda name: RemoteController(name, ip="127.0.0.1", port=of_port),
    link=TCLink)
    net.start()
    time.sleep(10)

    h1 = net.get('h1')
    h2 = net.get('h2')
    h5 = net.get('h5')
    h3 = net.get('h3')
    h4 = net.get('h4')

    # ---------- helpers ----------
    def clean_all():
        for h in [h1, h2, h5]:
            h.cmd('pkill -9 -f "python3 client.py" || true')

    #h3.cmd(f'cd {BASE_DIR} && RUN_ID={run_id} RESULTS_BASE="{RESULTS_BASE}" python3 server.py > /tmp/server_h3.log 2>&1 &')
    #h4.cmd(f'cd {BASE_DIR} && RUN_ID={run_id} RESULTS_BASE="{RESULTS_BASE}" python3 server.py > /tmp/server_h4.log 2>&1 &')
    def start_servers():
     #   print("*** Starting Servers ***")
      #  h3.cmd(f'cd {BASE_DIR} && RUN_ID={run_id} RESULTS_BASE="{RESULTS_BASE}" python3 server.py > /tmp/server_h3.log 2>&1 &')
       # h4.cmd(f'cd {BASE_DIR} && RUN_ID={run_id} RESULTS_BASE="{RESULTS_BASE}" python3 server.py > /tmp/server_h4.log 2>&1 &')
        #time.sleep(3)
        h3.cmd(f'cd {BASE_DIR} && RUN_ID={run_id} RESULTS_BASE="{RESULTS_BASE}" python3 -u server_slow.py > /tmp/server_h3.log 2>&1 &')
        h4.cmd(f'cd {BASE_DIR} && RUN_ID={run_id} RESULTS_BASE="{RESULTS_BASE}" python3 -u server_fast.py > /tmp/server_h4.log 2>&1 &')

    def stop_servers():
        #print("*** Stopping Servers ***")
        #h3.cmd('pkill -9 -f "python3 server.py" || true')
        #h4.cmd('pkill -9 -f "python3 server.py" || true')
        #h3.cmd('pkill -f "python3 -u server.py" || true')
        #h4.cmd('pkill -f "python3 -u server_fast.py" || true')
        time.sleep(1)
        h3.cmd('pkill -9 -f "python3 -u server_slow.py" || true')
        h4.cmd('pkill -9 -f "python3 -u server_fast.py" || true')



    def start_clients():
        print("*** Starting Clients with Individual Randomized Requests ***")
        gap_min = os.environ.get("GAP_MIN", "0.05")
        gap_max = os.environ.get("GAP_MAX", "0.20")
        h1.proc = h1.popen(
        f'cd {BASE_DIR} && RUN_ID={run_id} RESULTS_BASE="{RESULTS_BASE}" CLIENT_ID=h1 REQUESTS="{req1}" '
        f'GAP_MIN={gap_min} GAP_MAX={gap_max} MAX_IN_FLIGHT=1 python3 client.py > /tmp/client_h1.log 2>&1',
        shell=True)
        h2.proc = h2.popen(
        f'cd {BASE_DIR} && RUN_ID={run_id} RESULTS_BASE="{RESULTS_BASE}" CLIENT_ID=h2 REQUESTS="{req2}" '
        f'GAP_MIN={gap_min} GAP_MAX={gap_max} MAX_IN_FLIGHT=1 python3 client.py > /tmp/client_h2.log 2>&1',
        shell=True)
        h5.proc = h5.popen(
        f'cd {BASE_DIR} && RUN_ID={run_id} RESULTS_BASE="{RESULTS_BASE}" CLIENT_ID=h5 REQUESTS="{req5}" '
        f'GAP_MIN={gap_min} GAP_MAX={gap_max} MAX_IN_FLIGHT=1 python3 client.py > /tmp/client_h5.log 2>&1',
        shell=True)
           
    def stop_clients():
        print("*** Stopping Clients ***")
        h1.cmd('pkill -9 -f "python3 client.py" || true')
        h2.cmd('pkill -9 -f "python3 client.py" || true')
        h5.cmd('pkill -9 -f "python3 client.py" || true')

    def _ok(label, cond, extra_ok="", extra_fail=""):
        if cond:
            print(f"[PASS] {label} {extra_ok}".rstrip())
        else:
            print(f"[FAIL] {label} {extra_fail}".rstrip())

    def status():
        print("=== STATUS ===")
        print("h1 clients:\n", h1.cmd('pgrep -af "python3 client.py" || true').strip() or "none")
        print("h2 clients:\n", h2.cmd('pgrep -af "python3 client.py" || true').strip() or "none")
        print("h5 clients:\n", h5.cmd('pgrep -af "python3 client.py" || true').strip() or "none")
        print("h3 server:\n", h3.cmd('pgrep -af "python3 server_slow.py" || true').strip() or "none")
        print("h4 server:\n", h4.cmd('pgrep -af "python3 server_fast.py" || true').strip() or "none")
        print("\nListening on 5000:")
        print("h3:\n", h3.cmd('ss -ltnp | grep :5000 || true').strip() or "not listening")
        print("h4:\n", h4.cmd('ss -ltnp | grep :5000 || true').strip() or "not listening")
        print("\nTC qdisc (show shaping):")
        print("h1:\n", h1.cmd('tc qdisc show').strip() or "no tc")
        print("h3:\n", h3.cmd('tc qdisc show').strip() or "no tc")

    def logs():
        print("=== LOGS (last 15 lines) ===")
        print("--- h3 server ---")
        print(h3.cmd('tail -n 15 /tmp/server_h3.log 2>/dev/null || echo "no log"'))
        print("--- h4 server ---")
        print(h4.cmd('tail -n 15 /tmp/server_h4.log 2>/dev/null || echo "no log"'))
        print("--- h1 client ---")
        print(h1.cmd('tail -n 15 /tmp/client_h1.log 2>/dev/null || echo "no log"'))
        print("--- h2 client ---")
        print(h2.cmd('tail -n 15 /tmp/client_h2.log 2>/dev/null || echo "no log"'))
        print("--- h5 client ---")
        print(h5.cmd('tail -n 15 /tmp/client_h5.log 2>/dev/null || echo "no log"'))

    def _ping(host, ip):
        out = host.cmd(f'ping -c 1 -W 1 {ip} >/dev/null 2>&1; echo $?').strip()
        return out == "0"

    def _tcp(host, ip, port):
        cmd = f'bash -lc "</dev/tcp/{ip}/{port}" >/dev/null 2>&1; echo $?'
        rc = host.cmd(cmd).strip()
        return rc == "0"

    def checkvip():
        print("=== CHECK VIP ===")
        _ok("Ping VIP from h1", _ping(h1, VIP), extra_fail="(VIP not reachable)")
        _ok("TCP VIP:5000 from h1", _tcp(h1, VIP, PORT), extra_fail="(VIP:5000 not reachable)")
        _ok("Ping VIP from h2", _ping(h2, VIP), extra_fail="(VIP not reachable)")
        _ok("TCP VIP:5000 from h2", _tcp(h2, VIP, PORT), extra_fail="(VIP:5000 not reachable)")
        _ok("Ping VIP from h5", _ping(h5, VIP), extra_fail="(VIP not reachable)")
        _ok("TCP VIP:5000 from h5", _tcp(h5, VIP, PORT), extra_fail="(VIP:5000 not reachable)")

    def check():
        print("=== CHECK ALL ===")
        print("-> Temporarily stopping clients for clean checks")
        stop_clients()
        time.sleep(1)
        _ok("Ping h3 (10.0.0.3) from h1", _ping(h1, "10.0.0.3"))
        _ok("Ping h4 (10.0.0.4) from h1", _ping(h1, "10.0.0.4"))
        listen_h3 = "LISTEN" in h3.cmd('ss -ltn | grep :5000 || true')
        listen_h4 = "LISTEN" in h4.cmd('ss -ltn | grep :5000 || true')
        _ok("h3 listening on :5000", listen_h3)
        _ok("h4 listening on :5000", listen_h4)
        _ok("TCP 10.0.0.3:5000 from h1", _tcp(h1, "10.0.0.3", PORT))
        _ok("TCP 10.0.0.4:5000 from h1", _tcp(h1, "10.0.0.4", PORT))
        _ok("Ping VIP from h1", _ping(h1, VIP))
        _ok("TCP VIP:5000 from h1", _tcp(h1, VIP, PORT))
        print("-> Restarting clients")
        start_clients()
        print("=== DONE ===")

   # ---------- AUTO EXECUTION FLOW ----------
    clean_all()
    start_servers()
    start_clients()
    print("*** Simulation in progress. Waiting for clients to complete requests... ***")

    # המתנה לסיום של כל לקוח בנפרד
    h1.proc.wait()
    h2.proc.wait()
    h5.proc.wait()

    print("*** All clients finished their tasks. ***")
    
    # סגירה מסודרת
    stop_servers()
    net.stop()
    print("*** Mininet stopped. Process complete. ***")


if __name__ == '__main__':
    setLogLevel('info')
    run()