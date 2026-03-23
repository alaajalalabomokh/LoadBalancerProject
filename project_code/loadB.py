from ryu.base import app_manager
import os

from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, ether_types, arp, ipv4, tcp
from ryu.lib import hub

VIP_IP = '10.0.0.100'
VIP_MAC = '00:00:00:00:00:AA'
SERVICE_PORT = 5000

SERVER_IPS = {'10.0.0.3': 1, '10.0.0.4': 2}
ID_TO_IP = {1: '10.0.0.3', 2: '10.0.0.4'}
SERVER_MACS = {'10.0.0.3': '00:00:00:00:00:03', '10.0.0.4': '00:00:00:00:00:04'}

# --- TCP flag bits ---
TCP_FIN = 0x01
TCP_RST = 0x04


class IPHashBalancer:
    def __init__(self):
        self.flow_counts = {ip: 0 for ip in SERVER_IPS}

    def get_server(self, client_ip, live_servers):
        if not live_servers or not client_ip:
            return None

        servers = sorted(live_servers)

        try:
            ip_last_digit = int(client_ip.split('.')[-1])
        except (ValueError, IndexError):
            return None

        index = ip_last_digit % len(servers)
        selected_server = servers[index]

        self.flow_counts[selected_server] += 1
        #print(f"[IP-Hash] Client {client_ip} (Last Digit: {ip_last_digit}) -> Index {index} -> {selected_server}")
        return selected_server

    def release_server(self, server_ip):
        if server_ip in self.flow_counts and self.flow_counts[server_ip] > 0:
            self.flow_counts[server_ip] -= 1
            #print(f"[Release IP-Hash] {server_ip}. Active flows: {self.flow_counts[server_ip]}")


class LeastConnectionsBalancer:
    def __init__(self):
        self.flow_counts = {ip: 0 for ip in SERVER_IPS}

    def get_server(self, client_ip, live_servers):
        if not live_servers:
            return None

        candidates = {ip: self.flow_counts[ip] for ip in live_servers}
        #print(f"[LeastConn] candidates={candidates}")

        best_server = min(candidates, key=candidates.get)
        self.flow_counts[best_server] += 1

        #print(f"[LeastConn] Selected {best_server}. Loads: {self.flow_counts}")
        return best_server

    def release_server(self, server_ip):
        if server_ip in self.flow_counts and self.flow_counts[server_ip] > 0:
            self.flow_counts[server_ip] -= 1
            #print(f"[Release] {server_ip}. Loads: {self.flow_counts}")


class LoadBalancer(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(LoadBalancer, self).__init__(*args, **kwargs)

        self.mac_to_port = {}
        self.live_servers = list(SERVER_IPS.keys())

        algo_choice = os.environ.get("LB_ALGO", "LeastConn")
        if algo_choice == "IPHash":
            self.algorithm = IPHashBalancer()
        else:
            self.algorithm = LeastConnectionsBalancer()

        #print(
        #    f"[LB] ENV LB_ALGO={os.environ.get('LB_ALGO')} "
        #    f"-> using {type(self.algorithm).__name__}",
        #    flush=True
        #)

        # flow_key = (client_ip, client_src_port) -> server_ip
        self.flow_to_server = {}

        # Count how many NEW flows were assigned to each server
        self.assign_counts = {ip: 0 for ip in SERVER_IPS}

        self.monitor_thread = hub.spawn(self._monitor_health)

    # ---------- helpers ----------
    def _get_tcp_flags(self, p_tcp):
        """
        Ryu יכול לחשוף flags בשם bits/flag/flags לפי גרסה.
        נחזיר int או None.
        """
        for attr in ("bits", "flag", "flags"):
            if hasattr(p_tcp, attr):
                try:
                    return int(getattr(p_tcp, attr))
                except Exception:
                    pass
        return None

    def add_flow(self, datapath, priority, match, actions,
                 idle_timeout=0, hard_timeout=0, cookie=0, send_flow_removed=True):
        ofp = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, actions)]
        flags = ofp.OFPFF_SEND_FLOW_REM if send_flow_removed else 0

        mod = parser.OFPFlowMod(
            datapath=datapath,
            priority=priority,
            match=match,
            instructions=inst,
            idle_timeout=idle_timeout,
            hard_timeout=hard_timeout,
            cookie=cookie,
            flags=flags
        )
        datapath.send_msg(mod)

    def _l2_forward(self, dp, msg, in_port, eth):
        """Learning-switch fallback so ping and non-VIP traffic work."""
        ofp = dp.ofproto
        parser = dp.ofproto_parser

        dst = eth.dst
        out_port = self.mac_to_port.get(dst, ofp.OFPP_FLOOD)
        actions = [parser.OFPActionOutput(out_port)]

        match = parser.OFPMatch(eth_src=eth.src, eth_dst=dst)
        self.add_flow(dp, 1, match, actions, idle_timeout=30, send_flow_removed=False)

        data = None if msg.buffer_id != ofp.OFP_NO_BUFFER else msg.data
        out = parser.OFPPacketOut(
            datapath=dp,
            buffer_id=msg.buffer_id,
            in_port=in_port,
            actions=actions,
            data=data
        )
        dp.send_msg(out)

    def _handle_arp_reply(self, dp, port, pkt_arp):
        parser = dp.ofproto_parser
        ofp = dp.ofproto

        pkt = packet.Packet()
        pkt.add_protocol(ethernet.ethernet(
            ethertype=ether_types.ETH_TYPE_ARP,
            dst=pkt_arp.src_mac, src=VIP_MAC
        ))
        pkt.add_protocol(arp.arp(
            opcode=arp.ARP_REPLY,
            src_mac=VIP_MAC, src_ip=VIP_IP,
            dst_mac=pkt_arp.src_mac, dst_ip=pkt_arp.src_ip
        ))
        pkt.serialize()

        actions = [parser.OFPActionOutput(port)]
        out = parser.OFPPacketOut(
            datapath=dp,
            buffer_id=ofp.OFP_NO_BUFFER,
            in_port=ofp.OFPP_CONTROLLER,
            actions=actions,
            data=pkt.data
        )
        dp.send_msg(out)

    def _install_fin_rst_flows(self, dp, client_ip, client_port, server_ip, server_mac,
                               server_out_port, client_out_port, cookie):
        """
        מתקין 4 flows (FIN/RST קדימה + FIN/RST אחורה) עם priority גבוה,
        ששולחים עותק ל-controller וגם מעבירים את החבילה רגיל.
        """
        ofp = dp.ofproto
        parser = dp.ofproto_parser

        # ----- Forward (client -> VIP) FIN/RST -----
        base_match_fwd = dict(
            eth_type=0x0800, ip_proto=6,
            ipv4_src=client_ip, ipv4_dst=VIP_IP,
            tcp_src=client_port, tcp_dst=SERVICE_PORT
        )

        base_actions_fwd = [
            parser.OFPActionSetField(ipv4_dst=server_ip),
            parser.OFPActionSetField(eth_dst=server_mac),
            parser.OFPActionOutput(server_out_port)
        ]

        # FIN
        match_fin = parser.OFPMatch(**base_match_fwd, tcp_flags=(TCP_FIN, TCP_FIN))
        actions_fin = [parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)] + base_actions_fwd
        self.add_flow(dp, 20, match_fin, actions_fin, idle_timeout=30, cookie=cookie, send_flow_removed=True)

        # RST
        match_rst = parser.OFPMatch(**base_match_fwd, tcp_flags=(TCP_RST, TCP_RST))
        actions_rst = [parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)] + base_actions_fwd
        self.add_flow(dp, 20, match_rst, actions_rst, idle_timeout=30, cookie=cookie, send_flow_removed=True)

        # ----- Reverse (server -> client) FIN/RST -----
        base_match_rev = dict(
            eth_type=0x0800, ip_proto=6,
            ipv4_src=server_ip, ipv4_dst=client_ip,
            tcp_src=SERVICE_PORT, tcp_dst=client_port
        )

        base_actions_rev = [
            parser.OFPActionSetField(ipv4_src=VIP_IP),
            parser.OFPActionSetField(eth_src=VIP_MAC),
            parser.OFPActionOutput(client_out_port)
        ]

        match_rev_fin = parser.OFPMatch(**base_match_rev, tcp_flags=(TCP_FIN, TCP_FIN))
        actions_rev_fin = [parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)] + base_actions_rev
        self.add_flow(dp, 20, match_rev_fin, actions_rev_fin, idle_timeout=30, cookie=cookie, send_flow_removed=True)

        match_rev_rst = parser.OFPMatch(**base_match_rev, tcp_flags=(TCP_RST, TCP_RST))
        actions_rev_rst = [parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)] + base_actions_rev
        self.add_flow(dp, 20, match_rev_rst, actions_rev_rst, idle_timeout=30, cookie=cookie, send_flow_removed=True)

    # ------------------------- Release only on forward FIN/RST -------------------------
    def _release_by_packet(self, p_ip, p_tcp):
        """
        משחרר מונה לפי PacketIn (FIN/RST).

        תיקון:
        - משחררים *רק* על forward FIN/RST (client -> VIP:5000).
        - Reverse FIN/RST רק "נבלע" (handled=True) כדי שלא ייפול ל-L2 forwarding,
          אבל לא עושים release.
        """
        flags = self._get_tcp_flags(p_tcp)
        if flags is None:
            return False

        if not ((flags & TCP_FIN) or (flags & TCP_RST)):
            return False

        # forward: client -> VIP
        if p_ip.dst == VIP_IP and p_tcp.dst_port == SERVICE_PORT:
            flow_key = (p_ip.src, p_tcp.src_port)
            server_ip = self.flow_to_server.pop(flow_key, None)

            if server_ip:
                self.algorithm.release_server(server_ip)
                #print(f"[FIN/RST] Closed {flow_key} -> released {server_ip}")
            #else:
                #print(f"[FIN/RST] Closed {flow_key} but mapping not found (already released?)")
            return True

        # reverse FIN/RST: swallow but do NOT release
        #print(f"[FIN/RST] Reverse FIN/RST seen (ignored). src={p_ip.src} dst={p_ip.dst} dport={p_tcp.dst_port}")
        return True

    # ---------- Ryu events ----------
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        dp = ev.msg.datapath
        ofp = dp.ofproto
        parser = dp.ofproto_parser

        # table-miss -> controller
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)]
        self.add_flow(dp, 0, match, actions, send_flow_removed=False)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        dp = msg.datapath
        ofp = dp.ofproto
        parser = dp.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return

        # MAC learning
        self.mac_to_port[eth.src] = in_port

        # ---- ARP ----
        p_arp = pkt.get_protocol(arp.arp)
        if p_arp:
            if p_arp.dst_ip == VIP_IP:
                #print(f"[ARP] Client {p_arp.src_ip} asked for VIP")
                self._handle_arp_reply(dp, in_port, p_arp)
                return

            self._l2_forward(dp, msg, in_port, eth)
            return

        # ---- IPv4 ----
        p_ip = pkt.get_protocol(ipv4.ipv4)
        if p_ip:
            p_tcp = pkt.get_protocol(tcp.tcp)

            # FIN/RST handling first
            if p_tcp and self._release_by_packet(p_ip, p_tcp):
                return

            # VIP TCP load-balance only
            if p_tcp and p_ip.dst == VIP_IP and p_tcp.dst_port == SERVICE_PORT:
                client_ip = p_ip.src
                client_port = p_tcp.src_port
                flow_key = (client_ip, client_port)

                #print(f"\n[PACKET] Client-flow: {client_ip}:{client_port} -> {VIP_IP}:{SERVICE_PORT}")

                # prevent double increment on retransmits
                server_ip = self.flow_to_server.get(flow_key)
                new_flow = False

                if not server_ip:
                    server_ip = self.algorithm.get_server(client_ip, self.live_servers)
                    if not server_ip:
                        return

                    self.flow_to_server[flow_key] = server_ip
                    new_flow = True

                    # Count only NEW assignments
                    self.assign_counts[server_ip] += 1
                    print(f"[ASSIGN] {flow_key} -> {server_ip} | assign_counts={self.assign_counts}")

                server_mac = SERVER_MACS[server_ip]
                server_id = SERVER_IPS[server_ip]

                server_out_port = self.mac_to_port.get(server_mac, ofp.OFPP_FLOOD)
                client_out_port = self.mac_to_port.get(eth.src, ofp.OFPP_FLOOD)

                # Forward rule (per client flow)
                match = parser.OFPMatch(
                    eth_type=0x0800,
                    ip_proto=6,
                    ipv4_src=client_ip,
                    ipv4_dst=VIP_IP,
                    tcp_src=client_port,
                    tcp_dst=SERVICE_PORT
                )

                actions = [
                    parser.OFPActionSetField(ipv4_dst=server_ip),
                    parser.OFPActionSetField(eth_dst=server_mac),
                    parser.OFPActionOutput(server_out_port)
                ]
                self.add_flow(dp, 10, match, actions, idle_timeout=30, cookie=server_id, send_flow_removed=True)

                # Reverse rule
                match_rev = parser.OFPMatch(
                    eth_type=0x0800,
                    ip_proto=6,
                    ipv4_src=server_ip,
                    ipv4_dst=client_ip,
                    tcp_src=SERVICE_PORT,
                    tcp_dst=client_port
                )

                actions_rev = [
                    parser.OFPActionSetField(ipv4_src=VIP_IP),
                    parser.OFPActionSetField(eth_src=VIP_MAC),
                    parser.OFPActionOutput(client_out_port)
                ]
                self.add_flow(dp, 10, match_rev, actions_rev, idle_timeout=30, cookie=server_id, send_flow_removed=True)

                # FIN/RST flows (priority גבוה)
                self._install_fin_rst_flows(
                    dp, client_ip, client_port,
                    server_ip, server_mac,
                    server_out_port, client_out_port,
                    cookie=server_id
                )

                # Send first packet (PacketOut)
                data = None if msg.buffer_id != ofp.OFP_NO_BUFFER else msg.data
                out = parser.OFPPacketOut(
                    datapath=dp,
                    buffer_id=msg.buffer_id,
                    in_port=in_port,
                    actions=actions,
                    data=data
                )
                dp.send_msg(out)

                if new_flow:
                    #print(f"[LB] Redirected {client_ip}:{client_port} -> {server_ip} (installed flows).")
                    pass
                else:
                    #print(f"[LB] Existing mapping {client_ip}:{client_port} -> {server_ip} (reinstalled flows).")
                    pass
                return

            # non-VIP IPv4: normal L2 forwarding
            self._l2_forward(dp, msg, in_port, eth)
            return

        # everything else: L2 forwarding
        self._l2_forward(dp, msg, in_port, eth)

    def _monitor_health(self):
        while True:
            hub.sleep(10)
            #print(f"[HealthCheck] Alive servers: {self.live_servers} | assigned={self.assign_counts}")

    # ------------------------- FlowRemoved fallback only for forward VIP flow -------------------------
    @set_ev_cls(ofp_event.EventOFPFlowRemoved, MAIN_DISPATCHER)
    def flow_removed_handler(self, ev):
        try:
            m = ev.msg.match

            # Only forward VIP flow: client -> VIP:5000
            if m.get('ipv4_dst') != VIP_IP or m.get('tcp_dst') != SERVICE_PORT:
                return

            client_ip = m.get('ipv4_src')
            client_port = m.get('tcp_src')
            if not (client_ip and client_port):
                return

            flow_key = (client_ip, client_port)
            server_ip = self.flow_to_server.pop(flow_key, None)

            if server_ip:
                self.algorithm.release_server(server_ip)
                #print(f"[FlowRemoved fallback] released {server_ip} for {flow_key} | assign_counts={self.assign_counts}")

        except Exception:
            pass