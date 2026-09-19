import asyncio
import logging
import threading
import time
from typing import List, Dict, Any, Optional

try:
    from tplink_omada_client import OmadaClient
except ImportError:
    OmadaClient = None  # Will be caught at runtime if library missing

logger = logging.getLogger('omada')

# Global caches for Omada data
_omada_data_lock = threading.Lock()
_omada_sites: List[Dict[str, Any]] = []
_omada_devices: List[Dict[str, Any]] = []
_omada_clients: List[Dict[str, Any]] = []
_omada_nodes: List[Dict[str, Any]] = []  # Normalized dashboard node dictionaries

# Thread control flags
_stop_omada_thread = False
_omada_thread: threading.Thread = None


def _normalize_speed(speed_val: Any) -> str:
    """Normalize speed from enum or string into standard dashboard notation (e.g. 1000M)."""
    if speed_val is None:
        return ""
    s = str(speed_val).upper()
    if "10000" in s or "10G" in s:
        return "10G"
    if "2500" in s or "2.5G" in s:
        return "2.5G"
    if "1000" in s or "1G" in s:
        return "1000M"
    if "100" in s:
        return "100M"
    if "10" in s:
        return "10M"
    return str(speed_val)


def _safe_get_uplink(dev: Any) -> Optional[Dict[str, Any]]:
    """Safely extracts uplink dictionary from an Omada device without triggering KeyError in tplink_omada_client."""
    if not dev:
        return None
    try:
        # Check raw device dict first (_data) to bypass buggy properties in OmadaLink
        raw_dev = getattr(dev, "_data", None)
        if isinstance(raw_dev, dict):
            for up_key in ("uplink", "wiredUplink", "wirelessUplink"):
                raw_uplink = raw_dev.get(up_key)
                if isinstance(raw_uplink, dict):
                    mac = raw_uplink.get("mac") or raw_uplink.get("uplinkMac") or ""
                    port = raw_uplink.get("port") or raw_uplink.get("uplinkPort") or raw_uplink.get("portNumber")
                    name = raw_uplink.get("name") or raw_uplink.get("deviceName") or ""
                    if mac or name or port is not None:
                        return {"mac": str(mac), "port": port, "name": str(name)}

        # Fallback to device object's uplink attributes
        for attr in ("wired_uplink", "uplink"):
            try:
                up_obj = getattr(dev, attr, None)
            except (KeyError, Exception):
                up_obj = None
            if not up_obj:
                continue

            up_dict = {"mac": "", "port": None, "name": ""}
            up_raw = getattr(up_obj, "_data", None)
            if isinstance(up_raw, dict):
                up_dict["mac"] = str(up_raw.get("mac") or up_raw.get("uplinkMac") or "")
                up_dict["port"] = up_raw.get("port") or up_raw.get("uplinkPort") or up_raw.get("portNumber")
                up_dict["name"] = str(up_raw.get("name") or up_raw.get("deviceName") or "")
            else:
                for k in ("mac", "name", "port"):
                    try:
                        val = getattr(up_obj, k, None)
                        if val is not None:
                            up_dict[k] = val
                    except (KeyError, Exception):
                        pass

            if up_dict["mac"] or up_dict["name"] or up_dict["port"] is not None:
                return up_dict
    except Exception as e:
        logger.debug(f"Error extracting uplink safely: {e}")
    return None


def _get_client_ap_port(c: Dict[str, Any]) -> str:
    """Determine the AP port identifier for a client."""
    # If wired client connected to an AP downlink port
    if not c.get("wireless", False):
        p = c.get("port")
        if p is not None and str(p).strip() and str(p) != "-1":
            p_str = str(p).strip().upper()
            if p_str in ("0", "UPLINK", "ETH0", "WAN"):
                return ""
            if p_str.startswith("ETH") or p_str.startswith("LAN"):
                digits = "".join(filter(str.isdigit, p_str))
                if digits:
                    return f"ETH{digits}"
                return p_str
            # Integer or numeric port
            try:
                p_int = int(p_str)
                # On wall APs, port 1 is the Uplink (PoE In) and ports 2..4 are ETH1..ETH3.
                # If Omada reports port 1, do NOT map to ETH1 because port 1 is the Uplink.
                if p_int > 1:
                    return f"ETH{p_int - 1}"
                elif p_int == 1:
                    return ""
            except ValueError:
                return p_str
        return ""

    # Wireless client: check SSID first (each SSID is a separate port)
    ssid = str(c.get("ssid") or "").strip()
    if ssid:
        return f"SSID: {ssid}"

    # Fallback to radio_id (0=2.4G, 1=5G, 2=5G-2, 3=6G)
    rid = c.get("radio_id")
    if rid == 0:
        return "2.4G"
    elif rid == 1:
        return "5G"
    elif rid == 2:
        return "5G-2"
    elif rid == 3:
        return "6G"

    ch = c.get("channel")
    if ch is not None:
        try:
            ch_num = int(ch)
            if 1 <= ch_num <= 14:
                return "2.4G"
            elif 36 <= ch_num <= 177:
                return "5G"
            elif ch_num >= 180:
                return "6G"
        except (ValueError, TypeError):
            pass

    return "WLAN"


def _is_ssid_enabled(item: Any, is_override: bool = False) -> bool:
    """
    Determine if an SSID entry is enabled.
    
    In Omada SDN Controller:
    - In 'ssidOverrides' and 'wlanOverrides', 'enable' indicates whether the device-level
      override is active (False = inherit site defaults). The actual broadcast state of the SSID
      is controlled by 'ssidEnable' (or 'enableSsid' / 'broadcast').
      If 'ssidEnable' is not explicitly False, the SSID is broadcasting and active.
    - If 'is_override' is True or 'wlanId' is in item, 'enable: False' does NOT disable the SSID;
      it simply means the SSID inherits global site configuration (enabled).
    """
    if not isinstance(item, dict):
        return True

    # Check explicit disable flags
    if item.get("ssidEnable") is False or item.get("enableSsid") is False or item.get("broadcast") is False:
        return False
    if item.get("ssidEnable") is True or item.get("enableSsid") is True or item.get("broadcast") is True:
        return True

    # If this is an override entry or contains wlanId, an 'enable: False' property means
    # the override is inactive, so the SSID inherits global site WLAN settings (enabled)
    if is_override or "wlanId" in item or "override" in item or "overrideEnable" in item:
        return True

    # Direct enabled / status checks for general WLAN settings
    if item.get("enabled") is False or item.get("status") in (0, False, "disabled", "down"):
        return False
    if item.get("enable") is False:
        return False

    return True


async def _async_fetch_omada(base_url: str, username: str, password: str):
    """Asynchronously query Omada controller using tplink_omada_client and return normalized data."""
    if OmadaClient is None:
        logger.error("tplink-omada-client library is not installed.")
        return [], [], [], []

    async with OmadaClient(url=base_url, username=username, password=password, verify_ssl=False) as client:
        await client.login()
        sites = await client.get_sites()
        
        all_sites_data = []
        all_devices_raw = []
        all_clients_raw = []
        normalized_nodes = []

        for site in sites:
            site_id = getattr(site, "id", None) or getattr(site, "key", None) or str(site)
            site_name = getattr(site, "name", str(site_id))
            all_sites_data.append({"id": site_id, "name": site_name})

            site_client = await client.get_site_client(site)

            # 1. Fetch connected clients
            site_clients = []
            try:
                async for c in site_client.get_connected_clients():
                    c_raw = getattr(c, "_data", {}) if hasattr(c, "_data") else {}
                    traffic_up = getattr(c, "traffic_up", 0) or c_raw.get("trafficUp", 0) or 0
                    traffic_down = getattr(c, "traffic_down", 0) or c_raw.get("trafficDown", 0) or 0
                    up_pkt = getattr(c, "up_packet", 0) or c_raw.get("upPacket", 0) or 0
                    down_pkt = getattr(c, "down_packet", 0) or c_raw.get("downPacket", 0) or 0
                    activity = getattr(c, "activity", 0) or c_raw.get("activity", 0) or 0
                    c_tx_r = c_raw.get("txRate") or c_raw.get("tx_rate") or 0
                    c_rx_r = c_raw.get("rxRate") or c_raw.get("rx_rate") or activity
                    c_tx_bps = int(c_tx_r * 8)
                    c_rx_bps = int(c_rx_r * 8)

                    c_dev_type = str(getattr(c, "connect_dev_type", "") or c_raw.get("connectDevType", "") or "").lower()
                    c_sw_mac = getattr(c, "switch_mac", None)
                    c_ap_mac = getattr(c, "ap_mac", None)
                    if not c_ap_mac and c_dev_type == "ap" and c_sw_mac:
                        c_ap_mac = c_sw_mac

                    is_wl = getattr(c, "wireless", False)

                    c_rid = getattr(c, "radio_id", None) if getattr(c, "radio_id", None) is not None else c_raw.get("radioId")
                    c_ch = getattr(c, "channel", None) if getattr(c, "channel", None) is not None else c_raw.get("channel")
                    c_band = ""
                    if is_wl:
                        if c_rid == 0:
                            c_band = "2.4GHz"
                        elif c_rid in (1, 2):
                            c_band = "5GHz"
                        elif c_rid == 3:
                            c_band = "6GHz"
                        elif c_ch is not None:
                            try:
                                ch_i = int(c_ch)
                                if 1 <= ch_i <= 14:
                                    c_band = "2.4GHz"
                                elif 36 <= ch_i <= 177:
                                    c_band = "5GHz"
                                elif ch_i >= 180:
                                    c_band = "6GHz"
                            except (ValueError, TypeError):
                                pass

                    c_dict = {
                        "mac": getattr(c, "mac", ""),
                        "name": getattr(c, "name", "") or getattr(c, "host_name", "") or getattr(c, "hostname", ""),
                        "ip": getattr(c, "ip", "") or getattr(c, "ip_address", "") or c_raw.get("ip", "") or c_raw.get("ipAddress", "") or c_raw.get("clientIp", ""),
                        "switch_mac": c_sw_mac,
                        "port": getattr(c, "port", None),
                        "ap_mac": c_ap_mac,
                        "connect_dev_type": c_dev_type,
                        "wireless": bool(is_wl),
                        "radio_id": c_rid,
                        "channel": c_ch,
                        "wifi_band": c_band,
                        "ssid": getattr(c, "ssid", "") or c_raw.get("ssid", "") or "",
                        "traffic_up": traffic_up,
                        "traffic_down": traffic_down,
                        "up_packet": up_pkt,
                        "down_packet": down_pkt,
                        "activity": activity,
                        "tx_rate": c_tx_r,
                        "rx_rate": c_rx_r,
                        "speed_tx_bps": c_tx_bps,
                        "speed_rx_bps": c_rx_bps
                    }
                    site_clients.append(c_dict)
                    all_clients_raw.append(c_dict)
            except Exception as e:
                logger.warning(f"Error fetching Omada clients for site {site_name}: {e}")

            # Index clients by switch_mac and ap_mac
            clients_by_switch = {}
            clients_by_ap = {}
            for c in site_clients:
                if c.get("switch_mac"):
                    sm = c["switch_mac"].replace(":", "").replace("-", "").upper()
                    clients_by_switch.setdefault(sm, []).append(c)
                if c.get("ap_mac"):
                    am = c["ap_mac"].replace(":", "").replace("-", "").upper()
                    clients_by_ap.setdefault(am, []).append(c)

            # 2. Fetch Switches
            try:
                switches = await site_client.get_switches()
                for sw in switches:
                    mac = getattr(sw, "mac", "")
                    clean_mac = mac.replace(":", "").replace("-", "").upper()
                    ip = getattr(sw, "ip_address", "") or getattr(sw, "ip", "")
                    name = getattr(sw, "name", "") or getattr(sw, "model", "Omada Switch")
                    model = getattr(sw, "model", "switch")
                    
                    status_cat = getattr(sw, "status_category", None)
                    is_online = True
                    if status_cat is not None:
                        is_online = (getattr(status_cat, "name", "").upper() in ["CONNECTED", "ONLINE"]) or (status_cat == 1)

                    ports = []
                    sw_ports = getattr(sw, "ports", [])
                    for p in sw_ports:
                        p_num = getattr(p, "port", 1)
                        p_name = getattr(p, "name", f"Port {p_num}")
                        p_status = getattr(p, "port_status", None)
                        
                        link_up = False
                        link_speed = ""
                        tx_bytes = 0
                        rx_bytes = 0
                        tx_pkts = 0
                        rx_pkts = 0
                        tx_rate = 0
                        rx_rate = 0
                        tx_pkt_rate = 0
                        rx_pkt_rate = 0
                        poe_status = "disabled"
                        poe_power = 0.0

                        if p_status:
                            ls = getattr(p_status, "link_status", None)
                            link_up = bool(getattr(ls, "name", "") == "LINK_UP" or ls == 1 or ls is True)
                            link_speed = _normalize_speed(getattr(p_status, "link_speed", ""))
                            tx_bytes = getattr(p_status, "bytes_tx", 0) or 0
                            rx_bytes = getattr(p_status, "bytes_rx", 0) or 0
                            
                            p_status_raw = getattr(p_status, "_data", {}) if hasattr(p_status, "_data") else {}
                            tx_pkts = p_status_raw.get("txPkt") or p_status_raw.get("tx_pkt") or getattr(p_status, "tx_pkt", 0) or 0
                            rx_pkts = p_status_raw.get("rxPkt") or p_status_raw.get("rx_pkt") or getattr(p_status, "rx_pkt", 0) or 0
                            tx_rate = p_status_raw.get("txRate") or p_status_raw.get("tx_rate") or getattr(p_status, "tx_rate", 0) or 0
                            rx_rate = p_status_raw.get("rxRate") or p_status_raw.get("rx_rate") or getattr(p_status, "rx_rate", 0) or 0
                            tx_pkt_rate = p_status_raw.get("txPktRate") or p_status_raw.get("tx_pkt_rate") or 0
                            rx_pkt_rate = p_status_raw.get("rxPktRate") or p_status_raw.get("rx_pkt_rate") or 0

                            if getattr(p_status, "poe_active", False):
                                poe_status = "enabled"
                                poe_power = float(getattr(p_status, "poe_power", 0.0) or 0.0)

                        # Fallback to port _data if needed
                        p_raw = getattr(p, "_data", {}) if hasattr(p, "_data") else {}
                        if not tx_bytes:
                            tx_bytes = p_raw.get("tx", 0) or 0
                        if not rx_bytes:
                            rx_bytes = p_raw.get("rx", 0) or 0
                        if not tx_pkts:
                            tx_pkts = p_raw.get("txPkt") or p_raw.get("tx_pkt") or 0
                        if not rx_pkts:
                            rx_pkts = p_raw.get("rxPkt") or p_raw.get("rx_pkt") or 0
                        if not tx_rate:
                            tx_rate = p_raw.get("txRate") or p_raw.get("tx_rate") or 0
                        if not rx_rate:
                            rx_rate = p_raw.get("rxRate") or p_raw.get("rx_rate") or 0
                        if not tx_pkt_rate:
                            tx_pkt_rate = p_raw.get("txPktRate") or p_raw.get("tx_pkt_rate") or 0
                        if not rx_pkt_rate:
                            rx_pkt_rate = p_raw.get("rxPktRate") or p_raw.get("rx_pkt_rate") or 0

                        ports.append({
                            "port": p_num,
                            "name": p_name,
                            "status": "up" if link_up else "down",
                            "speed": link_speed if link_up else "Down",
                            "speed_tx_bps": int(tx_rate * 8),
                            "speed_rx_bps": int(rx_rate * 8),
                            "tx_rate": tx_rate,
                            "rx_rate": rx_rate,
                            "tx_pkt_rate": tx_pkt_rate,
                            "rx_pkt_rate": rx_pkt_rate,
                            "tx_bytes": tx_bytes,
                            "rx_bytes": rx_bytes,
                            "tx_packets": tx_pkts,
                            "rx_packets": rx_pkts,
                            "poe_status": poe_status,
                            "poe_power": poe_power,
                            "note": p_name if p_name != f"Port {p_num}" else ""
                        })

                    # MAC table from connected clients on this switch
                    mac_table = []
                    for c in clients_by_switch.get(clean_mac, []):
                        if c.get("port") is not None:
                            mac_table.append({
                                "port": c["port"],
                                "mac": c["mac"],
                                "host": c.get("name", ""),
                                "ip": c.get("ip", ""),
                                "traffic_up": c.get("traffic_up", 0),
                                "traffic_down": c.get("traffic_down", 0),
                                "up_packet": c.get("up_packet", 0),
                                "down_packet": c.get("down_packet", 0),
                                "activity": c.get("activity", 0),
                                "speed_tx_bps": c.get("speed_tx_bps", 0),
                                "speed_rx_bps": c.get("speed_rx_bps", 0)
                            })

                    uplink_data = _safe_get_uplink(sw)

                    node_dict = {
                        "name": name,
                        "ip": ip,
                        "mac": mac,
                        "model": model,
                        "type": "switch",
                        "source": "omada",
                        "status": "online" if is_online else "offline",
                        "ports": ports,
                        "mac_table": mac_table,
                        "uplink": uplink_data,
                        "timestamp": time.time()
                    }
                    normalized_nodes.append(node_dict)
                    all_devices_raw.append(node_dict)
            except Exception as e:
                logger.warning(f"Error fetching Omada switches for site {site_name}: {e}")

            # 3. Fetch Gateways (Routers)
            try:
                gateways = await site_client.get_gateways()
                for gw in gateways:
                    mac = getattr(gw, "mac", "")
                    ip = getattr(gw, "ip_address", "") or getattr(gw, "ip", "")
                    name = getattr(gw, "name", "") or getattr(gw, "model", "Omada Gateway")
                    model = getattr(gw, "model", "gateway")

                    ports = []
                    gw_ports = getattr(gw, "port_status", []) or []
                    for idx, p in enumerate(gw_ports, 1):
                        p_raw = getattr(p, "_data", {}) if hasattr(p, "_data") else {}
                        if isinstance(p_raw, dict) and "port" in p_raw:
                            p_num = p_raw.get("port") or idx
                        else:
                            try:
                                p_num = getattr(p, "port_number", None) or getattr(p, "port", idx)
                            except Exception:
                                p_num = idx

                        try:
                            p_name = getattr(p, "name", "") or getattr(p, "display_name", f"Port {p_num}")
                        except Exception:
                            p_name = f"Port {p_num}"

                        ls = getattr(p, "link_status", None)
                        link_up = bool(getattr(ls, "name", "") == "LINK_UP" or ls == 1 or ls is True)
                        link_speed = _normalize_speed(getattr(p, "link_speed", ""))

                        tx_b = getattr(p, "bytes_tx", 0) or p_raw.get("tx", 0) or 0
                        rx_b = getattr(p, "bytes_rx", 0) or p_raw.get("rx", 0) or 0
                        tx_p = p_raw.get("txPkt") or p_raw.get("tx_pkt") or getattr(p, "tx_pkt", 0) or 0
                        rx_p = p_raw.get("rxPkt") or p_raw.get("rx_pkt") or getattr(p, "rx_pkt", 0) or 0
                        tx_r = p_raw.get("txRate") or p_raw.get("tx_rate") or getattr(p, "tx_rate", 0) or 0
                        rx_r = p_raw.get("rxRate") or p_raw.get("rx_rate") or getattr(p, "rx_rate", 0) or 0
                        tx_pr = p_raw.get("txPktRate") or p_raw.get("tx_pkt_rate") or 0
                        rx_pr = p_raw.get("rxPktRate") or p_raw.get("rx_pkt_rate") or 0

                        ports.append({
                            "port": p_num,
                            "name": p_name,
                            "status": "up" if link_up else "down",
                            "speed": link_speed if link_up else "Down",
                            "speed_tx_bps": int(tx_r * 8),
                            "speed_rx_bps": int(rx_r * 8),
                            "tx_rate": tx_r,
                            "rx_rate": rx_r,
                            "tx_pkt_rate": tx_pr,
                            "rx_pkt_rate": rx_pr,
                            "tx_bytes": tx_b,
                            "rx_bytes": rx_b,
                            "tx_packets": tx_p,
                            "rx_packets": rx_p,
                            "poe_status": "disabled",
                            "poe_power": 0.0,
                            "note": p_name
                        })

                    node_dict = {
                        "name": name,
                        "ip": ip,
                        "mac": mac,
                        "model": model,
                        "type": "router",
                        "source": "omada",
                        "status": "online",
                        "ports": ports,
                        "mac_table": [],
                        "timestamp": time.time()
                    }
                    normalized_nodes.append(node_dict)
                    all_devices_raw.append(node_dict)
            except Exception as e:
                logger.warning(f"Error fetching Omada gateways for site {site_name}: {e}")

            # 4. Fetch Access Points (EAPs)
            try:
                aps = []
                try:
                    aps = await site_client.get_access_points()
                except Exception as e_aps:
                    logger.warning(f"get_access_points encountered an error, falling back to get_devices: {e_aps}")
                    all_devs = await site_client.get_devices()
                    for d in all_devs:
                        if getattr(d, "type", "") == "ap":
                            try:
                                ap_detail = await site_client.get_access_point(d)
                                aps.append(ap_detail)
                            except Exception:
                                aps.append(d)

                for ap in aps:
                    try:
                        mac = getattr(ap, "mac", "")
                        clean_mac = mac.replace(":", "").replace("-", "").upper()
                        ip = getattr(ap, "ip_address", "") or getattr(ap, "ip", "")
                        name = getattr(ap, "name", "") or getattr(ap, "model", "Omada AP")
                        model = getattr(ap, "model", "ap")
                        ap_clients = list(clients_by_ap.get(clean_mac, []))
                        for c in clients_by_switch.get(clean_mac, []):
                            if c not in ap_clients and (c.get("connect_dev_type") == "ap" or c.get("ap_mac") == clean_mac):
                                ap_clients.append(c)

                        # Wireless & wired client list attached to this AP
                        mac_table = []
                        clients_by_port = {}
                        for c in ap_clients:
                            p_label = _get_client_ap_port(c)
                            if p_label:
                                clients_by_port.setdefault(p_label, []).append(c)
                                digits = "".join(filter(str.isdigit, p_label))
                                if digits and not c.get("wireless", False):
                                    clients_by_port.setdefault(digits, []).append(c)
                                    clients_by_port.setdefault(f"ETH{digits}", []).append(c)
                            mac_table.append({
                                "port": p_label or "WLAN",
                                "mac": c["mac"],
                                "host": c.get("name", ""),
                                "ip": c.get("ip", ""),
                                "wireless": c.get("wireless", False),
                                "wifi_band": c.get("wifi_band", ""),
                                "traffic_up": c.get("traffic_up", 0),
                                "traffic_down": c.get("traffic_down", 0),
                                "up_packet": c.get("up_packet", 0),
                                "down_packet": c.get("down_packet", 0),
                                "activity": c.get("activity", 0),
                                "speed_tx_bps": c.get("speed_tx_bps", 0),
                                "speed_rx_bps": c.get("speed_rx_bps", 0)
                            })

                        uplink_data = _safe_get_uplink(ap)
                        ap_raw = getattr(ap, "_data", {}) if hasattr(ap, "_data") else {}

                        ports = []

                        # 1. Classify port stats into uplink vs downlink LAN ports
                        port_stats_list = ap_raw.get("portStats") or ap_raw.get("port_stats") or ap_raw.get("portStatus") or ap_raw.get("port_status") or []
                        lan_settings = getattr(ap, "lan_port_settings", []) or ap_raw.get("lanPortSettings") or []
                        dev_misc = ap_raw.get("deviceMisc") or {}
                        support_eth = int(ap_raw.get("supportEthNum") or dev_misc.get("portNum") or ap_raw.get("portNum") or 0)
                        model_str = str(model).lower()

                        if "wall" in model_str:
                            num_downlinks = 3
                            total_eth = 4
                        elif support_eth > 1:
                            num_downlinks = support_eth - 1
                            total_eth = support_eth
                        elif lan_settings:
                            num_downlinks = len(lan_settings)
                            total_eth = num_downlinks + 1
                        else:
                            num_downlinks = 0
                            total_eth = 1

                        uplink_pstat = None
                        downlink_pstats_by_eth = {}

                        raw_stats = []
                        for ps in port_stats_list:
                            ps_raw = ps if isinstance(ps, dict) else getattr(ps, "_data", {})
                            if ps_raw:
                                raw_stats.append(ps_raw)

                        # Step 1: Explicit uplink flag/name
                        for ps_raw in raw_stats:
                            p_name = str(ps_raw.get("name") or ps_raw.get("lanPort") or "").strip().lower()
                            p_num = ps_raw.get("port")
                            if (ps_raw.get("isUplink") or ps_raw.get("type") in ("uplink", "wan") or
                                "uplink" in p_name or "eth0" in p_name or "wan" in p_name or "poe in" in p_name or
                                p_num == 0):
                                uplink_pstat = ps_raw
                                break

                        # Step 2: Disambiguate if uplink was not explicitly labeled
                        if not uplink_pstat and len(raw_stats) > 0:
                            if len(raw_stats) == 1:
                                # A single port stat on an AP is always the Uplink (ETH0 / PoE In)
                                uplink_pstat = raw_stats[0]
                            elif len(raw_stats) > num_downlinks:
                                # Has more stats than downlinks -> the extra one is the Uplink
                                uplink_pstat = raw_stats[0]
                            elif len(raw_stats) == num_downlinks:
                                # Exactly matches downlink count: none is the uplink, all are downlinks
                                uplink_pstat = None

                        # Step 3: All remaining stats are downlink candidates
                        downlink_candidates = [ps for ps in raw_stats if ps is not uplink_pstat]

                        # Step 4: Map downlink candidates strictly to ETH1, ETH2, ETH3...
                        for idx, ps_raw in enumerate(downlink_candidates, 1):
                            if ps_raw is uplink_pstat:
                                continue
                            p_name = str(ps_raw.get("name") or ps_raw.get("lanPort") or "").strip().upper()
                            digits = "".join(filter(str.isdigit, p_name))
                            p_num = ps_raw.get("port")

                            eth_idx = None
                            if (p_name.startswith("ETH") or p_name.startswith("LAN")) and digits:
                                try:
                                    eth_idx = int(digits)
                                except ValueError:
                                    pass

                            if eth_idx is None and p_num is not None:
                                try:
                                    pn = int(p_num)
                                    if uplink_pstat and uplink_pstat.get("port") is not None:
                                        up_pn = int(uplink_pstat.get("port"))
                                        if pn == up_pn:
                                            continue
                                        if pn > up_pn and (pn - up_pn) <= num_downlinks:
                                            eth_idx = pn - up_pn
                                    if eth_idx is None and 1 <= pn <= num_downlinks:
                                        eth_idx = pn
                                except (ValueError, TypeError):
                                    pass

                            if eth_idx is None and len(downlink_candidates) == num_downlinks:
                                eth_idx = idx

                            if eth_idx is not None and 1 <= eth_idx <= num_downlinks:
                                downlink_pstats_by_eth[eth_idx] = ps_raw

                        # 2. Primary Wired Uplink Port (ETH0 / PoE In to Switch)
                        up_spd = "1000M"
                        if uplink_pstat and uplink_pstat.get("linkSpeed"):
                            up_spd = _normalize_speed(uplink_pstat.get("linkSpeed"))
                        elif ap_raw.get("uplinkSpeed"):
                            up_spd = _normalize_speed(ap_raw.get("uplinkSpeed"))
                        elif "2.5" in str(model) or "660" in str(model) or "670" in str(model):
                            up_spd = "2.5G"
                        elif "10g" in str(model).lower() or "690" in str(model):
                            up_spd = "10G"

                        up_tx_b = (uplink_pstat.get("tx") if uplink_pstat else None) or ap_raw.get("tx") or ap_raw.get("bytes_tx") or ap_raw.get("trafficDown") or sum(c.get("traffic_down", 0) for c in ap_clients)
                        up_rx_b = (uplink_pstat.get("rx") if uplink_pstat else None) or ap_raw.get("rx") or ap_raw.get("bytes_rx") or ap_raw.get("trafficUp") or sum(c.get("traffic_up", 0) for c in ap_clients)
                        up_tx_p = (uplink_pstat.get("txPkt") if uplink_pstat else None) or ap_raw.get("txPkt") or ap_raw.get("tx_pkt") or ap_raw.get("downPacket") or sum(c.get("down_packet", 0) for c in ap_clients)
                        up_rx_p = (uplink_pstat.get("rxPkt") if uplink_pstat else None) or ap_raw.get("rxPkt") or ap_raw.get("rx_pkt") or ap_raw.get("upPacket") or sum(c.get("up_packet", 0) for c in ap_clients)

                        up_tx_r = (uplink_pstat.get("txRate") if uplink_pstat else None) or ap_raw.get("txRate") or ap_raw.get("tx_rate") or ap_raw.get("activity") or sum(c.get("activity", 0) for c in ap_clients)
                        up_rx_r = (uplink_pstat.get("rxRate") if uplink_pstat else None) or ap_raw.get("rxRate") or ap_raw.get("rx_rate") or sum(c.get("tx_rate", 0) for c in ap_clients)
                        up_tx_bps = int(up_tx_r * 8)
                        up_rx_bps = int(up_rx_r * 8)

                        up_note = "ETH0 / Uplink (PoE In)"
                        if uplink_data and uplink_data.get("name"):
                            up_note += f" to {uplink_data['name']}"

                        ports.append({
                            "port": "Uplink",
                            "name": "ETH0 / Uplink",
                            "status": "up",
                            "speed": up_spd,
                            "speed_tx_bps": up_tx_bps,
                            "speed_rx_bps": up_rx_bps,
                            "tx_rate": up_tx_r,
                            "rx_rate": up_rx_r,
                            "tx_bytes": up_tx_b,
                            "rx_bytes": up_rx_b,
                            "tx_packets": up_tx_p,
                            "rx_packets": up_rx_p,
                            "poe_status": "disabled",
                            "poe_power": 0.0,
                            "note": up_note,
                            "is_uplink": True
                        })

                        # 3. Downlink Physical Ethernet Ports (ETH1, ETH2, ETH3...)
                        for p_idx in range(1, num_downlinks + 1):
                            ls_item = lan_settings[p_idx - 1] if p_idx <= len(lan_settings) else None
                            ls_data = ls_item if isinstance(ls_item, dict) else (getattr(ls_item, "_data", {}) if ls_item else {})

                            ps_raw = downlink_pstats_by_eth.get(p_idx, {})

                            p_name = ls_data.get("lanPort") or ps_raw.get("name") or f"ETH{p_idx} / LAN"
                            if p_idx == 1 and "wall" in model_str and "poe" not in p_name.lower():
                                p_name += " (PoE Out)"

                            p_clients = [c for c in clients_by_port.get(f"ETH{p_idx}", []) if not c.get("wireless", False)]

                            ls = ps_raw.get("linkStatus") if ps_raw else None
                            if ls is None and ls_data:
                                ls = ls_data.get("linkStatus")

                            is_up = bool(ls == 1 or getattr(ls, "name", "") == "LINK_UP" or len(p_clients) > 0)
                            spd = _normalize_speed(ps_raw.get("linkSpeed", "") or ls_data.get("linkSpeed", "")) if is_up else "Down"
                            if p_clients and spd == "Down":
                                spd = "1000M"

                            poe_active = bool(ps_raw.get("poe") or ls_data.get("poeOutEnable") or getattr(ls_item, "poe_enable", False))
                            poe_power = float(ps_raw.get("poePower", 0.0) or 0.0)

                            if is_up:
                                tx_b = ps_raw.get("tx", 0) or sum(c.get("traffic_down", 0) for c in p_clients)
                                rx_b = ps_raw.get("rx", 0) or sum(c.get("traffic_up", 0) for c in p_clients)
                                tx_p = ps_raw.get("txPkt", 0) or ps_raw.get("tx_pkt", 0) or sum(c.get("down_packet", 0) for c in p_clients)
                                rx_p = ps_raw.get("rxPkt", 0) or ps_raw.get("rx_pkt", 0) or sum(c.get("up_packet", 0) for c in p_clients)
                                tx_r = ps_raw.get("txRate", 0) or ps_raw.get("tx_rate", 0) or sum(c.get("activity", 0) for c in p_clients)
                                rx_r = ps_raw.get("rxRate", 0) or ps_raw.get("rx_rate", 0) or sum(c.get("tx_rate", 0) for c in p_clients)
                            else:
                                tx_b = 0
                                rx_b = 0
                                tx_p = 0
                                rx_p = 0
                                tx_r = 0
                                rx_r = 0

                            note_parts = []
                            if p_idx == 1 and "wall" in model_str:
                                note_parts.append("PoE Out")
                            if ls_data.get("localVlanEnable"):
                                note_parts.append(f"VLAN {ls_data.get('localVlanId')}")
                            if p_clients:
                                note_parts.append(f"{len(p_clients)} Client{'s' if len(p_clients) != 1 else ''}")

                            ports.append({
                                "port": f"ETH{p_idx}",
                                "name": p_name,
                                "status": "up" if is_up else "down",
                                "speed": spd,
                                "speed_tx_bps": int(tx_r * 8),
                                "speed_rx_bps": int(rx_r * 8),
                                "tx_rate": tx_r,
                                "rx_rate": rx_r,
                                "tx_bytes": tx_b,
                                "rx_bytes": rx_b,
                                "tx_packets": tx_p,
                                "rx_packets": rx_p,
                                "poe_status": "enabled" if poe_active else "disabled",
                                "poe_power": poe_power,
                                "note": " · ".join(note_parts),
                                "is_uplink": False
                            })

                        # 4. Wireless Ports: Consider every SSID as a distinct port
                        # Radio statuses, enabled states, and capabilities
                        w2g_setting = ap_raw.get("w2gSetting") or {}
                        w2g_status = ap_raw.get("w2gStatus") or ap_raw.get("w2gStat") or {}
                        w2g_enabled = (w2g_setting.get("radioEnable") is not False)
                        w2g_rate = w2g_status.get("maxTxRate")
                        spd_2g = f"{w2g_rate}M" if w2g_rate else ("574M" if "ax" in str(model).lower() or "6" in str(model) else "300M")

                        w5g_setting = ap_raw.get("w5gSetting") or {}
                        w5g_status = ap_raw.get("w5gStatus") or ap_raw.get("w5gStat") or {}
                        w5g_enabled = (w5g_setting.get("radioEnable") is not False)
                        w5g_rate = w5g_status.get("maxTxRate")
                        spd_5g = f"{w5g_rate}M" if w5g_rate else ("2402M" if "ax" in str(model).lower() or "6" in str(model) else "867M")

                        w6g_setting = ap_raw.get("w6gSetting") or {}
                        w6g_status = ap_raw.get("w6gStatus") or ap_raw.get("w6gStat") or {}
                        w6g_enabled = (w6g_setting.get("radioEnable") is not False) if (w6g_setting or getattr(ap, "supports_6g", False)) else False
                        w6g_rate = w6g_status.get("maxTxRate") or "4804M"

                        ap_radios_enabled = w2g_enabled or w5g_enabled or w6g_enabled

                        # Discover all SSIDs on this AP
                        ssids_dict = {}  # ssid_name -> {"enabled": bool, "clients": []}

                        # 1. Per-device SSID overrides
                        for s_item in (ap_raw.get("ssidOverrides") or []):
                            if isinstance(s_item, dict):
                                s_name = (s_item.get("ssid") or s_item.get("name") or "").strip()
                                if s_name:
                                    is_en = _is_ssid_enabled(s_item, is_override=True)
                                    ssids_dict[s_name] = {"enabled": is_en, "clients": []}

                        # 2. WLAN overrides
                        for w_item in (ap_raw.get("wlanOverrides") or []):
                            if isinstance(w_item, dict):
                                s_name = (w_item.get("ssid") or w_item.get("name") or "").strip()
                                if s_name:
                                    is_en = _is_ssid_enabled(w_item, is_override=True)
                                    if s_name not in ssids_dict:
                                        ssids_dict[s_name] = {"enabled": is_en, "clients": []}
                                    elif not is_en:
                                        ssids_dict[s_name]["enabled"] = False

                        # 3. Direct SSID list
                        for s_item in (ap_raw.get("ssidList") or ap_raw.get("ssids") or []):
                            if isinstance(s_item, dict):
                                s_name = (s_item.get("ssid") or s_item.get("name") or "").strip()
                                if s_name:
                                    is_en = _is_ssid_enabled(s_item, is_override=False)
                                    if s_name not in ssids_dict:
                                        ssids_dict[s_name] = {"enabled": is_en, "clients": []}
                                    elif not is_en:
                                        ssids_dict[s_name]["enabled"] = False
                            elif isinstance(s_item, str) and s_item.strip() and s_item.strip() not in ssids_dict:
                                ssids_dict[s_item.strip()] = {"enabled": True, "clients": []}

                        # 4. Global WLAN settings if present
                        for w_item in (ap_raw.get("wlanSettings") or []):
                            if isinstance(w_item, dict):
                                s_name = (w_item.get("ssid") or w_item.get("name") or "").strip()
                                if s_name:
                                    is_en = _is_ssid_enabled(w_item, is_override=False)
                                    if s_name not in ssids_dict:
                                        ssids_dict[s_name] = {"enabled": is_en, "clients": []}
                                    elif not is_en:
                                        ssids_dict[s_name]["enabled"] = False

                        # Group wireless clients by SSID
                        unmapped_wireless_clients = []
                        for c in ap_clients:
                            if c.get("wireless", False):
                                s_name = (c.get("ssid") or "").strip()
                                if s_name:
                                    if s_name not in ssids_dict:
                                        ssids_dict[s_name] = {"enabled": True, "clients": []}
                                    ssids_dict[s_name]["clients"].append(c)
                                else:
                                    unmapped_wireless_clients.append(c)

                        if ssids_dict:
                            # Create an individual port for every SSID, ordered and grouped by name
                            for ssid_name, s_info in ssids_dict.items():
                                c_list = s_info["clients"]
                                # Up/down status strictly reflects whether the SSID is enabled, NOT whether clients are connected
                                is_up = bool(s_info.get("enabled", True)) and ap_radios_enabled

                                bands_seen = set()
                                channels_seen = set()
                                max_client_rate = 0

                                for c in c_list:
                                    rid = c.get("radio_id")
                                    if rid == 0:
                                        bands_seen.add("2.4G")
                                    elif rid in (1, 2):
                                        bands_seen.add("5G")
                                    elif rid == 3:
                                        bands_seen.add("6G")

                                    ch = c.get("channel")
                                    if ch is not None:
                                        try:
                                            ch_num = int(ch)
                                            channels_seen.add(str(ch_num))
                                            if 1 <= ch_num <= 14:
                                                bands_seen.add("2.4G")
                                            elif 36 <= ch_num <= 177:
                                                bands_seen.add("5G")
                                            elif ch_num >= 180:
                                                bands_seen.add("6G")
                                        except (ValueError, TypeError):
                                            pass

                                    tx_k = c.get("tx_rate") or 0
                                    rx_k = c.get("rx_rate") or 0
                                    max_k = max(tx_k, rx_k)
                                    if max_k > max_client_rate:
                                        max_client_rate = max_k

                                if max_client_rate > 1000:
                                    client_spd_mbps = int(round(max_client_rate / 1000.0))
                                else:
                                    client_spd_mbps = max_client_rate

                                if not is_up:
                                    spd_str = "Down"
                                elif client_spd_mbps > 0:
                                    spd_str = f"{client_spd_mbps}M"
                                elif "6G" in bands_seen or getattr(ap, "supports_6g", False):
                                    spd_str = f"{w6g_rate}M" if w6g_rate else "4804M"
                                elif "5G" in bands_seen or getattr(ap, "supports_5g", True):
                                    spd_str = spd_5g
                                elif w2g_rate:
                                    spd_str = spd_2g
                                else:
                                    spd_str = "1000M"

                                tx_b = sum(c.get("traffic_down", 0) for c in c_list)
                                rx_b = sum(c.get("traffic_up", 0) for c in c_list)
                                tx_p = sum(c.get("down_packet", 0) for c in c_list)
                                rx_p = sum(c.get("up_packet", 0) for c in c_list)
                                tx_r = sum(c.get("activity", 0) for c in c_list)
                                rx_r = sum(c.get("tx_rate", 0) for c in c_list)
                                tx_bps = sum(c.get("speed_tx_bps", 0) for c in c_list) or int(tx_r * 8)
                                rx_bps = sum(c.get("speed_rx_bps", 0) for c in c_list) or int(rx_r * 8)

                                note_parts = []
                                if bands_seen:
                                    sorted_bands = sorted(list(bands_seen), key=lambda b: (0 if "2.4" in b else 1 if "5" in b else 2))
                                    note_parts.append("/".join(sorted_bands))
                                else:
                                    avail_bands = []
                                    if w2g_enabled: avail_bands.append("2.4G")
                                    if w5g_enabled: avail_bands.append("5G")
                                    if w6g_enabled: avail_bands.append("6G")
                                    if avail_bands:
                                        note_parts.append("/".join(avail_bands))

                                if channels_seen:
                                    ch_list = sorted(list(channels_seen), key=lambda x: int(x) if x.isdigit() else 0)
                                    note_parts.append(f"CH {','.join(ch_list[:3])}")

                                if not is_up:
                                    note_parts.append("Disabled")
                                else:
                                    note_parts.append(f"{len(c_list)} Client{'s' if len(c_list) != 1 else ''}")

                                ports.append({
                                    "port": f"SSID: {ssid_name}",
                                    "name": f"SSID: {ssid_name}",
                                    "status": "up" if is_up else "down",
                                    "speed": spd_str,
                                    "speed_tx_bps": tx_bps,
                                    "speed_rx_bps": rx_bps,
                                    "tx_rate": tx_r,
                                    "rx_rate": rx_r,
                                    "tx_bytes": tx_b,
                                    "rx_bytes": rx_b,
                                    "tx_packets": tx_p,
                                    "rx_packets": rx_p,
                                    "poe_status": "disabled",
                                    "poe_power": 0.0,
                                    "note": " · ".join(note_parts),
                                    "is_uplink": False
                                })

                            if unmapped_wireless_clients:
                                tx_b_w = sum(c.get("traffic_down", 0) for c in unmapped_wireless_clients)
                                rx_b_w = sum(c.get("traffic_up", 0) for c in unmapped_wireless_clients)
                                tx_p_w = sum(c.get("down_packet", 0) for c in unmapped_wireless_clients)
                                rx_p_w = sum(c.get("up_packet", 0) for c in unmapped_wireless_clients)
                                tx_r_w = sum(c.get("activity", 0) for c in unmapped_wireless_clients)
                                rx_r_w = sum(c.get("tx_rate", 0) for c in unmapped_wireless_clients)
                                ports.append({
                                    "port": "WLAN",
                                    "name": "WLAN (Wireless)",
                                    "status": "up" if ap_radios_enabled else "down",
                                    "speed": "1000M" if ap_radios_enabled else "Down",
                                    "speed_tx_bps": sum(c.get("speed_tx_bps", 0) for c in unmapped_wireless_clients) or int(tx_r_w * 8),
                                    "speed_rx_bps": sum(c.get("speed_rx_bps", 0) for c in unmapped_wireless_clients) or int(rx_r_w * 8),
                                    "tx_rate": tx_r_w,
                                    "rx_rate": rx_r_w,
                                    "tx_bytes": tx_b_w,
                                    "rx_bytes": rx_b_w,
                                    "tx_packets": tx_p_w,
                                    "rx_packets": rx_p_w,
                                    "poe_status": "disabled",
                                    "poe_power": 0.0,
                                    "note": f"{len(unmapped_wireless_clients)} Clients" if ap_radios_enabled else "Disabled",
                                    "is_uplink": False
                                })
                        else:
                            # Fallback to radio band ports if no SSIDs are known yet
                            # 2.4 GHz Radio
                            w2g_enabled = (w2g_setting.get("radioEnable") is not False)
                            c_2g = clients_by_port.get("2.4G", [])
                            w2g_ch = w2g_status.get("channel") or w2g_status.get("actualChannel") or w2g_setting.get("channel")
                            w2g_bw = w2g_status.get("bandWidth") or w2g_setting.get("channelWidth")

                            tx_b_2g = w2g_status.get("tx") or sum(c.get("traffic_down", 0) for c in c_2g)
                            rx_b_2g = w2g_status.get("rx") or sum(c.get("traffic_up", 0) for c in c_2g)
                            tx_p_2g = w2g_status.get("txPkt") or sum(c.get("down_packet", 0) for c in c_2g)
                            rx_p_2g = w2g_status.get("rxPkt") or sum(c.get("up_packet", 0) for c in c_2g)
                            tx_r_2g = w2g_status.get("txRate") or sum(c.get("activity", 0) for c in c_2g)
                            rx_r_2g = w2g_status.get("rxRate") or sum(c.get("tx_rate", 0) for c in c_2g)

                            note_2g_parts = []
                            if w2g_ch: note_2g_parts.append(f"CH {w2g_ch}")
                            if w2g_bw:
                                bw_str = str(w2g_bw).replace("BW", "")
                                note_2g_parts.append(bw_str if "mhz" in bw_str.lower() else f"{bw_str}MHz")
                            if not w2g_enabled:
                                note_2g_parts.append("Disabled")
                            else:
                                note_2g_parts.append(f"{len(c_2g)} Clients")

                            ports.append({
                                "port": "2.4G",
                                "name": "WLAN 2.4 GHz",
                                "status": "up" if w2g_enabled else "down",
                                "speed": spd_2g if w2g_enabled else "Down",
                                "speed_tx_bps": int(tx_r_2g * 8),
                                "speed_rx_bps": int(rx_r_2g * 8),
                                "tx_rate": tx_r_2g,
                                "rx_rate": rx_r_2g,
                                "tx_bytes": tx_b_2g,
                                "rx_bytes": rx_b_2g,
                                "tx_packets": tx_p_2g,
                                "rx_packets": rx_p_2g,
                                "poe_status": "disabled",
                                "poe_power": 0.0,
                                "note": " · ".join(note_2g_parts),
                                "is_uplink": False
                            })

                            # 5 GHz Radio
                            w5g_enabled = (w5g_setting.get("radioEnable") is not False)
                            c_5g = clients_by_port.get("5G", [])
                            w5g_ch = w5g_status.get("channel") or w5g_status.get("actualChannel") or w5g_setting.get("channel")
                            w5g_bw = w5g_status.get("bandWidth") or w5g_setting.get("channelWidth")

                            tx_b_5g = w5g_status.get("tx") or sum(c.get("traffic_down", 0) for c in c_5g)
                            rx_b_5g = w5g_status.get("rx") or sum(c.get("traffic_up", 0) for c in c_5g)
                            tx_p_5g = w5g_status.get("txPkt") or sum(c.get("down_packet", 0) for c in c_5g)
                            rx_p_5g = w5g_status.get("rxPkt") or sum(c.get("up_packet", 0) for c in c_5g)
                            tx_r_5g = w5g_status.get("txRate") or sum(c.get("activity", 0) for c in c_5g)
                            rx_r_5g = w5g_status.get("rxRate") or sum(c.get("tx_rate", 0) for c in c_5g)

                            note_5g_parts = []
                            if w5g_ch: note_5g_parts.append(f"CH {w5g_ch}")
                            if w5g_bw:
                                bw_str = str(w5g_bw).replace("BW", "")
                                note_5g_parts.append(bw_str if "mhz" in bw_str.lower() else f"{bw_str}MHz")
                            if not w5g_enabled:
                                note_5g_parts.append("Disabled")
                            else:
                                note_5g_parts.append(f"{len(c_5g)} Clients")

                            ports.append({
                                "port": "5G",
                                "name": "WLAN 5 GHz",
                                "status": "up" if w5g_enabled else "down",
                                "speed": spd_5g if w5g_enabled else "Down",
                                "speed_tx_bps": int(tx_r_5g * 8),
                                "speed_rx_bps": int(rx_r_5g * 8),
                                "tx_rate": tx_r_5g,
                                "rx_rate": rx_r_5g,
                                "tx_bytes": tx_b_5g,
                                "rx_bytes": rx_b_5g,
                                "tx_packets": tx_p_5g,
                                "rx_packets": rx_p_5g,
                                "poe_status": "disabled",
                                "poe_power": 0.0,
                                "note": " · ".join(note_5g_parts),
                                "is_uplink": False
                            })

                            # 6 GHz Radio
                            c_6g = clients_by_port.get("6G", [])
                            if w6g_setting or w6g_status or c_6g or getattr(ap, "supports_6g", False) or "6e" in str(model).lower() or "7" in str(model):
                                w6g_enabled = (w6g_setting.get("radioEnable") is not False)
                                w6g_ch = w6g_status.get("channel") or w6g_status.get("actualChannel") or w6g_setting.get("channel")
                                w6g_bw = w6g_status.get("bandWidth") or w6g_setting.get("channelWidth")

                                tx_b_6g = w6g_status.get("tx") or sum(c.get("traffic_down", 0) for c in c_6g)
                                rx_b_6g = w6g_status.get("rx") or sum(c.get("traffic_up", 0) for c in c_6g)
                                tx_p_6g = w6g_status.get("txPkt") or sum(c.get("down_packet", 0) for c in c_6g)
                                rx_p_6g = w6g_status.get("rxPkt") or sum(c.get("up_packet", 0) for c in c_6g)
                                tx_r_6g = w6g_status.get("txRate") or sum(c.get("activity", 0) for c in c_6g)
                                rx_r_6g = w6g_status.get("rxRate") or sum(c.get("tx_rate", 0) for c in c_6g)

                                note_6g_parts = []
                                if w6g_ch: note_6g_parts.append(f"CH {w6g_ch}")
                                if w6g_bw:
                                    bw_str = str(w6g_bw).replace("BW", "")
                                    note_6g_parts.append(bw_str if "mhz" in bw_str.lower() else f"{bw_str}MHz")
                                if not w6g_enabled:
                                    note_6g_parts.append("Disabled")
                                else:
                                    note_6g_parts.append(f"{len(c_6g)} Clients")

                                ports.append({
                                    "port": "6G",
                                    "name": "WLAN 6 GHz",
                                    "status": "up" if w6g_enabled else "down",
                                    "speed": f"{w6g_rate}M" if isinstance(w6g_rate, int) else str(w6g_rate),
                                    "speed_tx_bps": int(tx_r_6g * 8),
                                    "speed_rx_bps": int(rx_r_6g * 8),
                                    "tx_rate": tx_r_6g,
                                    "rx_rate": rx_r_6g,
                                    "tx_bytes": tx_b_6g,
                                    "rx_bytes": rx_b_6g,
                                    "tx_packets": tx_p_6g,
                                    "rx_packets": rx_p_6g,
                                    "poe_status": "disabled",
                                    "poe_power": 0.0,
                                    "note": " · ".join(note_6g_parts),
                                    "is_uplink": False
                                })

                        node_dict = {
                            "name": name,
                            "ip": ip,
                            "mac": mac,
                            "model": model,
                            "type": "repeater",
                            "source": "omada",
                            "status": "online",
                            "ports": ports,
                            "mac_table": mac_table,
                            "uplink": uplink_data,
                            "timestamp": time.time()
                        }
                        normalized_nodes.append(node_dict)
                        all_devices_raw.append(node_dict)
                    except Exception as e_ap:
                        logger.warning(f"Error processing single Omada AP {getattr(ap, 'name', ap)}: {e_ap}")
            except Exception as e:
                logger.warning(f"Error fetching Omada access points for site {site_name}: {e}")

        return all_sites_data, all_devices_raw, all_clients_raw, normalized_nodes


def _fetch_omada_data(base_url: str, username: str, password: str):
    """Fetch data from Omada controller and store in global caches."""
    global _omada_sites, _omada_devices, _omada_clients, _omada_nodes
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            sites, devices, clients, nodes = loop.run_until_complete(
                _async_fetch_omada(base_url, username, password)
            )
        finally:
            loop.close()

        with _omada_data_lock:
            _omada_sites = sites
            _omada_devices = devices
            _omada_clients = clients
            _omada_nodes = nodes
        logger.info("Successfully fetched %d sites, %d devices, %d clients, %d nodes from Omada",
                    len(sites), len(devices), len(clients), len(nodes))
    except Exception as e:
        logger.error(f"Error fetching Omada data: {e}")


def _omada_loop(base_url: str, username: str, password: str, poll_interval: int = 300):
    """Background loop that authenticates and periodically refreshes Omada data."""
    global _stop_omada_thread
    if OmadaClient is None:
        logger.error("tplink-omada-client library is not installed. Omada integration disabled.")
        return
    logger.info("Omada client initialized, starting polling loop with interval %s seconds", poll_interval)
    while not _stop_omada_thread:
        _fetch_omada_data(base_url, username, password)
        # Sleep in short increments so stop flag is responsive
        for _ in range(max(1, poll_interval)):
            if _stop_omada_thread:
                break
            time.sleep(1)
    logger.info("Omada polling thread stopped")


def start_omada_thread(config: Dict[str, Any]):
    """Start the background thread using the provided config dictionary."""
    global _omada_thread, _stop_omada_thread
    omada_cfg = config.get('omada', {})
    if not omada_cfg:
        logger.warning("Omada configuration not found, skipping Omada integration.")
        return
    base_url = omada_cfg.get('base_url')
    username = omada_cfg.get('username')
    password = omada_cfg.get('password')
    if not base_url or not username or not password:
        logger.warning("Omada base_url, username or password missing. Skipping Omada integration.")
        return
    poll_interval = int(omada_cfg.get('poll_interval', 300))
    _stop_omada_thread = False
    _omada_thread = threading.Thread(
        target=_omada_loop,
        args=(base_url, username, password, poll_interval),
        daemon=True,
        name='omada-poller'
    )
    _omada_thread.start()
    logger.info("Started Omada background thread")


def stop_omada_thread():
    global _stop_omada_thread
    _stop_omada_thread = True


def restart_omada_thread(new_config: Dict[str, Any]):
    """Stop existing thread (if any) and start a new one with updated config."""
    stop_omada_thread()
    time.sleep(0.1)
    start_omada_thread(new_config)


def get_sites() -> List[Dict[str, Any]]:
    with _omada_data_lock:
        return list(_omada_sites)


def get_devices() -> List[Dict[str, Any]]:
    with _omada_data_lock:
        return list(_omada_devices)


def get_clients() -> List[Dict[str, Any]]:
    with _omada_data_lock:
        return list(_omada_clients)


def get_omada_nodes() -> List[Dict[str, Any]]:
    """Return normalized dashboard node representations for Omada switches, routers, and APs."""
    with _omada_data_lock:
        return [dict(n) for n in _omada_nodes]

