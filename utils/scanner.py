# Python Port Scanner
import socket
import logging
import threading

from ip_utils import ip_range_to_list

logger  = logging.getLogger('Scanner')
logging.basicConfig(level=logging.INFO)

def thread_scanner(IpScanner, thread_id, ips):
    for ip in ips:
        IpScanner.scan_ip(ip)
    logger.info(f"Thread {thread_id} finished scanning.")

class Scanner(object):
    """Scanner Class.
    This class is used to create a Scanner (wow!)
    """

    SOCKET_FAMILY = socket.AF_INET
    SOCKET_KIND = socket.SOCK_STREAM

    def __init__(self, ipaddr, port, timeout=1, threads=4):
        self.ipaddr = ip_range_to_list(ipaddr)
        self.timeout = timeout
        self.threads = threads
        self.port = port
        
        self.potential_dbs = list()

    def scan_ip(self, ip):
        scanning_socket = socket.socket(Scanner.SOCKET_FAMILY, Scanner.SOCKET_KIND)
        scanning_socket.settimeout(self.timeout)
        logging.info(f"Scanning {ip}:{self.port}")

        scanning_result = scanning_socket.connect_ex((str(ip), self.port))
        if scanning_result == 0:
            self.potential_dbs.append(f"{ip}:{self.port}")
        scanning_socket.close()

    def run_scan(self):
        num_ips = len(self.ipaddr)
        if num_ips == 0:
            return

        ips_per_thread = max(num_ips // self.threads, 1)
        num_threads = min(num_ips, self.threads)

        threads = []
        for thread_id in range(num_threads):
            if thread_id == num_threads - 1:
                ip_addrs = self.ipaddr[thread_id * ips_per_thread]
            else:
                ip_addrs = self.ipaddr[thread_id * ips_per_thread: (thread_id + 1) * ips_per_thread]
            if not isinstance(ip_addrs, list):
                ip_addrs = [ip_addrs]

            scan_thread = threading.Thread(target=thread_scanner, args=(self, thread_id, ip_addrs))
            threads.append(scan_thread)

        for scan in threads:
            scan.start()

        for scan in threads:
            scan.join()
