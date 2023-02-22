import socket
import logging
import asyncio

import tqdm

from utils.ip_utils import ip_range_to_list, split_ip_list_into_subnets

logger = logging.getLogger('AsyncScanner')
logging.basicConfig(level=logging.INFO)

class AsyncScanner:
    """AsyncScanner Class.
    This class is used to create an asynchronous scanner.
    """

    SOCKET_FAMILY = socket.AF_INET
    SOCKET_KIND = socket.SOCK_STREAM

    def __init__(self, ipaddr, port, timeout=1, num_workers=4, max_subnets=16, max_hosts_per_subnet=256):
        self.ipaddr = ip_range_to_list(ipaddr)
        self.port = port
        self.timeout = timeout
        self.num_workers = num_workers
        self.max_subnets = max_subnets
        self.max_hosts_per_subnet = max_hosts_per_subnet
        
        self.potential_dbs = []

    async def scan_ip(self, ip, progress_queue):
        scanning_socket = socket.socket(AsyncScanner.SOCKET_FAMILY, AsyncScanner.SOCKET_KIND)
        scanning_socket.settimeout(self.timeout)

        try:
            await asyncio.get_running_loop().sock_connect(scanning_socket, (str(ip), self.port))
            self.potential_dbs.append(f"http://{ip}:{self.port}")
        except:
            pass
        finally:
            scanning_socket.close()
            await progress_queue.put(ip)

    async def scan_subnet(self, subnet, progress_queue):
        tasks = [self.scan_ip(ip, progress_queue) for ip in subnet]
        await asyncio.gather(*tasks)

    async def run_scan(self):
        progress_queue = asyncio.Queue()
        num_targets = len(self.ipaddr)
        pbar = tqdm.tqdm(total=num_targets, position=0, desc='Scanning IPs', unit='ip', dynamic_ncols=True)
        progress_message = 'Scanned {} out of {} IPs'

        if len(self.ipaddr) == 1:
            async with asyncio.Semaphore(self.num_workers):
                tasks = [self.scan_subnet(self.ipaddr, progress_queue)]
                await asyncio.gather(*tasks)

        if len(self.ipaddr) >= self.max_subnets:
            subnets = split_ip_list_into_subnets(self.ipaddr, self.max_subnets)

            async with asyncio.Semaphore(self.num_workers):
                tasks = [self.scan_subnet(subnet, progress_queue) for subnet in subnets]
                await asyncio.gather(*tasks)

        elif len(self.ipaddr) <= self.max_subnets:
            async with asyncio.Semaphore(self.num_workers):
                tasks = [self.scan_subnet(subnet, progress_queue) for subnet in self.ipaddr]
                await asyncio.gather(*tasks)

        # Print final progress message
        while not progress_queue.empty():
            ip = await progress_queue.get()
            pbar.update(1)
            pbar.set_description(progress_message.format(pbar.n, num_targets))

        pbar.close()