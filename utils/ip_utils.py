import ipaddress

def dashed_ip_range_to_list(ipaddr):
    """Convert IP Address or IP Address range to list

    Args:
        ipaddr (str): ipaddr or ipaddr range

    Returns:
        list : list of ip addresses
    """
    range_to_list = lambda ip_range: [str(ip) for ip in ipaddress.summarize_address_range(
        ipaddress.IPv4Address(ip_range.split('-')[0]),
        ipaddress.IPv4Address(ip_range.split('-')[1]))]
    ip_ranges = range_to_list(ipaddr)
    ip_list = list()
    for ip_range in ip_ranges:
        ip_list.extend(ip_range_to_list(ip_range))
    return ip_list

def ip_range_to_list(ipaddr):
    """Convert IP Address or IP Address range to list

    Args:
        ipaddr (str): ipaddr or ipaddr range

    Returns:
        list : list of ip addresses
    """
    if '/' in ipaddr: # IP range using slash notation (e.g. "192.168.0.0/16")
        range_to_list = lambda ip_range: [str(ip) for ip in ipaddress.IPv4Network(ipaddress.ip_network(ip_range, strict=False))]
        return range_to_list(ipaddr)
    if '-' in ipaddr: # Standalone IP address (e.g. "192.168.1.1")
        return dashed_ip_range_to_list(ipaddr)
    else: # Standalone IP address (e.g. "192.168.1.1")
        return [ipaddr]

def split_ip_list_into_subnets(ip_list, num_subnets):
    """Splits the given IP range into the specified number of subnets.

    Args:
        ip_list (list): The IP range to split (in CIDR notation).
        num_subnets (int): The number of subnets to split the IP range into.

    Returns:
        A list of `ipaddress.IPv4Network` objects representing the subnets.
    """
    sublist_length = len(ip_list) // num_subnets
    remainder = len(ip_list) % num_subnets
    # Use a list comprehension to create the sublists
    sublists = [ip_list[i:i + (sublist_length + 1 if i < remainder else sublist_length)]
                for i in range(0, len(ip_list), sublist_length)]
    return sublists

def ip_range_to_cidr(ip_range):
    """
    Convert an IP range in the form of "start_ip-end_ip" to CIDR notation.

    Args:
        ip_range (str): A string representing the IP range, in the form of "start_ip-end_ip".

    Returns:
        str: A string representing the CIDR notation for the given IP range.

    Raises:
        ValueError: If the input IP range is invalid.

    Example:
        >>> ip_range_to_cidr('192.168.1.1-192.168.2.5')
        '192.168.1.0/24'

    """
    start_ip, end_ip = ip_range.split('-')
    start_ip_int = int(ipaddress.IPv4Address(start_ip))
    end_ip_int = int(ipaddress.IPv4Address(end_ip))

    # Find the longest common prefix (LCP)
    xor = start_ip_int ^ end_ip_int
    lcp = 32
    while xor > 0:
        xor >>= 1
        lcp -= 1

    # Construct the CIDR notation
    network_address = ipaddress.IPv4Address(start_ip_int & (0xffffffff << (32 - lcp)))
    cidr_range = str(ipaddress.IPv4Network((network_address, lcp), strict=False))

    return cidr_range

def split_subnet_into_subnets(ip_addr, target_cidr=16):
    """Splits an IP Range (Slash Or Dashed)
    into smaller subnets.

    Args:
        ip_addr (str): IP range in CIDR notation (e.g., '192.168.0.0/16') or
                        dashed notation (e.g., '192.168.0.0-192.168.255.255')
        target_cidr (int): Number of /16 subnets to output. Defaults to 16.

    Returns:
        list: A list of subnets
    """
    ip_address = ip_addr
    if '-' in ip_addr:
        ip_address = ip_range_to_cidr(ip_addr)
    network = ipaddress.ip_network(ip_address, strict=False)
    subnets = network.subnets(new_prefix=target_cidr)
    return [str(subnet) for subnet in subnets]