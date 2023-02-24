import argparse
import asyncio
import json
import os
from typing import List

import tqdm

import elastic_api.abstract_filters as abstract_filters
import elastic_api.async_elastic_api as elastic_api
import elastic_api.filters as filters
import utils.async_scanner as async_scanner
import utils.cli_helper as cli_helper
import utils.ip_utils as ip_utils
import utils.parser as util_parser


def load_filters_from_file(filename: str) -> List[abstract_filters.Filter]:
    """
    Load a list of filters from a JSON file.

    Args:
        filename: The name of the JSON file to load filters from.

    Returns:
        A list of Filter objects.

    Raises:
        ValueError: If an unknown filter type is encountered.
    """
    data_filters = []
    with open(filename, 'r', encoding='utf8') as f:
        filter_configs = json.load(f)
        for filter_config in filter_configs:
            filter_name = filter_config["filter_name"]
            filter_type = filter_config["filter_type"]
            field_name = filter_config["field_name"]
            filter_items = filter_config["filter_items"]

            # Determine the filter type and instantiate the appropriate class
            if filter_type == "regex_dict":
                new_filter = filters.RegexDictFilter(field_name)
                for item in filter_items:
                    new_filter.add_filter(item)
            elif filter_type == "regex":
                new_filter = filters.RegexFilter(field_name)
                for item in filter_items:
                    new_filter.add_filter(item)
            else:
                raise ValueError(f"Unknown filter type: {filter_type}")

            data_filters.append(new_filter)

    return data_filters

class AsyncCLI:
    """
    Command-line interface for AsyncScanner and ElasticAPI classes.
    
    This class provides a CLI interface for creating and running an `AsyncScanner` 
    object to scan for potential
    Elasticsearch databases and an `ElasticAPI` object to download data 
    from Elasticsearch databases.
    
    The CLI supports two main commands: `scanner` and `elastic`. The `scanner` 
    command scans a given IP address or range for open ports and then attempts 
    to identify potential Elasticsearch databases. The `elastic` command connects
    to a given Elasticsearch database and downloads data according to the specified filters 
    (if any).
    
    Usage:
    >>> cli = AsyncCLI()
    >>> args = cli.parser.parse_args()
    >>> await cli.run(args)
    
    Arguments:
        None
    
    Methods:
        scan_db(db: str, args: argparse.Namespace) -> Coroutine:
            Checks if a given IP address is an Elasticsearch database and downloads
            data from it if it is.
        
        run(args: argparse.Namespace) -> Coroutine:
            Parses command-line arguments and runs the specified command (scanner or elastic).
    
    Attributes:
        None
    """
    # EXPERIMENTAL - NOT IMPLEMENTED YET
    FILENAME_SUBSTITUTIONS = {
        "Ih": "health",
        "Is": "status",
        "Iu": "uuid",
        "Ip": "pri",
        "Ir": "rep",
        "Dc": "docs_count",
        "Dd": "docs_deleted",
        "Sz": "store_size",
        "Pz": "pri_store_size"
    }

    # EXPERIMENTAL - NOT IMPLEMENTED YET
    FOLDER_SUBSTITUTIONS = {
        "Dn": "name",
        "Cn": "cluster_name",
        "Vn": "version_number",
        "Bf": "build_flavor",
        "Bt": "build_type",
        "Bs": "build_snapshot",
        "Lv": "lucene_version"
    }

    def __init__(self):
        self.parser = argparse.ArgumentParser(description="AsyncScanner/ElasticAPI CLI", 
                                              formatter_class=argparse.RawTextHelpFormatter)

        self.parser.add_argument("ipaddr", type=str, help="IP address or range to scan")
        self.parser.add_argument("port", type=int, default=9200, 
                                 help="Port to scan, defaults to 9200")

        # AsyncScanner parser
        scanner_parser = self.parser.add_argument_group("scanner options")
        scanner_parser.add_argument(
            "-sG", "--staged", action="store_true", help="Perform the scan in stages"
        )
        scanner_parser.add_argument(
            "-nS", "--numstages", type=int, default=16, help="CIDR Subnet for each scan stage. Use with --staged."
        )
        scanner_parser.add_argument(
            "-sT", "--scannertimeout", type=float, default=1.0, help="Scanner timeout"
        )
        scanner_parser.add_argument(
            "-nW", "--numworkers", type=int, default=4, help="Number of workers"
        )
        scanner_parser.add_argument(
            "-mS", "--maxsubnets", type=int, default=16, help="Maximum number of subnets"
        )
        scanner_parser.add_argument(
            "-mH",
            "--maxhosts",
            type=int,
            default=256,
            help="Maximum number of hosts per subnet",
        )

        # ElasticAPI parser
        elastic_parser = self.parser.add_argument_group("ElasticAPI Options")
        elastic_parser.add_argument(
            "-eT", "--elastictimeout", type=float, default=1.0, help="Elasticsearch timeout"
        )
        elastic_parser.add_argument(
            "-dp",
            "--downloadpath",
            type=str,
            default=os.getcwd(),
            help="Download path for Elasticsearch data",
        )
        elastic_parser.add_argument(
            "-f",
            "--filters",
            type=str,
            help="Filters to apply to Elasticsearch data (JSON File)",
        )

        single_downloader = self.parser.add_argument_group("Single DB Download Options")
        # We don't need to specify host or port because they're global
        single_downloader.add_argument(
            "--fieldname",
            "-fn",
            action='append',
            default=None,
            help="Field Name(s) to download. Use once for each Field Name"
            )
        single_downloader.add_argument(
            "--index",
            "-iX",
            type=str,
            help="Index to Download"
        )
        single_downloader.add_argument(
            "--output",
            "-o",
            type=str,
            help="Filename to store crawled data"
        )
        # EXPERIMENTAL - NOT FULLY IMPLEMENTED YET
        export_options=self.parser.add_argument_group("Export Options")
        export_options.add_argument(
            "--folderformat",
            "-fF",
            type=str,
            help='The format string to use, with percent-encoded substitutions.\n'
                         'Possible substitutions:\n'
                         '  %%Dn  Name\n'
                         '  %%Cn  Cluster name\n'
                         '  %%Vn  Version number\n'
                         '  %%Bf  Build flavor\n'
                         '  %%Bt  Build type\n'
                         '  %%Bs  Build snapshot\n'
                         '  %%Lv  Lucene version'
                         ' Only works when downloading a single database.'
        )

        self.parser.add_argument(
            '-t',
            '--timeout',
            type=int,
            help='Timeout for both the scanner and the Elasticsearch API in seconds.')

        self.parser.add_argument(
            "--download",
            "-dL",
            action="store_true",
            default=False,
            help="Download Indices Automatically"
        )
        self.parser.add_argument(
            "--single",
            "-s",
            action="store_true",
            default=False,
            help="use the Single Download Module"
        )

    def parse_foldername(self, args: argparse.Namespace, 
                               elastic_api_obj: elastic_api.ElasticAPI):
        # Create a temporary parser for the folder format
        temp_parser=util_parser.PercentParser(self.FOLDER_SUBSTITUTIONS, elastic_api_obj.ElasticDB)
        return temp_parser.parse_string(args.folderformat)

    async def download_single_index(self, args: argparse.Namespace):
        """Download a single index

        Args:
            args (argparse.Namespace): CLI Args
        """
        if args.folderformat:
            download_path = os.path.join(args.downloadpath, args.folderformat)
        else:
            download_path = args.downloadpath

        host = f"http://{args.ipaddr}:{args.port}"
        db_api = elastic_api.ElasticAPI(
            host=host,
            timeout=args.elastictimeout,
            download_path=download_path,
            download=True,
            Filters=None
        )
        output_filename = args.output if args.output else args.index
        await db_api.get_db_info()
        await db_api.get_db_indicies()
        await db_api.download_index(
            host=host,
            index=args.index,
            timeout=args.timeout,
            filename=output_filename,
            download_path=download_path,
            folder_name=None,
            fieldnames=args.fieldname
            )

    async def scan_db(self, db: str, args: argparse.Namespace):
        """Scan Host for databases

        Args:
            db (str): host IP/Port
            args (argparse.Namespace): CLI Args
        """
        elastic_filters = None
        if args.filters:
            elastic_filters = load_filters_from_file(args.filters)
        eapi = elastic_api.ElasticAPI(
            db,
            timeout=args.elastictimeout,
            download_path=args.downloadpath,
            download=args.download,
            Filters=elastic_filters,
        )
        await eapi.automate()

    async def run_scanner(self, args: argparse.Namespace):
        """Run the scanner

        Args:
            args (argparse.Namespace): CLI Args
        """
        scanner = async_scanner.AsyncScanner(
            args.ipaddr,
            args.port,
            timeout=args.scannertimeout,
            num_workers=args.numworkers,
            max_subnets=args.maxsubnets,
            max_hosts_per_subnet=args.maxhosts,
        )
        tqdm.tqdm.write("Scanning for hosts... (This may take a few minutes)")
        await scanner.run_scan()
        tasks: List[asyncio.Task] = []
        potential_dbs = scanner.potential_dbs
        tqdm.tqdm.write("Checking IPs...")
        for potential_database in potential_dbs:
            tasks.append(asyncio.create_task(self.scan_db(potential_database, args)))
        await asyncio.gather(*tasks)

    async def run_scan_staged(self, args: argparse.Namespace):
        """Run the staged scanner

        Args:
            args (argparse.Namespace): CLI Args
        """
        ip_addrs = ip_utils.split_subnet_into_subnets(args.ipaddr, args.numstages)
        tqdm.tqdm.write(f"Prepared {len(ip_addrs)} Stages for Scanning...")
        potential_databases = []
        tqdm.tqdm.write("Scanning for hosts... (This may take a few minutes)")
        for ip_addr_range in tqdm.tqdm(ip_addrs, position=1, desc="IP Ranges"):
            scanner = async_scanner.AsyncScanner(
                ip_addr_range,
                args.port,
                timeout=args.scannertimeout,
                num_workers=args.numworkers,
                max_subnets=args.maxsubnets,
                max_hosts_per_subnet=args.maxhosts,
            )
            await scanner.run_scan()
            potential_databases.extend(scanner.potential_dbs)
            #tqdm.tqdm.write(f"Found {len(scanner.potential_dbs)} Potential Databases")
        tasks: List[asyncio.Task] = []
        for potential_database in potential_databases:
            tasks.append(asyncio.create_task(self.scan_db(potential_database, args)))
        await asyncio.gather(*tasks)

    async def run_cli(self, args: argparse.Namespace):
        """Run the CLI

        Args:
            args (argparse.Namespace): CLI Args
        """
        cli_helper.print_banner()
        if args.single is True:
            await self.download_single_index(args)
        elif args.staged is True:
            await self.run_scan_staged(args)
        else:
            await self.run_scanner(args)

# Now Run The CLI
loop = asyncio.new_event_loop()
cli = AsyncCLI()
args = cli.parser.parse_args()
loop.run_until_complete(cli.run_cli(args))
