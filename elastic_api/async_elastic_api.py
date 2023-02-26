# Open Elastic API
import asyncio
import csv
import json
import os
from dataclasses import asdict, dataclass

import aiohttp
import prettytable
import tqdm

class ElasticAPI(object):
    
    """This is the Async ELastic api (Woah!)
    We will use this to download databases and gather
    importatnt information on them.
    """

    # BASIC OPTIONS
    INDICES_URL = "/_cat/indices?format=json"
    SEARCH_SIZE = 5700

    @dataclass
    class ElasticDatabase:
        """
        Elastic Database Information
        """
        name: str = ""
        cluster_name: str = ""
        cluster_uuid: str = ""
        version_number: str = ""
        build_flavor: str = ""
        build_type: str = ""
        build_hash: str = ""
        build_date: str = ""
        build_snapshot: bool = False
        lucene_version: str = ""
        minimum_wire_compatibility_version: str = ""
        minimum_index_compatibility_version: str = ""
        tagline: str = ""

    @dataclass
    class ElasticIndex:
        """
        Elastic Index Field Names
        """
        health: str
        status: str
        index: str
        uuid: str
        pri: str
        rep: str
        docs_count: str
        docs_deleted: str
        store_size: str
        pri_store_size: str

    def __init__(self, host, download_path=os.getcwd(), timeout=1, Filters=None, download=False):
        self.host = host
        self.timeout = timeout
        self.download_path = download_path
        self.download = download

        self.iselastic = None
        self.indices = list()
        self.index_schema = list() # List of Lists, where each list contains
        # the field names for each index
        self.Filters = Filters
        self.ElasticDB = None
        self.filtered_indices = list()
        
        # Clean the hostname for folder naming purposes
        self.clean_host = self.host[7:-5]

    async def is_elastic(self):
        """Check if the Host is an elasticsearch database"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.host}/_cat", timeout=self.timeout) as response:
                    rtext = await response.text()
                    if "=^.^=" in rtext:
                        self.iselastic = True
                    else:
                        self.iselastic = False
        except Exception:
            self.iselastic = False

    async def get_db_info(self):
        """Retrieve the Elastic Database Information"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.host, timeout=self.timeout) as response:
                    data = await response.text()
                    json_data = json.loads(data)

                    self.ElasticDB = ElasticAPI.ElasticDatabase(
                    name=json_data.get("name", ""),
                    cluster_name=json_data.get("cluster_name", ""),
                    cluster_uuid=json_data.get("cluster_uuid", ""),
                    version_number=json_data.get("version", {}).get("number", ""),
                    build_flavor=json_data.get("version", {}).get("build_flavor", ""),
                    build_type=json_data.get("version", {}).get("build_type", ""),
                    build_hash=json_data.get("version", {}).get("build_hash", ""),
                    build_date=json_data.get("version", {}).get("build_date", ""),
                    build_snapshot=json_data.get("version", {}).get("build_snapshot", False),
                    lucene_version=json_data.get("version", {}).get("lucene_version", ""),
                    minimum_wire_compatibility_version=json_data.get(
                        "version", {}).get("minimum_wire_compatibility_version", ""),
                    minimum_index_compatibility_version=json_data.get(
                        "version", {}).get("minimum_index_compatibility_version", ""),
                    tagline=json_data.get("tagline", "")
                )
        except Exception as e:
            tqdm.tqdm.write(f"Error retrieving database information: {e}")
            return None

    async def get_db_indicies(self):
        """Retrieve the elastic DB Indicies"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.host + ElasticAPI.INDICES_URL, 
                                       timeout=self.timeout) as response:
                    data = await response.text()
                    json_data = json.loads(data)

                    for index in json_data:
                        elastic_index = ElasticAPI.ElasticIndex(
                            health=index.get("health", ""),
                            status=index.get("status", ""),
                            index=index.get("index", ""),
                            uuid=index.get("uuid", ""),
                            pri=index.get("pri", ""),
                            rep=index.get("rep", ""),
                            docs_count=index.get("docs.count", ""),
                            docs_deleted=index.get("docs.deleted", ""),
                            store_size=index.get("store.size", ""),
                            pri_store_size=index.get("pri.store.size", "")
                        )
                        self.indices.append(elastic_index)

        except Exception as e:
            tqdm.tqdm.write(f"Error retrieving database indicies from {self.host}: {e}")
            tqdm.tqdm.write(f"Indicies may be private. Navigate to {self.host}/_cat/indices")
            tqdm.tqdm.write("To Check if the indices are hidden. They may be exposed by")
            tqdm.tqdm.write(f"Requesting {self.host}/_mapping")

    async def filter_db_indices(self):
        """Filter Database Indicies"""
        if self.Filters is None:
            self.filtered_indices = self.indices
        if self.Filters is not None:
            indices = self.indices
            for Filter in self.Filters:
                f_indices = Filter.apply(indices)
                indices = f_indices
            self.filtered_indices = indices

    @staticmethod
    async def get_fieldnames_from_index_mapping(host, index, timeout):
        """Get the fieldnames from an Elasticsearch index mapping"""
        disallowed_types = ['alias', 'completion', 'aggregate_metric_double', 'dense_vector',
                            'rank_feature', 'rank_features', 'properties']
        mapping_url = f"{host}/{index}/_mapping"
        async with aiohttp.ClientSession() as session:
            async with session.get(mapping_url, timeout=timeout) as mapping_request:
                mapping_data = await mapping_request.json()

                # Find the mapping for the index
                index_mapping = mapping_data[index]['mappings']

                # Get the fieldnames from the mapping
                fieldnames = []
                for mapping in index_mapping.values():
                    try:
                        for fieldname, field_mapping in mapping.items():
                            if mapping.get('properties', {}):
                                properties = mapping.get('properties', {})
                                for fieldname, field_mapping in properties.items():
                                    if field_mapping.get('type'):
                                        fieldnames.append(fieldname)
                            elif field_mapping.get('type') not in disallowed_types:
                                fieldnames.append(fieldname)
                    except AttributeError as err:
                        print(f"Failed to get index mapping for {host}: {err}")
                        break
                return fieldnames

    @staticmethod
    async def fetch_scroll_id(session, host, index, timeout, scroll_time="720m", search_size=1000):
        """Fetch a scroll ID

        Args:
            session (aiohttp.ClientSession()): session object
            host (str): host
            index (str): Name of index to scroll
            timeout (int): _description_
            scroll_time (str, optional): How long to scroll. Defaults to "720m".
            search_size (int, optional): How many records to return. Defaults to 1000.

        Returns:
            str: scroll ID
        """
        scroll_url = f"{host}/{index}/_search?scroll={scroll_time}&size={search_size}"
        async with session.post(scroll_url, timeout=timeout) as scroll_request:
            # Return the scroll Data
            return await scroll_request.json()

    @staticmethod
    async def fetch_scroll_data(session, host, timeout, scroll_id, scroll_time="720m", 
                                retry_count=10, retry_delay=3):
        """Fetch Data With the scroll API"""
        fetch_url = f"{host}/_search/scroll?scroll={scroll_time}&scroll_id={scroll_id}"
        for i in range(retry_count):
            try:
                async with session.get(fetch_url, timeout=timeout) as scroll_request:
                    scroll_data = await scroll_request.json()
                    await asyncio.sleep(0) # To avoid payload not completed?
                    # Find any hits in the data and return them
                    hits = scroll_data["hits"]["hits"]
                    # If we're not getting any hits, we're either getting ratelimited
                    # Or our search size is too large
                    # TODO: Implement Adaptive Search Size
                    if not hits:
                        return

                    return hits
            except Exception as ex:
                # If we hit an exception, and the number of retries hasn't
                # Exceeded retry_count, then try again in retry_delay seconds.
                if i < retry_count - 1:
                    await asyncio.sleep(retry_delay)
                else:
                    # If there's been too many retries, give up.
                    raise ex

    @staticmethod
    async def export_scroll_data(fetch_hits, data_file, writer,
                                 fieldnames=None, export_format='csv', writeheader=False):
        """Export Scroll Data

        Args:
            fetch_hits (list): list of hits we fetched
            data_file (fileobj): context handler to our output file
            fieldnames (list): fieldnames to export (optional)
            export_format (str): what fileformat to export in
        """
        # Iterate through the hits we fetched earlier
        for fetch_hit in fetch_hits:
            source = fetch_hit["_source"]
            if fieldnames:
                # If There are fieldnames only write the ones with the data we're
                # Interested in
                source = {key: value for key, value in
                        source.items() if key in fieldnames}
            if not writer:
                if not fieldnames:
                    fieldnames = source.keys()
                if export_format == 'csv':
                    writer = csv.DictWriter(data_file,
                                            fieldnames=fieldnames)
                    if writeheader is True:
                        writer.writeheader()
                else:
                    writer = data_file

            # Write the hit data to the file
            if export_format == 'csv':
                writer.writerow(source)
            else:
                writer.write(json.dumps(source).encode('utf8') + b'\n')

    async def download_index(self, host, index, timeout, filename, download_path=os.getcwd(),
                            folder_name=None, fieldnames=None, export_format='csv'):
        """Download an index"""
        async with aiohttp.ClientSession() as session:
            scroll_data = await self.fetch_scroll_id(
                session, host, index,timeout, scroll_time="720m", search_size=self.SEARCH_SIZE)
            scroll_id = scroll_data["_scroll_id"]

            if export_format not in ['csv', 'json']:
                raise ValueError(f"Invalid export format: {export_format}. \
                                Supported formats are 'csv' and 'json'")

            folder_path = os.path.join(download_path,
                                    folder_name) if folder_name else download_path
            os.makedirs(folder_path, exist_ok=True)

            file_path = os.path.join(folder_path, f"{filename}.{export_format}")

            if not fieldnames:
                fieldnames = await ElasticAPI.get_fieldnames_from_index_mapping(
                    host, index, timeout=timeout)

            try:
                if isinstance(scroll_data["hits"]["total"], int):
                    total_hits = scroll_data["hits"]["total"]
                else:
                    total_hits = scroll_data["hits"]["total"]["value"]
            except TypeError:
                total_hits = len(scroll_data["hits"]["hits"])

            with open(file_path, 'w', encoding='utf8', newline="") as data_file:
                with tqdm.tqdm(total=total_hits, desc="Downloading index") as pbar:
                    writer=None
                    accumulated_hits = 0

                    await self.export_scroll_data(
                        scroll_data["hits"]["hits"],data_file, writer, fieldnames,export_format, writeheader=True)
                    
                    accumulated_hits += len(scroll_data["hits"]["hits"])
                    
                    pbar.update(len(scroll_data["hits"]["hits"]))

                    # Keep scrolling until there are no more results
                    while accumulated_hits <= total_hits:
                        # Retrieve the hits from the database
                        hits = await self.fetch_scroll_data(session, host, timeout,
                                                            scroll_id, scroll_time="720m")

                        if not hits:
                            break

                        # Write the hits to the file
                        await self.export_scroll_data(hits, data_file,
                                                        writer, fieldnames, export_format)
                        # Update the progress bar
                        pbar.update(len(hits))

                        # Update the accumulated hits
                        accumulated_hits += len(hits)

                        # Get the next Scroll ID
                        scroll_id = scroll_data["_scroll_id"]

                print(f"Index downloaded and saved to {file_path}")


    async def download_index_single(self, index, fieldnames=None):
        """Download Filtered Indices"""
        print(f"Downloading {index}")
        await self.download_index(self.host, index, self.timeout, index,
                                  self.download_path, fieldnames=fieldnames)

    async def download_indices(self):
        """Download Filtered Indices"""
        for Index in self.filtered_indices:
            print(f"Downloading {Index.index}")
            await self.download_index(self.host, Index.index, self.timeout,
                                      Index.index, self.download_path)

    async def automate(self):
        await self.is_elastic()
        if self.iselastic is False:
            return
        await self.get_db_info()
        await self.get_db_indicies()
        await self.filter_db_indices()

        if self.filtered_indices:
            table = prettytable.PrettyTable()
            table.field_names = ["Index", "Docs Count", "Store Size", "Search URL"]
            table.align["Index"] = "l"
            table.align["Docs Count"] = "r"
            table.align["Store Size"] = "r"
            table.align["Search URL"] = "l"

            table.title = f"{self.host} | {self.ElasticDB.name}"

            for index in self.filtered_indices:
                table.add_row([index.index, index.docs_count, index.store_size,
                               f"{self.host}/{index.index}/_search"])

            print(table)

            if self.download is True:
                await self.download_indices()
