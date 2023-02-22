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

    INDICES_URL = "/_cat/indices?format=json"
    SEARCH_SIZE = 1000

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
    async def download_index(host, index, timeout, filename, download_path=os.getcwd(),
                            folder_name=None, fieldnames=None, export_format='csv'):
        """Download an index"""
        scroll_url = f"{host}/{index}/_search?scroll=180m&size={ElasticAPI.SEARCH_SIZE}"
        #scroll_fallback_url = f"{host}/{index}/_search?scroll=180m&size=500"
        async with aiohttp.ClientSession() as session:
            async with session.post(scroll_url, timeout=timeout) as scroll_request:
                scroll_data = await scroll_request.json()
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

                with open(file_path, 'w', encoding='utf8', newline="") as data_file:
                    writer = None
                    try:
                        total_hits = scroll_data["hits"]["total"]["value"]
                    except TypeError:
                        total_hits = len(scroll_data["hits"]["hits"])

                    # Keep scrolling until there are no more results
                    with tqdm.tqdm(total=total_hits, desc="Downloading index") as pbar:
                        while True:
                            # Get the next scroll batch
                            url = f"{host}/_search/scroll?scroll=180m&scroll_id={scroll_id}"
                            async with session.get(url, timeout=timeout) as scroll_request:
                                scroll_data = await scroll_request.json()

                                # Find any hits
                                hits = scroll_data["hits"]["hits"]
                                # If we're not getting any hits, we're either getting ratelimited
                                # Or our search size is too high. Almost always the former.
                                # TODO: IMPLEMENT ADAPTIVE SEARCH SIZE
                                if not hits:
                                    break

                                # Write each hit to the CSV file
                                for hit in hits:
                                    # Get the fields from the hit and set up the CSV writer 
                                    # if necessary
                                    source = hit["_source"]
                                    if fieldnames:
                                        source = {key: value for key, value in
                                                source.items() if key in fieldnames}
                                    if not writer:
                                        if not fieldnames:
                                            fieldnames = source.keys()
                                        if export_format == 'csv':
                                            writer = csv.DictWriter(data_file,
                                                                    fieldnames=fieldnames)
                                            writer.writeheader()
                                        else:
                                            writer = data_file

                                    # Write the hit data to the file
                                    if export_format == 'csv':
                                        writer.writerow(source)
                                    else:
                                        writer.write(json.dumps(source).encode('utf8') + b'\n')

                                    pbar.update(1)

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