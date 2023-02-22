# Open Elastic API
from dataclasses import dataclass
import os
import re
import csv
import json
import requests

class ElasticAPI(object):
    
    INDICES_URL = "/_cat/indices?format=json"
    SEARCH_SIZE = 1000
    SEARCH_HEADERS = {"Content-Type": "application/json"}
    
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
        
    def __init__(self, host, download_path=os.getcwd(), timeout=1, Filters=None):
        self.host = host
        self.timeout = timeout
        self.download_path = download_path
        
        self.iselastic = None
        self.indices = list()
        self.index_schema = list() # List of Lists, where each list contains
        # the field names for each index
        self.Filters = Filters
        self.ElasticDB = None
        self.filtered_indices = list()
        
    def is_elastic(self):
        """Check if the Host is an elasticsearch database"""
        try:
            rtext = requests.get(f"{self.host}/_cat", timeout=self.timeout)
            if "=^.^=" in rtext.text:
                self.iselastic = True
            else:
                self.iselastic = False
        except Exception:
            self.iselastic = False

    def get_db_info(self):
        """Retrieve the Elastic Database Information"""

        data = requests.get(self.host, timeout=self.timeout)
        json_data = json.loads(data.text)

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
        
    def get_db_indicies(self):
        """Retrieve the elastic DB Indicies"""
        data = requests.get(self.host + ElasticAPI.INDICES_URL, timeout=self.timeout)
        json_data = json.loads(data.text)
        
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

    def filter_db_indices(self):
        """Filter Database Indicies"""
        if self.Filters is None:
            self.filter_db_indices = self.indices
            return
        for Filter in self.Filters:
            f_indices = Filter.apply(self.indices)
            self.filtered_indices.extend(f_indices)
            
    @staticmethod
    def download_index(host, index, timeout, filename, download_path=os.getcwd()):
        """Download an index"""
        scroll_url = f"{host}/{index}/_search?scroll=180m&size={ElasticAPI.SEARCH_SIZE}"
        scroll_request = requests.post(scroll_url, headers=ElasticAPI.SEARCH_HEADERS, timeout=timeout)

        scroll_data = json.loads(scroll_request.text)
        scroll_id = scroll_data["_scroll_id"]

        with open(os.path.join(download_path, f"{filename}.csv"), "w", encoding='utf8', newline="") as csv_file:
            writer = None

            # Keep scrolling until there are no more results
            while True:
                # Get the next scroll batch
                url = f"{host}/_search/scroll?scroll=180m&scroll_id={scroll_id}"
                scroll_request = requests.post(url)
                scroll_data = json.loads(scroll_request.text)

                # Find any hits
                hits = scroll_data["hits"]["hits"]
                if not hits:
                    break

                # Write each hit to the CSV file
                for hit in hits:
                    # Get the fields from the hit and set up the CSV writer if necessary
                    source = hit["_source"]
                    if not writer:
                        fieldnames = source.keys()
                        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                        writer.writeheader()

                    
                # Get the next Scroll ID
                scroll_id = scroll_data["_scroll_id"]
                
    def download_indices(self):
        """Download Filtered Indices"""
        for Index in self.filtered_indices:
            print(f"Downloading {Index.index}")
            self.download_index(self.host, Index.index, self.timeout, Index.index, self.download_path)
            
    def automate(self):
        self.is_elastic()
        if self.iselastic is False:
            return
        self.get_db_info()
        self.get_db_indicies()
        self.filter_db_indices()
        print(f"{self.ElasticDB.name}")
        print(f"{self.indices}")