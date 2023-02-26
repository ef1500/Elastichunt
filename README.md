# Elastichunt

## Inspiration and purpose
This tool, inspired by Michael Bazzell from [Inteltechniques](https://inteltechniques.com), helps you search the web for open Elasticsearch databases for OSINT purposes.
The project was based off of a [resource](https://github.com/AmIJesse/Elasticsearch-Crawler) presented in his book, [Open Source Intelligence Techniques](https://inteltechniques.com/book1.html). The tool is great, however, it didn't encompass all the features I would have liked to have.

This program aims to provide you with the "swiss army knife" of elasticsearch databases. Allowing you to Search, locate, and download these databases all in one. 

## Features
- Built-in scanner to allow you to scan the internet, similar to [masscan](https://github.com/robertdavidgraham/masscan)
  - Full control over the number of workers, timeout and stages (staging allows you to split the search up into smaller scans)
- Built-in, OSINT-driven Elasticsearch API
  - Fully customizeable filters so you can download databases of interest automatically
  - Download Elasticsearch databases directly from the CLI
    - Automatically resolve database mapping and fieldnames
    - Prefer the functionality of AmIJesse's [tool](https://github.com/AmIJesse/Elasticsearch-Crawler)? No Problem! You can specify fieldnames you'd like to download directly from the CLI. Use the `-fn` argument for each fieldname you'd like to download.
    
### Elastichunt in action
![OPEMAS](https://user-images.githubusercontent.com/45581646/220704670-3abe8e9f-ebcf-43f6-92ec-e18de551b423.gif)


## Getting Started
### Installation
To get started, clone this repository and run `python3 -m pip install -r requirements.txt` to install the necessary requirements. 

Once that finishes, run `python3 elastichunt.py -h` to view the avaliable options. You should be greeted with a wall of options.
You do not need to use all of these options. Depending on the use case, a different combination of arguments will be used. 

## Basic Usage
Once you have installed Elastichunt, you can start using it right away. The following are a few basic examples of ElasticHunt's usage.

### Searching the internet for databases

To search databases, you can use the following command:

`python3 elastichunt.py 192.168.0.0/16 9200 --elastictimeout 16 --scannertimeout 16`

This will scan for any elasticdatabases in the given IP address or IP range. `--elastictimeout` is the timeout for the elastic API, and `--scannertimeout` is the timeout for the scanner. I've found that anything above 10 seems to work best. Play around and experiment to find what timeout best suits your circumstance.

**NOTE: THE PROGRESS BAR WILL NOT START RIGHT AWAY**

### Downloading databases

To Download a database index, you can use the following command:

`python3 elastichunt.py 192.168.1.1 9200 --elastictimeout 16 --index user_index --single`

This will download the `user_index` index, and will download it to the current path. Using the `--single` argument tells elastichunt that we want to download a single index. Elastichunt will automatically resolve the fieldnames on its own, but if you would like to specify your own, you can use the `-fn` argument once for each fieldname you would like to download. (e.g. `-fn username -fn display_name -fn email`)

To download indices automatically, you can use the `--download` argument like so:
`python3 elastichunt.py 192.168.0.0 --elastictimeout 16 --scannertimeout 16 --download`
- NOTE: I reccomend using filters when downloading indices automatically. Some servers have thousands of logs, and if your filters aren't on, you may end up downloading over a terabyte of redundant information!

### Using filters

Filters do exactly what you think they allow you to do. They let you filter indices based on different criteria. Filters are completely customizeable, and are extremely convenient when you want to download databases automatically.

Here is what an example filters.json file may look like:
```json
[
    {
      "filter_name": "allowed_indices",
      "filter_type": "regex",
      "field_name": "index",
      "filter_items": ["users", "customer"]
    },
    {
      "filter_name": "allowed_filesizes",
      "filter_type": "regex",
      "field_name": "store_size",
      "filter_items": ["mb"]
    }
  ]
```
- `filter_name` - Can be whatever you want, it's just to let you know what the filter's for if you ever revise the file
- `filter_type` - The type of filter to use. as of now "regex" is only supported.
- `field_name` - The field name to filter on. This is **not** the fieldnames of the data. This refers to the dataclass field names for each index, and has the following field names: `health`, `status`, `index`, `uuid`, `pri`, `rep`, `docs_count`, `docs_deleted`, `store_size` and `pri_store_size`.
- `filter_items` - The items you would like the filter to find. Will match any indices with the specified items.
You can then add the filters to the cli like so:

`python3 elastichunt.py 192.168.0.0/16 9200 --elastictimeout 16 --scannertimeout 16 --filters=filters.json`

## Advanced usage
Sometimes the basic arguments aren't enough to get decent results. If you scan large parts of the internet, your computer will error out, lose funtionality, and return dissapointing results, or none at all. To fix this, I have provided more options to provide you with full control over the scanner. 

### Using `--staged`

This argument splits up the scans into smaller scans. It does this by attempting to convert the IP range into a CIDR range, and then splitting it into smaller CIDR ranges. For example 192.0.0.0/8 would be split into 256 stages of the /16 subnet, 192.0.0.0/16, 192.1.0.0/16 ... 192.255.0.0/16. You can change which CIDR Subnet elastichunt splits the stages into by using the `--numstages` option.

Example: `python3 elastichunt.py 192.0.0.0/8 --elastictimeout 16 --scannertimeout 16 --filters=filters.json --staged`

### Using `--numworkers`,  `--maxhosts`, and `--maxsubnets`

- `--numworkers` is the number of semaphore tasks python will use. This defaults to 4.
WARNING: Increasing this number will also increase memory usage, use at your own peril!

- `--maxhosts` is the maximum number of hosts per subnet. Defaults to 256, Not really too useful.

- `--maxsubnets` is the number of subnets per worker. Each worker is assigned a certain number of subnets to scan, and once they finish, they get a new batch of subnets. The `--maxsubnets` determines the size of each batch. 
## Upcoming Feautres
These are features that I am working to implement currently (or hope to implement in the future):
- Adaptive Search Size (So you can download any database) DONE!
- No Warn Option (Sometimes the terminal gets really crowded because of the mapping warning)
- Better database checking when using the `--staged` option
- Cleaner CLI (The CLI gets really hard to read with all the text sometimes)
- Staged Database Checking (Like scanning, checking for databases can consume a lot of memory, and occasionally error out, leading to inconsistent results)
- More, better export options (Allow to create folder for each host, full customization of filenames, etc.)
