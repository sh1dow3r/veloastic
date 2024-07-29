
# Veloastic 

This repository contains a Python script for ingesting JSON data from ZIP files into Elasticsearch, optimized for bulk upload and duplicate prevention.

## Features

- Bulk upload to Elasticsearch
- Duplicate record prevention
- Configuration via `.ini` file
- Comprehensive logging and error handling
- Supports multiple operations (ingest, enrich_download, enrich_upload)

## Configuration

Create a `.ini` file with the following structure:

```ini
[logging]
level = INFO

[elasticsearch]
host = cloud.elastic.come
port = 443
scheme = https
username = myusername
password = mypassword

[cases]
myindex01 = /Users/x/Documents/Collections/c01
myindex02 = /Users/x/Documents/Collections/c02

```

## Getting Started

### Prerequisites

- Python 3.9+
- Elasticsearch 7.x
- `pip` package manager

### Installation

```bash
git clone https://github.com/sh1dow3r/veloastic/
cd veloastic
pip install -r requirements.txt
```

## Usage

Run the script:

```bash
python veloastic.py --config path/to/your/config.ini
```
OR 

Specify an operation (ingest, enrich_download, or enrich_upload):

```bash
python veloastic.py --config path/to/your/config.ini --operation <operation>
```

## Directory Structure

```
veloastic/
│
├── veloastic.py
├── config.ini
├── requirements.txt
└── README.md
```

## Logging

The script logs its activity per JSON file, including errors and warnings.


## Contributing

1. Fork the repository.
2. Create a branch.
3. Make your changes.
4. Commit and push.
5. Submit a pull request.

## License

Licensed under the MIT License.
