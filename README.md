
# Elasticsearch JSON Ingestion Script

This repository contains a Python script for ingesting JSON data from ZIP files into Elasticsearch, optimized for bulk upload and duplicate prevention.

## Features

- Bulk upload to Elasticsearch
- Duplicate record prevention
- Configuration via `.ini` file
- Comprehensive logging and error handling

## Configuration

Create a `.ini` file with the following structure:

```ini
[elasticsearch]
host = your-elasticsearch-host
port = 9200
scheme = https
username = your-username
password = your-password

[cases]
case1 = /path/to/folder1
case2 = /path/to/folder2

[logging]
level = INFO
```

## Getting Started

### Prerequisites

- Python 3.9+
- Elasticsearch 7.x
- `pip` package manager

### Installation

```bash
git clone https://github.com/yourusername/elasticsearch-json-ingestion.git
cd elasticsearch-json-ingestion
pip install -r requirements.txt
```

## Usage

Run the script:

```bash
python ingest.py --config path/to/your/config.ini
```

## Directory Structure

```
elasticsearch-json-ingestion/
│
├── ingest.py
├── config.ini
├── requirements.txt
└── README.md
```

## Logging

The script logs its activity, including errors and warnings.

## Contributing

1. Fork the repository.
2. Create a branch.
3. Make your changes.
4. Commit and push.
5. Submit a pull request.

## License

Licensed under the MIT License.
