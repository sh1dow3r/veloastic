import os
import zipfile
import json
import hashlib
from datetime import datetime
from pathlib import Path
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from tqdm import tqdm
import argparse
import configparser
from colorama import init, Fore
import concurrent.futures
import logging
from logging.handlers import RotatingFileHandler
import uuid
import backoff
from jsonschema import validate, ValidationError

# Initialize colorama and logging
init(autoreset=True)

# Set up logging with rotation
log_file = 'veloastic_script.log'
file_handler = RotatingFileHandler(log_file, maxBytes=1000000, backupCount=3)
console_handler = logging.StreamHandler()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[file_handler, console_handler])

# JSON Schema for configuration validation
CONFIG_SCHEMA = {
    "type": "object",
    "properties": {
        "elasticsearch": {
            "type": "object",
            "properties": {
                "host": {"type": "string"},
                "port": {"type": "string"},
                "scheme": {"type": "string"},
                "username": {"type": "string"},
                "password": {"type": "string"}
            },
            "required": ["host", "port", "scheme", "username", "password"]
        },
        "cases": {
            "type": "object",
            "patternProperties": {
                "^.*$": {"type": "string"}
            },
            "additionalProperties": False
        }
    },
    "required": ["elasticsearch", "cases"]
}

# Load configuration from an .ini file and validate against schema
def load_config(config_path):
    config = configparser.ConfigParser(interpolation=None)  # Disable interpolation
    config.read(config_path)
    config_dict = {s: dict(config.items(s)) for s in config.sections()}
    try:
        validate(instance=config_dict, schema=CONFIG_SCHEMA)
    except ValidationError as e:
        logging.error(Fore.RED + f"Configuration validation error: {e}")
        raise
    return config

# Parse command-line arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description='Process ZIP files and perform operations on JSON data.')
    parser.add_argument('--config', default='config.ini', help='Path to the configuration file. Default is "config.ini".')
    parser.add_argument('-o', '--operation', choices=['ingest', 'enrich_download', 'enrich_upload'], default='ingest', help='Operation to perform: ingest (default), enrich_download, enrich_upload.')
    return parser.parse_args()

# Configure Elasticsearch client with retry logic
@backoff.on_exception(backoff.expo, Exception, max_tries=3)
def configure_es(config):
    return Elasticsearch(
        hosts=[{"host": os.getenv('ES_HOST', config.get('elasticsearch', 'host')), "port": config.get('elasticsearch', 'port')}],
        http_auth=(os.getenv('ES_USER', config.get('elasticsearch', 'username')), os.getenv('ES_PASS', config.get('elasticsearch', 'password'))),
        scheme=config.get('elasticsearch', 'scheme'),
        timeout=60,  # Increase timeout to 60 seconds
        max_retries=3,  # Set the maximum number of retries
        retry_on_timeout=True,  # Enable retry on timeout
        connection_class=RequestsHttpConnection  # Use RequestsHttpConnection for more control
    )

# Create an index in Elasticsearch
@backoff.on_exception(backoff.expo, Exception, max_tries=3)
def create_index(es, index_name):
    try:
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name)
            logging.info(f"Index {index_name} created.")
        else:
            logging.info(f"Index {index_name} already exists.")
    except Exception as e:
        logging.error(Fore.RED + f"Error creating index {index_name}: {e}")

# Convert epoch time to UTC ISO format
def convert_epoch_to_iso(epoch_time):
    try:
        return datetime.utcfromtimestamp(epoch_time).strftime('%Y-%m-%dT%H:%M:%S')
    except Exception as e:
        logging.error(Fore.RED + f"Error converting epoch time: {e}")
        return None

# Recursively update SystemTime and cast long integers to strings in nested dictionaries
def process_json_fields(json_data):
    if isinstance(json_data, dict):
        for key, value in json_data.items():
            if key == 'SystemTime' and isinstance(value, (int, float)):
                json_data[key] = convert_epoch_to_iso(value)
            elif isinstance(value, int) and (value > 2**31 - 1 or value < -2**31):
                json_data[key] = str(value)
            else:
                process_json_fields(value)
    elif isinstance(json_data, list):
        for item in json_data:
            process_json_fields(item)

# Upload JSON data to Elasticsearch in bulk with retry logic
@backoff.on_exception(backoff.expo, Exception, max_tries=3)
def bulk_upload_json_to_elasticsearch(es, index_name, json_data_list):
    actions = [
        {
            "_index": index_name,
            "_id": hashlib.sha256(json.dumps(json_data, sort_keys=True).encode('utf-8')).hexdigest(),
            "_source": json_data,
        }
        for json_data in json_data_list
    ]
    try:
        helpers.bulk(es, actions)
    except Exception as e:
        logging.error(Fore.RED + f"Bulk indexing error: {e}")

# Process a JSON file handling NDJSON format
def process_json_file(es, json_file_path, index_name, processed_records):
    try:
        json_data_list = []
        with open(json_file_path, 'r') as json_file:
            for line in json_file:
                try:
                    json_data = json.loads(line)
                    json_data['sourcetype'] = json_file_path.name
                    
                    # Process fields for SystemTime conversion and long integer casting
                    process_json_fields(json_data)
                    
                    json_data_list.append(json_data)
                except json.JSONDecodeError as e:
                    logging.error(Fore.RED + f"Error decoding JSON object from line in file {json_file_path}: {e}")
        bulk_upload_json_to_elasticsearch(es, index_name, json_data_list)
        logging.info(Fore.GREEN + f"Successfully processed {json_file_path}")
    except Exception as e:
        logging.error(Fore.RED + f"Error processing file {json_file_path}: {e}")

# Save processed file's checksum to avoid reprocessing
def save_processed_file(hash_file_path, file_checksum):
    try:
        with open(hash_file_path, 'a') as hash_file:
            hash_file.write(file_checksum + '\n')
    except Exception as e:
        logging.error(Fore.RED + f"Error saving processed file checksum: {e}")

# Load processed files to avoid reprocessing
def load_processed_files(hash_file_path):
    try:
        if os.path.exists(hash_file_path):
            with open(hash_file_path, 'r') as hash_file:
                return set(line.strip() for line in hash_file)
    except Exception as e:
        logging.error(Fore.RED + f"Error loading processed files: {e}")
    return set()

# Calculate the checksum of a file
def calculate_file_checksum(file_path):
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, 'rb') as file:
            for byte_block in iter(lambda: file.read(4096), b""):
                sha256_hash.update(byte_block)
    except Exception as e:
        logging.error(Fore.RED + f"Error calculating file checksum: {e}")
    return sha256_hash.hexdigest()

# Extract ZIP files and ensure unique directory names with parallelization
def extract_and_rename_zip(zip_file_path, destination_dir):
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            unique_dir = Path(destination_dir) / f"{zip_file_path.stem}_{uuid.uuid4().hex}"
            unique_dir.mkdir(parents=True, exist_ok=True)
            zip_ref.extractall(unique_dir)
            logging.info(Fore.GREEN + f"Extracted {zip_file_path} to {unique_dir}")
    except Exception as e:
        logging.error(Fore.RED + f"Error extracting ZIP file {zip_file_path}: {e}")

# Process directories to find and extract ZIP files in parallel
def process_directories(folder, destination_dir):
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for path in folder.iterdir():
                if path.is_dir():
                    futures.append(executor.submit(process_directories, path, destination_dir))
                elif path.is_file() and path.suffix == '.zip':
                    futures.append(executor.submit(extract_and_rename_zip, path, destination_dir))
            for future in futures:
                future.result()
    except Exception as e:
        logging.error(Fore.RED + f"Error processing directories: {e}")

# Dummy function for enrich_download
def enrich_download():
    logging.info("Enrich download operation - to be implemented.")

# Dummy function for enrich_upload
def enrich_upload():
    logging.info("Enrich upload operation - to be implemented.")

def ingest(config):
    case_dirs = config.items('cases')

    es = configure_es(config)
    
    for case_id, folder_path in case_dirs:
        if case_id.startswith(';') or case_id.startswith('#'):
            continue

        folder = Path(folder_path)
        if not folder.exists():
            logging.warning(Fore.YELLOW + f"Folder {folder_path} does not exist. Skipping.")
            continue

        destination_dir = folder / 'ready_to_ingest'
        destination_dir.mkdir(exist_ok=True)

        try:
            process_directories(folder, destination_dir)
        except Exception as e:
            logging.error(Fore.RED + f"Error processing directories for folder {folder_path}: {e}")

        ready_dirs = destination_dir.iterdir()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for ready_dir in ready_dirs:
                if ready_dir.is_dir():
                    hash_file_path = ready_dir / f"processed_files_{ready_dir.name}.txt"
                    processed_files = load_processed_files(hash_file_path)
                    processed_records = set()

                    for root, dirs, files in os.walk(ready_dir):
                        for file in files:
                            if file.endswith('.json'):
                                json_file_path = Path(root) / file
                                create_index(es, case_id)
                                futures.append(executor.submit(process_json_file, es, json_file_path, case_id, processed_records))

            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Ingesting JSON files"):
                try:
                    future.result()
                except Exception as e:
                    logging.error(Fore.RED + f"Error in processing future: {e}")

def main():
    args = parse_arguments()
    config = load_config(args.config)

    if args.operation == 'ingest':
        ingest(config)
    elif args.operation == 'enrich_download':
        enrich_download()
    elif args.operation == 'enrich_upload':
        enrich_upload()

if __name__ == "__main__":
    main()
