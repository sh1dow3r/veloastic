import os
import zipfile
import json
import hashlib
from pathlib import Path
from elasticsearch import Elasticsearch, exceptions, RequestsHttpConnection
from tqdm import tqdm
import argparse
import configparser
from colorama import init, Fore
import concurrent.futures
import logging

# Initialize colorama and logging
init(autoreset=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration from an .ini file
def load_config(config_path):
    config = configparser.ConfigParser(interpolation=None)  # Disable interpolation
    config.read(config_path)
    return config

# Parse command-line arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description='Process ZIP files and perform operations on JSON data.')
    parser.add_argument('--config', default='config.ini', help='Path to the configuration file. Default is "config.ini".')
    parser.add_argument('-o', '--operation', choices=['ingest', 'enrich_download', 'enrich_upload'], default='ingest', help='Operation to perform: ingest (default), enrich_download, enrich_upload.')
    return parser.parse_args()

# Configure Elasticsearch client
def configure_es(config):
    return Elasticsearch(
        hosts=[{"host": config.get('elasticsearch', 'host'), "port": config.getint('elasticsearch', 'port')}],
        http_auth=(config.get('elasticsearch', 'username'), config.get('elasticsearch', 'password')),
        scheme=config.get('elasticsearch', 'scheme'),
        timeout=60,  # Increase timeout to 60 seconds
        max_retries=3,  # Set the maximum number of retries
        retry_on_timeout=True,  # Enable retry on timeout
        connection_class=RequestsHttpConnection  # Use RequestsHttpConnection for more control
    )

# Create an index in Elasticsearch
def create_index(es, index_name):
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
        logging.info(f"Index {index_name} created.")
    else:
        logging.info(f"Index {index_name} already exists.")

# Upload JSON data to Elasticsearch in bulk
def bulk_upload_json_to_elasticsearch(es, index_name, json_data_list):
    actions = [
        {
            "_index": index_name,
            "_source": json_data,
        }
        for json_data in json_data_list
    ]
    try:
        from elasticsearch.helpers import bulk
        bulk(es, actions)
    except exceptions.BulkIndexError as e:
        logging.error(Fore.RED + f"Bulk indexing error: {e}")

# Process a JSON file
def process_json_file(es, json_file_path, index_name, processed_records):
    try:
        with open(json_file_path, 'r') as json_file:
            json_data_list = []
            for line in json_file:
                json_data = json.loads(line)
                json_data_list.append(json_data)
            bulk_upload_json_to_elasticsearch(es, index_name, json_data_list)
            logging.info(Fore.GREEN + f"Successfully processed {json_file_path}")
    except json.JSONDecodeError as e:
        logging.error(Fore.RED + f"Error decoding JSON from file {json_file_path}: {e}")
    except Exception as e:
        logging.error(Fore.RED + f"Error processing file {json_file_path}: {e}")

# Save processed file's checksum to avoid reprocessing
def save_processed_file(hash_file_path, file_checksum):
    with open(hash_file_path, 'a') as hash_file:
        hash_file.write(file_checksum + '\n')

# Load processed files to avoid reprocessing
def load_processed_files(hash_file_path):
    if os.path.exists(hash_file_path):
        with open(hash_file_path, 'r') as hash_file:
            return set(line.strip() for line in hash_file)
    return set()

# Calculate the checksum of a file
def calculate_file_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, 'rb') as file:
        for byte_block in iter(lambda: file.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

# Extract and rename ZIP files
def extract_and_rename_zip(zip_file_path, destination_dir):
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            temp_dir = Path(destination_dir) / zip_file_path.stem
            temp_dir.mkdir(parents=True, exist_ok=True)
            zip_ref.extractall(temp_dir)

            for item in temp_dir.iterdir():
                if item.is_dir():
                    new_dir_name = temp_dir
                    counter = 1
                    while new_dir_name.exists():
                        new_dir_name = temp_dir.parent / f"{temp_dir.stem}_{counter}"
                        counter += 1
                    item.rename(new_dir_name)
                    logging.info(Fore.GREEN + f"Extracted and renamed to {new_dir_name}")
                    break

    except Exception as e:
        logging.error(Fore.RED + f"Error processing file {zip_file_path}: {e}")

# Process directories to find and extract ZIP files
def process_directories(folder, destination_dir):
    for path in folder.iterdir():
        if path.is_dir():
            process_directories(path, destination_dir)
        elif path.is_file() and path.suffix == '.zip':
            extract_and_rename_zip(path, destination_dir)

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

        process_directories(folder, destination_dir)

        ready_dirs = destination_dir.iterdir()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for ready_dir in ready_dirs:
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
                future.result()

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
