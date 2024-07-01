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
    parser = argparse.ArgumentParser(description='Process ZIP files and upload JSON data to Elasticsearch.')
    parser.add_argument('--config', required=True, help='Path to the configuration file.')
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
    actions = []
    for doc in json_data_list:
        actions.append({
            "index": {
                "_index": index_name,
                "_id": doc.get('UUID')
            }
        })
        actions.append(doc)
    
    try:
        if actions:
            es.bulk(body=actions, refresh=True, timeout='60s')  # Increase timeout for bulk operation
            logging.info(f"Indexed {len(json_data_list)} documents to {index_name}")
    except exceptions.RequestError as e:
        logging.error(Fore.RED + f"RequestError indexing documents in {index_name}: {e.info}")
    except exceptions.ConnectionTimeout as e:
        logging.error(Fore.RED + f"ConnectionTimeout indexing documents in {index_name}: {e.info}")
    except Exception as e:
        logging.error(Fore.RED + f"Error indexing documents in {index_name}: {e}")

# Calculate SHA-1 checksum of a file
def calculate_checksum(file_path):
    hash_sha1 = hashlib.sha1()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha1.update(chunk)
    return hash_sha1.hexdigest()

# Load processed file hashes from a file
def load_processed_files(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            return set(file.read().splitlines())
    return set()

# Save a processed file hash to a file
def save_processed_file(file_path, file_hash):
    with open(file_path, 'a') as file:
        file.write(file_hash + '\n')

# Process a JSON file and upload its data to Elasticsearch
def process_json_file(es, json_file_path, index_name, processed_records):
    if os.path.getsize(json_file_path) == 0:
        logging.warning(Fore.YELLOW + f"Skipping empty file: {json_file_path}")
        return

    sourcetype = json_file_path.stem

    documents = []
    with open(json_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                json_data = json.loads(line)
                json_data['sourcetype'] = sourcetype
                
                # Convert System.Keywords to string if it exists
                if 'System' in json_data and 'Keywords' in json_data['System']:
                    json_data['System']['Keywords'] = str(json_data['System']['Keywords'])
                
                # Use the UUID field as the document ID
                doc_id = json_data.get('UUID')
                if doc_id and doc_id not in processed_records:
                    documents.append(json_data)
                    processed_records.add(doc_id)
                elif not doc_id:
                    logging.warning(Fore.YELLOW + f"Document missing UUID in file {json_file_path}, skipping.")
            except json.JSONDecodeError as e:
                logging.error(Fore.RED + f"Error decoding JSON in file {json_file_path}, line: {line[:50]}...: {e}")
                continue

    if documents:
        bulk_upload_json_to_elasticsearch(es, index_name, documents)

# Process a ZIP file and extract JSON data
def process_zip_file(es, zip_file_path, case_id, hash_file_path, processed_files, processed_records):
    try:
        file_checksum = calculate_checksum(zip_file_path)
    
        if file_checksum in processed_files:
            logging.info(Fore.YELLOW + f"Skipping duplicate file: {zip_file_path}")
            return
        
        processed_files.add(file_checksum)

        if not zipfile.is_zipfile(zip_file_path):
            logging.info(Fore.YELLOW + f"Skipping non-ZIP file: {zip_file_path}")
            return

        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            temp_dir = Path(zip_file_path).with_suffix('')
            zip_ref.extractall(temp_dir)
            
            json_dir = temp_dir             
            if not json_dir.exists():
                logging.info(Fore.RED + f"Directory {json_dir} not found in {zip_file_path}. Skipping.")
                return
            
            index_name = case_id.lower()
            create_index(es, index_name)

            for json_file in json_dir.glob('*.json'):
                process_json_file(es, json_file, index_name, processed_records)

        save_processed_file(hash_file_path, file_checksum)
    except Exception as e:
        logging.error(Fore.RED + f"Error processing file {zip_file_path}: {e}")

def main():
    args = parse_arguments()
    config = load_config(args.config)
    
    es = configure_es(config)
    case_dirs = config.items('cases')
    
    for case_id, folder_path in case_dirs:
        if case_id.startswith(';') or case_id.startswith('#'):
            continue

        hash_file_path = f"{folder_path}/processed_files_{case_id}.txt"
        processed_files = load_processed_files(hash_file_path)
        processed_records = set()

        folder = Path(folder_path)
        
        for subdir in folder.iterdir():
            if subdir.is_dir() and subdir.name.lower() != 'thoroutput':
                zip_files = subdir.glob('*.zip')
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = [
                        executor.submit(process_zip_file, es, zip_file, case_id, hash_file_path, processed_files, processed_records)
                        for zip_file in zip_files
                    ]
                    
                    for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc=f"Processing {subdir.name}"):
                        future.result()

if __name__ == "__main__":
    main()

