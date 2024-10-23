import requests
from bs4 import BeautifulSoup
import re
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests_html import HTMLSession
from scraper_utils import filter_link
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
import logging
import os
import pandas as pd
from io import BytesIO
from const import BASE_FOLDER, CONTAINER_NAME
from exclusion import excluded_words

load_dotenv()
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Blob File paths
json_blob_name = "start-urls.json"
output_blob_name = f"{BASE_FOLDER}/scraper_input_urls/"
excel_blob_name = "re-events-test.xlsx"


website_pattern = re.compile(r'^(https?://)?(?:www\.)?([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,})$')
max_concurrent_runs = 100
max_url_limit = 80
smaller_sites = []
start_time = time.perf_counter()


#Read/Write Functions for Blob
def upload_data_to_blob(data, blob_name):
    blob_client = container_client.get_blob_client(f"{blob_name}")
    blob_client.upload_blob(json.dumps(data), overwrite=True)
    logging.info(f"Uploaded data to Blob Storage at {blob_name}")

def load_urls_from_blob(blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    data = blob_client.download_blob().readall()
    decoded_data = data.decode('utf-8')
    json_data = json.loads(decoded_data)
    return json_data


#Name/Domain Correction
def extract_domain (url):
    match = website_pattern.search(url)
        
    if match:
        website_name = match.group(2)
    else:
        clean_url = url.replace("https://", "").replace("http://", "").replace("/", "")          
        if clean_url.startswith("www."):
            clean_url = clean_url[4:]
        clean_url = clean_url.split('.')[0] if '.' in clean_url else clean_url

        website_name = clean_url
    return website_name

def file_maker():
    url_list = load_urls_from_blob(json_blob_name)
    url_filename_list = [[extract_domain(url) + "_urls.json", url] for url in url_list]
    blob_list = container_client.list_blobs()
    existing_files = [blob.name for blob in blob_list]
    for url in url_filename_list:
        url[1]  = extract_domain(url[1])
        upload_data_to_blob (url[1], f"{output_blob_name}{url[0]}")
    
    end_time = time.perf_counter()
    total_time = end_time - start_time
    logging.info(f"\nTotal time taken: {total_time} seconds\n")