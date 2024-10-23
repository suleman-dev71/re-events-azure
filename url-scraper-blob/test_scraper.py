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
excel_blob_name = "re-events-test.xlsx"
input_blob_name = f"{BASE_FOLDER}/scraper_input_urls/"
larger_results_blob_path = f"{BASE_FOLDER}/scraper_results/url-list/larger_website_urls/"
smaller_results_blob_path = f"{BASE_FOLDER}/scraper_results/url-list/smaller_website_urls/"


website_pattern = re.compile(r'^(https?://)?(?:www\.)?([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,})$')
max_concurrent_runs = 100
max_url_limit = 80
smaller_sites = []
start_time = time.perf_counter()
processed_blob_path = f"{BASE_FOLDER}/processed_files/url_scraper_files"


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


def move_single_file_to_processed(blob_name, destination_folder=processed_blob_path):
    try:
        # Create the source blob client for the specific blob name
        source_blob_client = container_client.get_blob_client(blob_name)
        
        # Create the destination blob name (move it to the processed folder)
        destination_blob_name = f"{destination_folder}/{blob_name.split('/')[-1]}"
        destination_blob_client = container_client.get_blob_client(destination_blob_name)
        
        # Copy the source blob to the destination
        copy_source = source_blob_client.url
        logging.info(f"Starting copy from {copy_source} to {destination_blob_name}")
        destination_blob_client.start_copy_from_url(copy_source)
        copy_status = destination_blob_client.get_blob_properties().copy.status
        while copy_status == 'pending':
            copy_status = destination_blob_client.get_blob_properties().copy.status
        if copy_status == 'success':
            source_blob_client.delete_blob()
            logging.info(f"Successfully moved {blob_name} to {destination_blob_name}")
        else:
            logging.error(f"Failed to move {blob_name}. Copy status: {copy_status}")
    
    except Exception as e:
        logging.error(f"Error moving {blob_name}: {e}")


def move_files_to_processed(blob_names, destination_folder=processed_blob_path):
    for blob_name in blob_names:
        try:
            source_blob_client = container_client.get_blob_client(blob_name)
            destination_blob_name = f"{destination_folder}/{blob_name.split('/')[-1]}"
            destination_blob_client = container_client.get_blob_client(destination_blob_name)
            copy_source = source_blob_client.url
            destination_blob_client.start_copy_from_url(copy_source)
            copy_status = destination_blob_client.get_blob_properties().copy.status
            while copy_status == 'pending':
                copy_status = destination_blob_client.get_blob_properties().copy.status
            if copy_status == 'success':
                source_blob_client.delete_blob()
                logging.info(f"Successfully moved {blob_name} to {destination_blob_name}")
            else:
                logging.error(f"Failed to move {blob_name}. Copy status: {copy_status}")
        except Exception as e:
            logging.error(f"Error moving {blob_name}: {e}")


def load_exhibitor_data_from_blob(blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    data = blob_client.download_blob().readall()
    df = pd.read_excel(BytesIO(data))
    required_columns = ['Exhibitor ID', 'Company Name', 'Company Website']

    if all(col in df.columns for col in required_columns):
        exhibitor_data = df[required_columns]
    else:
        missing_cols = [col for col in required_columns if col not in df.columns]
        raise ValueError(f"Missing required columns in the Excel file: {', '.join(missing_cols)}")

    return exhibitor_data

#Read Functions for local files
def load_urls_from_json (file_path):
    with open(file_path, "r") as file:
        data = json.load(file)

    url_list = data["urls"]
    return url_list

def load_urls_from_excel (file_path):
    file_data = pd.read_excel(file_path)
    file_data = file_data.drop(columns=['Company Name', 'Exhibitor ID'])
    urls = file_data['Company Website'].dropna().tolist()
    output = {
        "urls": urls
    }
    return output ["urls"]

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


#URL Extraction Utilities
def get_links_from_url(url, start_url, all_links, new_links):
    try:
        r = requests.get(f"https://{url}", timeout=15)
        soup = BeautifulSoup(r.content, features="html.parser")
        a_tags = soup.find_all("a")
        for tag in a_tags:
            link = tag.get("href")
            if link:
                if any(word in link for word in excluded_words):
                    continue
                if filter_link(link):
                    if link.startswith("/"):
                        link = f"{start_url}{link}"
                    elif not link.startswith(start_url):
                        continue
                    if link not in all_links:
                        if link.startswith("www."):
                            link = link[4:]
                        new_links.append(link)
                        all_links.append(link)
                        logging.info(link)
    except Exception as e:
        logging.info(e)


def get_links_from_url_with_js(url, start_url, all_links, new_links):
    try:
        session = HTMLSession()
        r = session.get(f"https://{url}", timeout=15)
        r.html.render()
        soup = BeautifulSoup(r.content, features="html.parser")
        a_tags = soup.find_all("a")
        for tag in a_tags:
            link = tag.get("href")
            if link:
                if any(word in link for word in excluded_words):
                    continue
                if filter_link(link):
                    if link.startswith("/"):
                        link = f"{start_url}{link}"
                    elif not link.startswith(start_url):
                        continue
                    if link not in all_links:
                        if link.startswith("www."):
                            link = link[4:]
                        new_links.append(link)
                        all_links.append(link)
                        logging.info(link)
    except Exception as e:
        logging.info(e)


def extract_urls_for_site(start_url):
    new_links = []
    all_links = []
    url_start_time = time.perf_counter()
    new_links.append(start_url)
    all_links.append(start_url)
    while len(new_links) > 0:
        url = new_links.pop()
        get_links_from_url(url, start_url, all_links, new_links)
        if len(all_links) > max_url_limit:
            logging.info("Max url limit reached")
            break
    website_name = extract_domain(start_url)
    url_end_time = time.perf_counter()
    total_time = url_end_time - url_start_time
    logging.info(f"{len(all_links)} for {website_name} in {total_time} seconds\n")

    if len(all_links) > 10:
        larger_blob_name = f"{larger_results_blob_path}{website_name}_urls.json"
        upload_data_to_blob(list(set(all_links)), larger_blob_name)
    else:
        smaller_sites.clear()
        smaller_sites.append(start_url)
        smaller_blob_name = f"{smaller_results_blob_path}{website_name}_urls.json"
        upload_data_to_blob(list(set(smaller_sites)), smaller_blob_name)



def test_main_url_extractor():

    start_time = time.perf_counter()
    blob_list = container_client.list_blobs(name_starts_with=input_blob_name)
    existing_file = [blob.name for blob in blob_list if blob.name.endswith('.json')][:1]

    if not existing_file:
        logging.error("No JSON files found in the specified path.")
        return
    blob_name = existing_file[0]
    logging.info(f"Processing the file: {blob_name}")

    blob_client = container_client.get_blob_client(blob_name)
    
    if blob_client.exists():
        url_data = load_urls_from_blob(blob_name)
        print (f"\nNow Doing: {url_data}")    
        if isinstance(url_data, str):
            extract_urls_for_site(url_data)
            move_single_file_to_processed(blob_name)
        else:
            logging.error(f"Unexpected format in {blob_name}, expected a single URL as a string.")

    else:
        logging.error(f"Blob {blob_name} does not exist.")
    end_time = time.perf_counter()
    total_time = end_time - start_time
    logging.info(f"\nTotal time taken: {total_time} seconds\n")