import requests
from bs4 import BeautifulSoup
import re
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests_html import HTMLSession
import pandas as pd
from excluded_words import excluded_words
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
import logging

load_dotenv()
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_KEY')


container_name = "re-events-v1"
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(container_name)

# File paths
json_blob_name = "start-urls.json"
excel_blob_name = 're_2024.xlsx'
larger_results_blob_path = "results/url-list/larger_website_urls/"
smaller_results_blob_path = "results/url-list/smaller_website_urls/"
website_pattern = re.compile(r'^(https?://)?(?:www\.)?([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,})$')
max_concurrent_runs = 10
max_url_limit = 1000
smaller_sites = []
start_time = time.perf_counter()


def contains_excluded_words(string):
    return any(word in string for word in excluded_words)

def filter_link(link):
    if (not link.endswith(("png", "jpg", "jpeg", "pdf", "mp4", "mp3"))) and (
        not contains_excluded_words(link.lower())
    ):
        return True

# Function to read JSON from Blob Storage
def load_urls_from_blob(blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    data = blob_client.download_blob().readall()
    decoded_data = data.decode('utf-8')
    json_data = json.loads(decoded_data)
    print(f"\n\nthe data type is: {type(json_data)}\n\n and the data is: {json_data}")
    return json_data['urls']
    

# Function to read Excel from Blob Storage
def load_urls_from_excel_blob(blob_name):
    local_excel_path = "/tmp/re_2024.xlsx"
    blob_client = container_client.get_blob_client(blob_name)
    with open(local_excel_path, "wb") as file:
        file.write(blob_client.download_blob().readall())
    file_data = pd.read_excel(local_excel_path)
    file_data = file_data.drop(columns=['Company Name', 'Exhibitor ID'])
    urls = file_data['Company Website'].dropna().tolist()
    return urls

# Function to upload data to Blob Storage
def upload_data_to_blob(data, blob_name):
    blob_client = container_client.get_blob_client(f"url-classifier-{blob_name}")
    blob_client.upload_blob(json.dumps(data), overwrite=True)
    logging.info(f"Uploaded data to Blob Storage at {blob_name}")

# Extract domain name
def extract_domain(url):
    match = website_pattern.search(url)

    if match:
        return match.group(2)
    else:
        clean_url = url.replace("https://", "").replace("http://", "").replace("/", "")
        if clean_url.startswith("www."):
            clean_url = clean_url[4:]
        clean_url = clean_url.split('.')[0] if '.' in clean_url else clean_url
        return clean_url

# Get links from URL (without JS)
def get_links_from_url(url, start_url, all_links, new_links):
    try:
        r = requests.get(f"https://{url}", timeout=20)
        soup = BeautifulSoup(r.content, features="html.parser")
        a_tags = soup.find_all("a")
        for tag in a_tags:
            link = tag.get("href")
            if link:
                if filter_link(link):
                    if link.startswith("/"):
                        link = f"{start_url}{link}"
                    elif not link.startswith(start_url):
                        continue
                    if link not in all_links:
                        new_links.append(link)
                        all_links.append(link)
                        logging.info(link)
    except Exception as e:
        logging.info(e)

# Extract URLs for a website and save results to Blob Storage
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
            logging.info("Max URL limit reached")
            break

    website_name = extract_domain(start_url)
    url_end_time = time.perf_counter()
    total_time = url_end_time - url_start_time
    logging.info(f"{len(all_links)} for {website_name} in {total_time} seconds\n")

    if len(all_links) > 10:
        larger_blob_name = f"{larger_results_blob_path}{website_name}_urls.json"
        upload_data_to_blob(all_links, larger_blob_name)
    else:
        smaller_sites.append(all_links[0])
        smaller_blob_name = f"{smaller_results_blob_path}smaller_sites_urls.json"
        upload_data_to_blob(smaller_sites, smaller_blob_name)

# Main function to extract URLs
def main_url_extractor():
    url_list = load_urls_from_blob(json_blob_name)
    print (url_list)

    url_filename_list = [[extract_domain(url) + "_urls.json", url] for url in url_list]
    print (url_filename_list)
    # List blobs in the container to check existing results
    blob_list = container_client.list_blobs()
    existing_files = [blob.name for blob in blob_list]
    print (existing_files)

    for name in url_filename_list:
        if name[0] in existing_files:
            url_list.remove(name[1])

    with ThreadPoolExecutor(max_workers=max_concurrent_runs) as executor:
        futures = [executor.submit(extract_urls_for_site, url) for url in url_list]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.info(f"An error occurred: {e}")

    end_time = time.perf_counter()
    total_time = end_time - start_time
    logging.info(f"\nTotal time taken: {total_time} seconds\n")
