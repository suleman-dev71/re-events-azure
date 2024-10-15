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

load_dotenv()
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# review: like I said, move all constants to separate file
# Blob File paths
json_blob_name = "start-urls.json"
excel_blob_name = "re-events-test.xlsx"
larger_results_blob_path = f"{BASE_FOLDER}/scraper_results/url-list/larger_website_urls/"
smaller_results_blob_path = f"{BASE_FOLDER}/scraper_results/url-list/smaller_website_urls/"


website_pattern = re.compile(r'^(https?://)?(?:www\.)?([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,})$')
max_concurrent_runs = 50
max_url_limit = 1000
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


def get_links_from_url_with_js(url, start_url, all_links, new_links):
    try:
        session = HTMLSession()
        r = session.get(f"https://{url}", timeout=20)
        r.html.render()
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


#review: the following method is unnecessarily complex
def extract_urls_for_site(start_url):
    #review why not `new_links = [start_url]` instead?
    new_links = []
    all_links = []
    url_start_time = time.perf_counter()
    new_links.append(start_url)
    all_links.append(start_url)
    #review why not a for loop here? Also, we should add a duplicate url check
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
        smaller_sites.append(all_links[0])
        smaller_blob_name = f"{smaller_results_blob_path}smaller_sites_urls.json"
        upload_data_to_blob(list(set(smaller_sites)), smaller_blob_name)
    

def test_main_url_extractor():
    url_list = load_urls_from_blob(json_blob_name)
    url_filename_list = [[extract_domain(url) + "_urls.json", url] for url in url_list]
    blob_list = container_client.list_blobs()
    existing_files = [blob.name for blob in blob_list]
  
    for name in url_filename_list:
        if name[0] in existing_files:
            url_list.remove(name[1])

    with ThreadPoolExecutor(max_workers=max_concurrent_runs) as executor:
        futures = [executor.submit(extract_urls_for_site, url) for url in url_list]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                #review: do logging.error for errors
                logging.info(f"An error occurred: {e}")

    end_time = time.perf_counter()
    total_time = end_time - start_time
    logging.info(f"\nTotal time taken: {total_time} seconds\n")