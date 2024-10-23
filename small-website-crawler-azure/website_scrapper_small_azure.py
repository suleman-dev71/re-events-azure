import os
import json
import time
import re
import logging
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from apify_client import ApifyClient
from azure.storage.blob import BlobServiceClient
from exclusion import excluded_words
import os
from dotenv import load_dotenv
from concurrent.futures import TimeoutError
from const import BASE_FOLDER, CONTAINER_NAME

load_dotenv()
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
APIFY_CLIENT_API = os.getenv ('APIFY_CLIENT_API')

client = ApifyClient(APIFY_CLIENT_API)
start_time = time.perf_counter()
website_pattern = re.compile(r'^(https?://)?(?:www\.)?([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,})$')
max_concurrent_runs = 1

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

input_blob_path = f"{BASE_FOLDER}/scraper_results/url-list/smaller_website_urls/"
output_blob_path = f"{BASE_FOLDER}/website_content/"
processed_blob_path = f"{BASE_FOLDER}/processed_files/smaller_sites"
logging.basicConfig(level=logging.INFO)


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


def move_single_file_to_processed(blob_name, destination_folder=processed_blob_path):
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


# Read URL list from Azure Blob Storage
def load_urls_from_blob(blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    data = blob_client.download_blob().readall()
    decoded_data = data.decode('utf-8')
    json_data = json.loads(decoded_data)
    logging.info(f"\nLoaded {len(json_data)} URLs.")
    return json_data

# Upload results to Azure Blob Storage
def upload_data_to_blob(blob_name, data):
    try:
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(json.dumps(data), overwrite=True)
        logging.info(f"Uploaded data to Blob Storage at {blob_name}")
    except Exception as e:
        logging.error(f"Failed to upload data to {blob_name}: {e}")

# Ensure URL is well-formed
def ensure_https(url):
    if not url.startswith('http://') and not url.startswith('https://'):
        return f"https://{url}"
    return url



# Extract hostname
def extract_website_name(url):
    parsed_url = urlparse(url)
    hostname = parsed_url.hostname or url
    return hostname.split('.')[0] if hostname.startswith('www.') else hostname

def read_json_blob(blob_name):
    """Read the first five entries from the JSON file in Azure Blob Storage."""
    blob_client = container_client.get_blob_client(blob_name)
    download_stream = blob_client.download_blob()
    data = json.loads(download_stream.readall())
    return data


def run_actor(file_blob_name, timeout=100):
    data = read_json_blob(file_blob_name)
    
    if isinstance(data, list) and all(isinstance(url, str) for url in data):
        urls = data
    else:
        logging.info(f"Unexpected data format in blob {file_blob_name}: {type(data).__name__}, Content: {data}")
        raise ValueError(f"Unexpected data format in blob {file_blob_name}: Expected a list of URLs")
    
    total_site_data = []
    
    for url in urls:
        run_input = {
            "aggressivePrune": False,
            "clickElementsCssSelector": "[aria-expanded=\"False\"]",
            "clientSideMinChangePercentage": 15,
            "crawlerType": "playwright:adaptive",
            "debugLog": False,
            "debugMode": False,
            "excludeUrlGlobs": [
                {"glob": "/**/*.{png,jpg,jpeg,pdf}"},
                {"glob": "*{news, News}*"},
                {"glob": "*{blog, Blog}*"},
                {"glob": "*{media, Media}*"},
                {"glob": "*{contact, Contact}*"},
                *[{ "glob": f"*{word, {word.capitalize()}}*"} for word in excluded_words]
            ], 
            "expandIframes": True,
            "ignoreCanonicalUrl": False,
            "keepUrlFragments": False,
            "maxCrawlPages": 30,
            "proxyConfiguration": {
                "useApifyProxy": True,
                "apifyProxyGroups": []
            },
            "readableTextCharThreshold": 100,
            "removeCookieWarnings": True,
            "removeElementsCssSelector": "nav, footer, script, style, noscript, svg,\n[role=\"alert\"],\n[role=\"banner\"],\n[role=\"dialog\"],\n[role=\"alertdialog\"],\n[role=\"region\"][aria-label*=\"skip\" i],\n[aria-modal=\"True\"]",
            "renderingTypeDetectionPercentage": 10,
            "saveFiles": False,
            "saveHtml": False,
            "saveHtmlAsFile": False,
            "saveMarkdown": True,
            "saveScreenshots": False,
            "startUrls": [ {
                    "url": f"https://{url}",
                }],
            "useSitemaps": False
        }

    logging.info(f"Now Crawling: {url}")
    try:
        run = client.actor("apify/website-content-crawler").call(run_input=run_input, timeout_secs=timeout)
    except Exception as e:
        logging.error(f"Error crawling {url}: {e}")
        return

    website_name = extract_website_name(url)

    try:
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            page_url = item.get('url', 'URL not found')
            content = item.get('text', 'Content not available')
            total_site_data.append({
                'url': page_url,
                'content': content
            })
    except Exception as e:
        logging.error(f"Error fetching content for {url}: {e}")
    
    # Upload results to Azure Blob Storage
    output_blob_name = f"{output_blob_path}{website_name}_data.json"
    upload_data_to_blob(output_blob_name, total_site_data)



def small_site_crawler():
    start_time = time.perf_counter()
    blob_list = container_client.list_blobs(name_starts_with=input_blob_path)
    existing_files = [blob.name for blob in blob_list if blob.name.endswith('.json')][:1]

    if not existing_files:
        logging.error("No JSON files found in the specified path.")
        return
    blob_name = existing_files[0]
    logging.info(f"Processing the file: {blob_name}")

    try:
        run_actor(blob_name)
        move_single_file_to_processed(blob_name)

    except Exception as e:
        logging.error(f"An error occurred while processing {blob_name}: {e}")
    end_time = time.perf_counter()
    total_time = end_time - start_time
    logging.info(f"\nTotal time taken: {total_time} seconds\n")