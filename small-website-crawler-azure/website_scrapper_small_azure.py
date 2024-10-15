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
max_concurrent_runs = 3

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

input_blob_path = f"{BASE_FOLDER}/scraper_results/url-list/smaller_website_urls/smaller_sites_urls.json"
output_blob_path = f"{BASE_FOLDER}/website_content/"

logging.basicConfig(level=logging.INFO)

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

# Function to run Apify actor and store results in Azure Blob Storage
def run_actor(url, timeout = 80):
    total_site_data = []
    url = ensure_https(url)

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
        "startUrls": [{"url": url}],
        "useSitemaps": False
    }

    logging.info(f"Now Crawling: {url}")
    try:
        # Set the timeout in the call to the actor
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
    url_list = load_urls_from_blob(input_blob_path)
    with ThreadPoolExecutor(max_workers=max_concurrent_runs) as executor:
        futures = [executor.submit(run_actor, url) for url in url_list]
        for future in as_completed(futures):
            try:
                future.result()
            except TimeoutError:
                logging.error(f"Timeout: A request took longer than time limit")
            except Exception as e:
                logging.error(f"An error occurred: {e}")

    end_time = time.perf_counter()
    total_time = end_time - start_time
    logging.info(f"Total time taken: {total_time} seconds")