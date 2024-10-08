from apify_client import ApifyClient
import json
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient
from exclusion import excluded_words

client = ApifyClient("apify_api_8tYXptHW0uUKOE1kvn7UPhUqUgo4jG2nupQF")

start_time = time.perf_counter()
website_pattern = re.compile(r'^(https?://)?(?:www\.)?([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,})$')
max_concurrent_runs = 2

container_name = "re-events-v1"
blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=reeventsstorage;AccountKey=rLjyQgvzuhQBoJbT0nxnPHTwoLzDqTsPBnBJVm7tTgAbC2moeaU4wmP6P6J2MFajzC+s8P30bSzx+ASt2YNzVg==;EndpointSuffix=core.windows.net")
container_client = blob_service_client.get_container_client(container_name)

# Blob File paths
input_blob_name = "results/sorted-url-lists/"
output_blob_name = "results/websites_content/"

def read_json_blob(blob_name):
    """Read the JSON file from Azure Blob Storage."""
    blob_client = container_client.get_blob_client(blob_name)
    download_stream = blob_client.download_blob()
    data = json.loads(download_stream.readall())
    return data

def upload_json_blob(blob_name, data):
    """Upload JSON data to Azure Blob Storage."""
    blob_client = container_client.get_blob_client(blob_name)
    json_data = json.dumps(data, indent=4)
    blob_client.upload_blob(json_data, overwrite=True)

def run_actor(file_blob_name):
    data = read_json_blob(file_blob_name)
    
    if isinstance(data, list) and all(isinstance(url, str) for url in data):
        urls = data
    else:
        print(f"Unexpected data format in blob {file_blob_name}: {type(data).__name__}, Content: {data}")
        raise ValueError(f"Unexpected data format in blob {file_blob_name}: Expected a list of URLs")
    
    total_site_data = []
    
    for url in urls:
        run_input = {
            "aggressivePrune": False,
            "clickElementsCssSelector": "[aria-expanded=\"false\"]",
            "clientSideMinChangePercentage": 15,
            "debugLog": False,
            "debugMode": False,
            "expandIframes": True,
            "ignoreCanonicalUrl": False,
            "maxCrawlPages": 1,
            "keepUrlFragments": False,
            "readableTextCharThreshold": 100,
            "removeCookieWarnings": True,
            "removeElementsCssSelector": "nav, footer, script, style, noscript, svg,\n[role=\"alert\"],\n[role=\"banner\"],\n[role=\"dialog\"],\n[role=\"alertdialog\"],\n[role=\"region\"][aria-label*=\"skip\" i],\n[aria-modal=\"true\"]",
            "renderingTypeDetectionPercentage": 10,
            "saveFiles": False,
            "saveHtml": False,
            "saveHtmlAsFile": False,
            "saveMarkdown": True,
            "saveScreenshots": False,
            "startUrls": [
                {
                    "url": f"https://{url}",
                }
            ],
            "crawlerType": "playwright:adaptive",
            "includeUrlGlobs": [],
            "excludeUrlGlobs" : [
                {"glob": "/**/*.{png,jpg,jpeg,pdf}"},
                {"glob": "*{news, News}*"},
                {"glob": "*{blog, Blog}*"},
                {"glob": "*{media, Media}*"},
                {"glob": "*{contact, Contact}*"},
                *[{ "glob": f"*{word, {word.capitalize()}}*"} for word in excluded_words]
            ],
            "initialCookies": [],
            "proxyConfiguration": { "useApifyProxy": True },
            "useSitemaps": False
        }

        print(f"\nNow Doing: {url}")
        run = client.actor("apify/website-content-crawler").call(run_input=run_input)

        match = website_pattern.search(url)

        if match:
            website_name = match.group(2)
        else:
            clean_url = url.replace("https://", "").replace("http://", "").replace("/", "")
            if clean_url.startswith("www."):
                clean_url = clean_url[4:]
            clean_url = clean_url.split('.')[0] if '.' in clean_url else clean_url

            website_name = clean_url

        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            url = item.get('url', 'URL not found')
            content = item.get('text', 'Content not available')

            total_site_data.append({
                'url': url,
                'content': content
            })
    output_blob_name = f"output/{website_name}-data.json"
    upload_json_blob(output_blob_name, total_site_data)
    return website_name


def website_crawler():

    blob_list = container_client.list_blobs(name_starts_with=input_blob_name)
    existing_files = [blob.name for blob in blob_list if blob.name.endswith('.json')]
    
    with ThreadPoolExecutor(max_workers=max_concurrent_runs) as executor:
        futures = [executor.submit(run_actor, file) for file in existing_files]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"An error occurred: {e}")


    end_time = time.perf_counter()
    total_time = end_time - start_time
    print(f"\nTotal time taken: {total_time} seconds\n")