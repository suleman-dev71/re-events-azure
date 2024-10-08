from apify_client import ApifyClient
import json
import time
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from exclusion import excluded_words

client = ApifyClient("apify_api_8tYXptHW0uUKOE1kvn7UPhUqUgo4jG2nupQF")
start_time = time.perf_counter()
website_pattern = re.compile(r'^(https?://)?(?:www\.)?([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,})$')
max_concurrent_runs = 2
json_folder_path = 'results/sorted-url-lists'


def run_actor(file_path):
    
    with open(file_path, 'r') as file:
        data = json.load(file)

        if isinstance(data, list) and all(isinstance(url, str) for url in data):
            urls = data
        else:
            print(f"Unexpected data format in file {file_path}: {type(data).__name__}, Content: {data}")
            raise ValueError(f"Unexpected data format in file {file_path}: Expected a list of URLs")
        

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
        print (f"\nNow Doing: {url}")
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

            # Append extracted information to the list
            total_site_data.append({
                'url': url,
                'content': content
            })

    with open(f"results/websites_content/{website_name}-data.json", 'w') as outfile:
        json.dump(total_site_data, outfile)


def website_crawler():
    json_files = [os.path.join(json_folder_path, filename) for filename in os.listdir(json_folder_path) if filename.endswith('.json')]

    with ThreadPoolExecutor(max_workers=max_concurrent_runs) as executor:
        futures = [executor.submit(run_actor, json_file) for json_file in json_files]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"An error occurred: {e}")

    end_time = time.perf_counter()
    total_time = end_time - start_time
    print (f"\nTotal time taken: {total_time} seconds\n")