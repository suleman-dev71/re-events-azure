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
max_concurrent_runs = 3
input_file_path = 'results/url-list/smaller_website_urls/smaller_sites_urls.json'
with open(input_file_path, 'r') as file:
    data = json.load(file)

url_list = data
total_number_of_websites = len(url_list)

def run_actor(url):
    
    total_site_data = []
    
    run_input = {
    "aggressivePrune": False,
    "clickElementsCssSelector": "[aria-expanded=\"False\"]",
    "clientSideMinChangePercentage": 15,
    "crawlerType": "playwright:adaptive",
    "debugLog": False,
    "debugMode": False,
    "excludeUrlGlobs" : [
    {"glob": "/**/*.{png,jpg,jpeg,pdf}"},
    {"glob": "*{news, News}*"},
    {"glob": "*{blog, Blog}*"},
    {"glob": "*{media, Media}*"},
    {"glob": "*{contact, Contact}*"},
    *[{ "glob": f"*{word, {word.capitalize()}}*"} for word in excluded_words]], 
    "expandIframes": True,
    "ignoreCanonicalUrl": False,
    "keepUrlFragments": False,
    "maxCrawlPages": 40,
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
    "startUrls": [
        {
            "url": f"https://{url}"
        }
    ],
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
        # total_site_data.append (item)

        total_site_data.append({
            'url': url,
            'content': content
        })
    with open(f"results/websites_content/{website_name}_data.json", 'w') as outfile:
        json.dump(total_site_data, outfile)



def small_site_crawler():
    with ThreadPoolExecutor(max_workers=max_concurrent_runs) as executor:
        futures = [executor.submit(run_actor, url) for url in url_list]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"An error occurred at {e}")


    end_time = time.perf_counter()
    total_time = end_time - start_time
    print (f"\nTotal time taken: {total_time} seconds\n")