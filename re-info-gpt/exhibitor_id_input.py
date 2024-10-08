import pandas as pd
import json
import re

exhibitor_ids_file_path = 'input_data/exhibitor_ids.json'

def extract_website_name(url):
    website_pattern = re.compile(r'^(https?://)?(?:www\.)?([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,})(/.*)?$')

    match = website_pattern.search(url)
    if match:
        return match.group(2)
    else:
        clean_url = url.replace("https://", "").replace("http://", "").replace("/", "")
        if clean_url.startswith("www."):
            clean_url = clean_url[4:]
        clean_url = clean_url.split('.')[0] if '.' in clean_url else clean_url
        return clean_url

def get_exhibitor_ids():
    file_path = 're_2024.xlsx'
    file_data = pd.read_excel(file_path)
    file_data = file_data.drop(columns=['Company Name'])
    file_data_dict = file_data.to_dict(orient='records')

    with open('input_data/exhibitor_ids.json', 'w') as file:
        json.dump(file_data_dict, file, indent=4)


def exhibitor_id_search(url):
    with open(exhibitor_ids_file_path, 'r') as file:
        exhibitor_list = json.load(file)

    website_name = extract_website_name(url)
    for exhibitor in exhibitor_list:
        if isinstance(exhibitor, dict):
            exhibitor_website_name = extract_website_name(exhibitor.get("Company Website", ""))
            if exhibitor_website_name == website_name:
                return exhibitor["Exhibitor ID"]
    return



print (exhibitor_id_search('https://www.ampion.net/'))