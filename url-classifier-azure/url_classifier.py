import os
from dotenv import load_dotenv
from openai import OpenAI
import json
import time
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
import logging
from const import BASE_FOLDER, CONTAINER_NAME
import math
import re

load_dotenv()
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)
website_pattern = re.compile(r'^(https?://)?(?:www\.)?([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,})$')
results = {}

urls_blob_folder = f"{BASE_FOLDER}/scraper_results/url-list/larger_website_urls/"
output_blob_folder = f"{BASE_FOLDER}/url-classifier-results/sorted-url-lists/"

client = OpenAI()
OpenAI.api_key = os.getenv("OPENAI_API_KEY")
start_time = time.perf_counter()


def download_blob_to_string(blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    data = blob_client.download_blob().readall()
    json_data = json.loads(data)
    return json_data


def upload_data_to_blob(data, blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(json.dumps(data), overwrite=True)
    logging.info(f"Uploaded data to Blob Storage at {blob_name}")

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


def ensure_list(input_data):
    """
    Takes either a string or a list as input.
    If the input is a string that looks like a JSON list, it is parsed as a list.
    If the input is a regular string, it is split into a list of words.
    If the input is already a list, it is returned unchanged.
    """
    if isinstance(input_data, str):
        input_data = input_data.strip()  # Remove leading/trailing whitespace
        # If it looks like a JSON array, try to parse it
        if input_data.startswith('[') and input_data.endswith(']'):
            try:
                return json.loads(input_data)  # Parse the string as JSON
            except json.JSONDecodeError:
                print(f"Warning: Failed to parse JSON string. Treating as plain text: {input_data}")
                return input_data.split()  # Fallback to splitting the string
        else:
            # Split the regular string into a list by whitespace
            return input_data.split()
    
    elif isinstance(input_data, list):
        return input_data
    
    else:
        raise ValueError("Input must be either a string or a list")


def openai_call(data, resp_format="json"):
    
    if isinstance(data, list) and all(isinstance(url, str) for url in data):
        urls = data
        if (len(urls) >=40):
            index = int(len(urls)*0.8)
            clean_urls = urls[0:index]

            prompt = (
        """Please classify the following URLs according to the rules below:

        1. Extract and list only the URLs that fit into these categories:
            - URLs related to Products or Services for Renewable Energy
            - URLs related to Company Overview or About Us pages

        2. Make sure to return less than 40 URLs

        3. Do not return any text other than the list of URLs.

        4. Remove 'https://' from each URL before including it in the list.

        5. Ensure the total number of classified URLs does not exceed 40.

        ####EXAMPLE OUTPUT####
        ["www.solar123.com", "ABCmachines.com", "www.power-xyz.com"]

        Here are the URLs to classify:
        """
        )
            prompt += "\n".join(clean_urls)

            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert at extracting URLs that contain Products and Services from a given set of URLs.",
                    },
                    {
                        "role": "user",
                        "content": prompt,
                    },
                ],
                temperature=0.1,
                n=1,
                stop=None
            )
            
            return response.choices[0].message.content

        else:
            return urls


def classify_urls():
    """Main function to classify URLs from blobs and save results to Blob Storage."""
    # List all blobs in the specified folder path
    blob_list = container_client.list_blobs(name_starts_with=urls_blob_folder)
    existing_files = [blob.name for blob in blob_list if blob.name.endswith('.json')]
    batch_size = 5
    for blob_name in existing_files:
        try:
            data = download_blob_to_string(blob_name)
            
            result = openai_call(data)
            result = ensure_list (result)
            total_batches = math.ceil(len(result) / batch_size)

            for i in range(total_batches):
                batch = result[i * batch_size: (i + 1) * batch_size]        
                result_filename = f"{blob_name.split('/')[-1].replace('.json', '')}_sorted_urls_batch{i + 1}.json"
                blob_file_path = os.path.join(output_blob_folder, result_filename).replace("\\", "/")
                upload_data_to_blob(batch, blob_file_path)
                print(f"Processed batch {i + 1} of {blob_name} into {blob_file_path}")

        except Exception as e:
            print(f"Error processing {blob_name}: {e}")
            continue
        
    end_time = time.perf_counter()
    total_time = end_time - start_time
    print(f"\nTotal time taken: {total_time} seconds\n")