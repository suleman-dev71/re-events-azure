import os
from dotenv import load_dotenv
from openai import OpenAI
import json
import time
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
AZURE_STORAGE_KEY = os.getenv('AZURE_STORAGE_KEY')


container_name = "re-events-v1"
blob_service_client = BlobServiceClient.from_connection_string(f"DefaultEndpointsProtocol=https;AccountName=reeventsstorage;AccountKey={AZURE_STORAGE_KEY};EndpointSuffix=core.windows.net")
container_client = blob_service_client.get_container_client(container_name)

load_dotenv()
results = {}
urls_folder_path = "results/url-list/larger_website_urls/"
results_folder_path = "results/sorted-url-lists/"

urls_blob_folder = "url-classifier-results/url-list/larger_website_urls/"
output_blob_folder = "url-classifier-results/sorted-url-lists/"

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
    print(f"Uploaded data to Blob Storage at {blob_name}")


def openai_call(data, resp_format="json"):

    if isinstance(data, list) and all(isinstance(url, str) for url in data):
        urls = data
    print (urls)
    index = int(len(urls)*0.8)
    clean_urls = urls[0:index]

    prompt = (
"""Please classify the following URLs according to the rules below:

1. Only if the number of URLs is more than 40, extract and list only the URLs that fit into these categories:
    - URLs related to Products or Services for Renewable Energy
    - URLs related to Company Overview or About Us pages

2. If the number of URLs is less than 40, return only the first 39 URLs.

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

def classify_urls():
    """Main function to classify URLs from blobs and save results locally."""
    os.makedirs(results_folder_path, exist_ok=True)
    # List all blobs in the specified folder path
    blob_list = container_client.list_blobs(name_starts_with=urls_blob_folder)
  
    existing_files = [blob.name for blob in blob_list if blob.name.endswith('.json')]
 
    for blob_name in existing_files:
        print(f"Processing blob: {blob_name}")

        data = download_blob_to_string(blob_name)
        result = openai_call(data)
        result = json.loads(result)
        result_filename = blob_name.replace('.json', '-sorted.json').replace(urls_blob_folder, '')
        result_file_path = os.path.join(results_folder_path, result_filename)
        

        result_filename = blob_name.replace('.json', '-sorted.json').replace(urls_blob_folder, '')
        blob_file_path = os.path.join(results_folder_path, result_filename).replace("\\", "/")
        upload_data_to_blob(result, blob_file_path)
        print(f"Processed {blob_name} into {result_file_path}")

    end_time = time.perf_counter()
    total_time = end_time - start_time
    print(f"\nTotal time taken: {total_time} seconds\n")
    return result
