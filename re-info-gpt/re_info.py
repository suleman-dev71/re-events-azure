import json
import os
from dotenv import load_dotenv
from openai import OpenAI
import openai
from prompts import prod_match_prompt, info_prompt, johns_prompt
from exhibitor_id_input import exhibitor_id_search
import time
import re
from azure.storage.blob import BlobServiceClient
import sys
import signal

import os
AZURE_STORAGE_KEY = os.getenv('AZURE_STORAGE_KEY')

load_dotenv()

# Initialize OpenAI Client
client = OpenAI()


# Initialize BlobServiceClient
container_name = "re-events-v1"
blob_service_client = BlobServiceClient.from_connection_string(f"DefaultEndpointsProtocol=https;AccountName=reeventsstorage;AccountKey={AZURE_STORAGE_KEY};EndpointSuffix=core.windows.net")
container_client = blob_service_client.get_container_client(container_name)

manual_review = {"review_manually": []}
result_path = "GPT-results/"
input_path = "output/"
ratelimit_delay = 65
retries = 0
start_time = time.perf_counter()
processed_files = set()

# Download blob content from Azure Blob Storage
def download_blob(blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    downloaded_blob = blob_client.download_blob().readall()
    return json.loads(downloaded_blob.decode("utf-8"))

# Upload content to Azure Blob Storage
def upload_blob(blob_name, data):
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(json.dumps(data), overwrite=True)

# Function to make OpenAI call
def opnenai_call(prompt, resp_format="json_object"):
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert at summarising information about a company given content from their website",
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
            temperature=0.2,
            response_format={"type": resp_format},
        )
        return response.choices[0].message.content
    except openai.RateLimitError as e:
        print(f"Rate limit exceeded: {e}")
        time.sleep(ratelimit_delay)
        return opnenai_call(prompt)
    except openai.OpenAIError as e:
        print(f"An OpenAI error occurred: {e}")
        raise e

def main_gpt_script():
    results = {}
    max_retries = 3  # Maximum number of retries

    def extract_file_info(blob_name):
        try:
            data = download_blob(blob_name)
        except Exception as e:
            print(f"Failed to download or parse blob {blob_name}: {e}")
            return "error"

        token_limit = 120000
        alltext = ""

        if isinstance(data, list) and data and 'url' in data[0]:
            url = data[0]["url"]
            for page in data:
                alltext += page.get("url", "")
                alltext += page.get("text", "")

        elif isinstance(data, dict) and 'websites' in data:
            urls = data.get('websites', [])
            for url in urls:
                alltext += url

        tokens = len(alltext) / 4
        if tokens > token_limit:
            print(f"tokens more than {token_limit}")
            manual_review["review_manually"].append(blob_name)
            return "error"

        prompt_1_response = opnenai_call(prompt=johns_prompt(alltext))
        try:
            result = json.loads(prompt_1_response)
            result["url"] = url
            result["exhibitor id"] = exhibitor_id_search(url)
            result.pop("reasoning_and_references", None)
            print("prompt 1 done\n")
        except:
            print("prompt 1 failed\n")
            return "error"

        return result

    try:
        while True:
            # List blobs in the container
            blobs = container_client.list_blobs(name_starts_with=input_path)
            json_files = [blob.name for blob in blobs if blob.name.endswith(".json")]
            new_files = [f for f in json_files if f not in processed_files]

            if len(json_files) == len(processed_files):
                print("All files have been processed.")
                break

            for blob_name in new_files:
                print(f"Processing blob: {blob_name}")
                success = False
                retry_attempts = 0

                # Retry processing the file up to 'max_retries' times
                while retry_attempts < max_retries:
                    gpt_response = extract_file_info(blob_name)
                    if gpt_response != "error":
                        results[blob_name] = gpt_response
                        processed_files.add(blob_name)  # Mark as processed
                        success = True
                        break
                    else:
                        retry_attempts += 1
                        print(f"Error processing {blob_name}, retrying... ({retry_attempts}/{max_retries})")
                        time.sleep(2)  # Optional: Add delay between retries

                # If retries exhausted, add file to manual review
                if not success:
                    print(f"Exhausted retries for {blob_name}, adding to manual review.")
                    manual_review["review_manually"].append(blob_name)
                    processed_files.add(blob_name)  # Mark as processed to avoid retry loop

            # Load existing manual reviews and results from Azure Blob Storage if they exist
            try:
                manual_review_blob = f"{result_path}/manual_review.json"
                manual_review_data = download_blob(manual_review_blob)
                manual_review["review_manually"].extend(manual_review_data.get("review_manually", []))
            except Exception as e:
                print(f"No previous manual reviews found or failed to download: {e}")

            try:
                result_blob = f"{result_path}/results.json"
                existing_results = download_blob(result_blob)
                existing_results.update(results)
                results = existing_results
            except Exception as e:
                print(f"No previous results found or failed to download: {e}")

            # Save results and manual reviews to Azure Blob Storage
            upload_blob(f"{result_path}/manual_review.json", manual_review)
            upload_blob(f"{result_path}/results.json", results)

            # Wait for a short time before checking the folder again
            time.sleep(5)

    except KeyboardInterrupt:
        print("Process interrupted. Saving data...")
        upload_blob(f"{result_path}/manual_review.json", manual_review)
        upload_blob(f"{result_path}/results.json", results)

    end_time = time.perf_counter()
    total_time = end_time - start_time
    print(f"\nTotal time taken: {total_time} seconds\n")
