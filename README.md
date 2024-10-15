Start by making a virtual environment for each of the folder:
python -m venv venv

Activate the virtual environment:
source venv/bin/activate

Install requirements:
pip install -r requirements.txt

Start function locally with:
func start

Publish function to Azure:
func azure functionapp publish (function-name)

The corresponding folder for each of the function app is (Folder: Function App):
url-scraper-blob: re-events-url-scraper
url-classifier-azure: url-classifier
large-website-crawler-azure: large-website-scraper
small-website-crawler-azure: small-website-scraper
re-info-gpt: re-info-gpt