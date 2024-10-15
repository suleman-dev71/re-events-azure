import azure.functions as func
import datetime
import json
import logging
from website_content_scraper_azure import website_crawler

app = func.FunctionApp()

@app.route(route="", auth_level=func.AuthLevel.ANONYMOUS)
def large_website_crawler(req: func.HttpRequest) -> func.HttpResponse:
     logging.info('Large Websites Scraper processing request.')
     result = website_crawler()
     return func.HttpResponse(f"Large Websites Scraper executed successfully. Result: {result}", status_code=200)