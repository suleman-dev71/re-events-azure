import azure.functions as func
import datetime
import json
import logging
from website_scrapper_small_azure import small_site_crawler

app = func.FunctionApp()

@app.route(route="", auth_level=func.AuthLevel.ANONYMOUS)
def small_website_crawler(req: func.HttpRequest) -> func.HttpResponse:
     logging.info('Small Websites Scraper processing request.')
     result = small_site_crawler()
     return func.HttpResponse(f"Small Websites Scraper executed successfully. Result: {result}", status_code=200)