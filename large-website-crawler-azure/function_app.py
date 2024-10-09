import azure.functions as func
import datetime
import json
import logging
from website_content_scraper_azure import website_crawler

app = func.FunctionApp()

@app.route(route="", auth_level=func.AuthLevel.ANONYMOUS)
def large_website_crawler(req: func.HttpRequest) -> func.HttpResponse:
   logging.info('Large Websites Scraper processing request.')
   try:
        result = website_crawler()
        return func.HttpResponse(f"Large Websites Scraper executed successfully. Result: {result}", status_code=200)

   except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)