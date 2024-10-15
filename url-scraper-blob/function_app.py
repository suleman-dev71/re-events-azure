import azure.functions as func
import datetime
import json
import logging
from test_scraper import test_main_url_extractor
app = func.FunctionApp()

@app.route(route="", auth_level=func.AuthLevel.ANONYMOUS)
def url_scraper(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('URL Extractor Started')
    result = test_main_url_extractor()
    return func.HttpResponse(f"RE-events info GPT executed successfully. Result: {result}", status_code=200)