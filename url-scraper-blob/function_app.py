import azure.functions as func
import datetime
import json
import logging
from test_scraper import test_main_url_extractor
from url_file_maker import file_maker

app = func.FunctionApp()

@app.route(route="", auth_level=func.AuthLevel.ANONYMOUS)
def url_scraper(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('URL Extractor Started')
    result = test_main_url_extractor()
    return func.HttpResponse(f"RE-events URL Scraper executed successfully. Result: {result}", status_code=200)

@app.route(route="", auth_level=func.AuthLevel.ANONYMOUS)
def url_file_maker(req: func.HttpRequest) -> func.HttpResponse:
    logging.info ('URL File Setting started')
    result = file_maker()
    return func.HttpResponse(f"Re-Events URL Files Ready . Result: {result}", status_code=200)