import azure.functions as func
import datetime
import json
import logging
from test_scraper import test_main_url_extractor
app = func.FunctionApp()

# todo: restrict auth level
@app.route(route="url-scraper", auth_level=func.AuthLevel.ANONYMOUS)
def url_scraper(req: func.HttpRequest) -> func.HttpResponse:
    # todo:logs retention in daignostic settings
    logging.info('Python HTTP trigger function processed a request.')
    test_main_url_extractor()

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )