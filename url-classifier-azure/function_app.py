import azure.functions as func
import datetime
import json
import logging
from url_classifier import classify_urls

app = func.FunctionApp()

@app.route(route="url_classifier", auth_level=func.AuthLevel.ANONYMOUS)
def url_classifier(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processing request.')

    try:
        result = classify_urls()

        return func.HttpResponse(f"URL classifier executed successfully. Result: {result}", status_code=200)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)