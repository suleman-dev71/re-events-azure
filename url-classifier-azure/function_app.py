import azure.functions as func
import datetime
import json
import logging
from url_classifier import classify_urls

app = func.FunctionApp()

@app.route(route="", auth_level=func.AuthLevel.ANONYMOUS)
def url_classifier(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('URL Classifier Started')
    result = classify_urls()
    return func.HttpResponse(f"URL classifier executed successfully. Result: {result}", status_code=200)