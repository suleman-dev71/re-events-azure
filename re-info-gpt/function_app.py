import azure.functions as func
import datetime
import json
import logging
from re_info import main_gpt_script

app = func.FunctionApp()

@app.route(route="", auth_level=func.AuthLevel.ANONYMOUS)
def gpt(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Re-Info Started')
    result = main_gpt_script()
    return func.HttpResponse(f"RE-events info GPT executed successfully. Result: {result}", status_code=200)