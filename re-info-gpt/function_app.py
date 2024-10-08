import azure.functions as func
import datetime
import json
import logging
from re_info import main_gpt_script

app = func.FunctionApp()

@app.route(route="gpt", auth_level=func.AuthLevel.ANONYMOUS)
def gpt(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processing request.')

    try:
        result = main_gpt_script()

        return func.HttpResponse(f"RE-events info GPT executed successfully. Result: {result}", status_code=200)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)