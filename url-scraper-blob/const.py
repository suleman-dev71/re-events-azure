from datetime import datetime

current_date = datetime.now().strftime("%d-%b")

BASE_FOLDER = f"RE-Events-{current_date}-run"
CONTAINER_NAME = "re-events-v1"