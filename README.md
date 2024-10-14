Start by making a virtual environment for each of the folder:
python -m venv venv

Activate the virtual environment:
source venv/bin/activate

Install requirements:
pip install -r requirements.txt

Start function locally with:
func start

Publish function to Azure:
func azure functionapp publish (function-name)