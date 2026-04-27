import requests
from datetime import datetime
import os

today = datetime.today().strftime("%Y-%m-%d")   
output_dir = os.path.join(os.path.dirname(__file__), "..","output", today)
file_path = os.path.join(output_dir, "latestHumanlist.xml")                        

url = "https://assets.hpra.ie/products/xml/latestHumanlist.xml"
#url = "https://assets.hpra.ie/products/xml/withdrawnHumanlist.xml"
response = requests.get(url, timeout=60)
response.raise_for_status()

with open(file_path, "wb") as f:
    f.write(response.content)

print("Saved latestHumanlist.xml")