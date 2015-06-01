import time
from datetime import datetime, timedelta
import os
from powertrack.api import *


config = ConfigParser.RawConfigParser()
config.read("pepsicoca.conf")

IMPORT_API_ENDPOINT = config.get('cartodb', 'import_api_endpoint')
ACCOUNT_NAME = config.get('cartodb', 'account_name')
API_KEY = config.get('cartodb', 'api_key')
TABLE_NAME = config.get('cartodb', 'table_name')
RUN_AFTER_S = config.get('intervals', 'run_after_s')
CLEAN_OLDER_THAN_D = config.get('intervals', 'clean_older_than_d')

p = PowerTrack(api="search")

categories = [["#pepsi"], ["#cocacola"]]

start_timestamp = datetime.utcnow() - timedelta(days=int(CLEAN_OLDER_THAN_D))
end_timestamp = datetime.utcnow()

table_name = TABLE_NAME

for i, category in enumerate(categories):
    new_job = p.jobs.create(start_timestamp, end_timestamp, table_name, category)
    new_job.export_tweets(category=i + 1, append=False if i == 0 else True)

files = {'file': open(table_name + '.csv', 'rb')}

r = requests.post(IMPORT_API_ENDPOINT, files=files, params={"api_key": API_KEY})
response_data = r.json()
print "SUCCESS", response_data["success"]

state = "uploading"
item_queue_id = response_data["item_queue_id"]
while state != "complete" and state != "failure":
    time.sleep(5)
    r = requests.get(IMPORT_API_ENDPOINT + item_queue_id, params={"api_key": API_KEY})
    response_data = r.json()
    state = response_data["state"]
    print response_data

if state == "complete":
    table_name = response_data["table_name"]  # Just in case it changed during import
    with open("{table_name}_next.conf".format(table_name=table_name), "w") as conf:
        conf.write(json.dumps({"start_timestamp": end_timestamp.strftime("%Y%m%d%H%M%S")}))

try:
    os.remove(table_name + '.csv')
except OSError:
    pass
