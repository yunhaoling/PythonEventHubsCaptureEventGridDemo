import os
import datetime
import random
import time
import sys

os.environ["StorageConnectionString"] = "[provide the storage connection string]"
os.environ["PythonSqlDwConnection"] = "[provide the python sql data warehouse connection string]"

sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/..'))
from FunctionEGDWDumper.EventGridTriggerMigrateData import dump, batch_insert, WindTurbineMeasure

def test_dump():
    azure_storage_file_path = "[provide the storage file path]"
    dump(azure_storage_file_path)

def test_batch_insert():
    event_list = []

    random.seed(int(time.time()))
    
    for i in range(1000):
        scale_factor = random.randrange(0,25)
        measture_time = str(datetime.datetime.now())[0:-3]
        generated_power = float(2.5) * scale_factor
        wind_speed = 15 * scale_factor
        turbine_speed = float(0.3) * scale_factor
        event = WindTurbineMeasure("TestDevice" + str(i), measture_time, generated_power, wind_speed, turbine_speed)
        event_list.append(event)
    
    batch_insert(event_list)