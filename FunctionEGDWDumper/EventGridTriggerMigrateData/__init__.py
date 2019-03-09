#----------------------------------------------------------------------------------------------
# Follow the following two links to create a python function app in azure
# https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-first-function-python
# https://docs.microsoft.com/en-us/azure/azure-functions/functions-reference-python
#----------------------------------------------------------------------------------------------
# Basic info:
# 1. Azure SQL, SqlServer 12.0, data warehouse
# 2. ODBC Driver, 17 (The docker image is based on 17, but locally 13 is also compatible)
# 3. pyodbc, 4.0.26
#----------------------------------------------------------------------------------------------
# TODO: 
# 1. batch/bulk insert
#----------------------------------------------------------------------------------------------
# NOTE:
# 1. bulk insert syntax of T-SQL isn't supported in sql server 12.0 of data warehouse
# 2. executemany runs very slow on sql dataware house, on sql database runs normally
# 3. the average sql insertion performs nearly 50 rows/s
# 4. the concatenated sql statement string can't be too long, 500 insertion into one sql
# string will fall.
#----------------------------------------------------------------------------------------------

import json
import logging
import os
import sys
import time
from io import BytesIO
from urllib.parse import urlparse

import azure.functions as func
from azure.storage import CloudStorageAccount
import pyodbc
from avro.datafile import DataFileReader
from avro.io import DatumReader

from ..SharedCode.windturbine_measure import WindTurbineMeasure 

env_list = os.environ
storage_connection_str = env_list["StorageConnectionString"]
db_connection_str = env_list["PythonSqlDwConnection"]

# parse the environment
storage_infos = storage_connection_str.split(';')
storage_account_name = storage_infos[1].split('=', 1)[1]
storage_account_key = storage_infos[2].split('=', 1)[1]

TABLE_NAME = "dbo.Fact_WindTurbineMetrics"

BATCH_AMOUNT = 250  # an empirical number 

# return container_name and blob_name
def parse_file_url(file_url):

    parse_obj = urlparse(file_url)
    path = str(parse_obj.path)[1:]  # skip the first slash
    arry = path.split("/", 1)

    return arry[0], arry[1] # first part being container_name, second pard being blob_name
    

def dump(file_url):

    # step1: download blob from storage
    storage_account = CloudStorageAccount(storage_account_name, storage_account_key)

    container_name, blob_name = parse_file_url(file_url)
    blob_service = storage_account.create_block_blob_service()
    blob = blob_service.get_blob_to_bytes(container_name, blob_name)
    f = BytesIO(blob.content) # arvo file bytes data

    reader = DataFileReader(f, DatumReader())

    event_list = []
    # step2: get the event data
    for record in reader:
        event_data = json.loads(record["Body"], encoding="ascii", object_hook=WindTurbineMeasure.obj_hook)
        event_list.append(event_data)
    
    # step3: dump to the warehouse
    batch_insert(event_list)


def batch_insert(data_list):

    print('start insert')

    with pyodbc.connect(db_connection_str, autocommit=False) as db_connection:
        with db_connection.cursor() as db_cursor:
            data_table = [(r.device_id, r.measture_time, r.generated_power, r.wind_speed, r.turbine_speed) for r in data_list]

            start_time = time.time()

            try:
                sql, exec_cnt = "", 0

                for data in data_table:
                    if exec_cnt < BATCH_AMOUNT:
                        sql += "INSERT INTO " + TABLE_NAME + " VALUES "
                        sql += ('(' + 'N\'' + data[0] + '\',N\'' + data[1] + '\',' + str(data[2]) + ',' + str(data[3]) + ',' + str(data[4]) + ');')
                        exec_cnt += 1
                    else:
                        sub_start_time = time.time()
                        logging.info('execute the sql')
                        db_cursor.execute(sql)

                        sub_end_time = time.time()
                        logging.info(str(sub_end_time - sub_start_time))
                        exec_cnt = 0
                        sql = ""

                if exec_cnt != 0:
                    logging.info('final execute the sql')
                    db_cursor.execute(sql)

                end_time = time.time()
                logging.info(str(end_time-start_time))

                #db_cursor.executemany("insert into " + TABLE_NAME + " (DeviceId, MeasureTime, GeneratedPower, WindSpeed, TurbineSpeed) values (?,?,?,?,?)", data_table)
                print("batch insert done")
                logging.info("batch insert done")

            except Exception as ex:
                print(ex)
                logging.error(ex)
            finally:
                db_connection.commit()


def main(event: func.EventGridEvent):
    
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    logging.info("received event:" + result)

    try:
        if event.get_json()["eventCount"] != 0:
            dump(event.get_json()["fileUrl"])
    except Exception as e:
        logging.error(e)