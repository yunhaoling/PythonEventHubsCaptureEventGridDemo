#------------------------------------------------------
# Based on Python 3.6.8 64bit
#------------------------------------------------------
# Done:
# 1. sync send event data
# 2. async send event data
#------------------------------------------------------
# TODO:
# 1. optimization, error handling, argument validation
#------------------------------------------------------
# Note:
# 1. use threading.Event as a signal
#------------------------------------------------------

import datetime
import time
import json
import random
import logging
import asyncio
from threading import Thread, Event
from concurrent.futures import ThreadPoolExecutor

from azure.eventhub import EventHubClient, Sender, EventData, EventHubClientAsync, AsyncSender

import sys
sys.path.append('..')
from windturbine_measure import WindTurbineMeasure

#-------------------- START OF GLOBAL VARIABLES --------------------*
EVENTHUB_CONNECTION_STRING = "[provide the EH connection string]"
EVENTHUB_NAME = "[provide the EH name]"

use_async = True

# logger setting
logger = logging.getLogger('WindTurbineDataGenerator')
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('generator.log')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

logger.addHandler(fh)
#-------------------- END OF GLOBAL VARIABLES --------------------*

def generate_turbine_measure(turbine_id : str, scale_factor : int):

    device_id = turbine_id
    measture_time = datetime.datetime.now()
    generated_power = float(2.5) * scale_factor
    wind_speed = 15 * scale_factor
    turbine_speed = float(0.3) * scale_factor

    return WindTurbineMeasure(device_id, measture_time, generated_power, wind_speed, turbine_speed)

def serialize_windturbine_to_eventdata(windturbine_measure):
    message_string = json.dumps(windturbine_measure.to_dict(), ensure_ascii=True)
    message_bytes = message_string.encode(encoding="ascii")

    return message_bytes

# input() is io-blocked
def user_input():
    val = input()
    return val

# async wrapper for the input
async def user_input_async(loop, thread_pool):
    await loop.run_in_executor(thread_pool, user_input)

async def user_input_monitor(loop, cancellation_token):
    with ThreadPoolExecutor(1) as thread_pool:
        await user_input_async(loop, thread_pool)
        cancellation_token.clear()
        return

async def start_event_generation_async_impl(cancellation_token): 

    random.seed(int(time.time())) # use ticks as seed
    
    client = EventHubClientAsync.from_connection_string(conn_str=EVENTHUB_CONNECTION_STRING, eventhub=EVENTHUB_NAME)
    sender = client.add_async_sender()

    client.run()
    
    while cancellation_token.is_set():
        # Simulate sending data from 100 weather sensors
        devices_data = []
        
        for i in range(0, 100):
            scale_factor = random.randrange(0,25)
            windturbine_measure = generate_turbine_measure("Python_Turbine_" + str(i), scale_factor)
            ev_data = serialize_windturbine_to_eventdata(windturbine_measure)
            devices_data.append(ev_data)
        
        await sender.send(EventData(batch=[event for event in devices_data]))
        print(".", end='', flush=True)
        logger.info("100 events sent!")

    client.stop()
    return

def start_event_generation_async():
    cancellation_token = Event()
    cancellation_token.set()
    
    loop = asyncio.get_event_loop()
    tasks = [
        loop.create_task(user_input_monitor(loop, cancellation_token)),
        loop.create_task(start_event_generation_async_impl(cancellation_token))
    ]

    #loop.run_until_complete(asyncio.wait(tasks))
    loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()

def start_event_generation_sync_impl(cancellation_token):

    random.seed(int(time.time())) # use ticks as seed
    
    client = EventHubClient.from_connection_string(conn_str=EVENTHUB_CONNECTION_STRING, eventhub=EVENTHUB_NAME)
    sender = client.add_sender()
    client.run()

    while cancellation_token.is_set():
        try:
            # Simulate sending data from 100 weather sensors
            devices_data = []
        
            for i in range(0, 100):
                scale_factor = random.randrange(0,25)
                windturbine_measure = generate_turbine_measure("Python_Turbine_" + str(i), scale_factor)
                ev_data = serialize_windturbine_to_eventdata(windturbine_measure)
                devices_data.append(ev_data)

            sender.send(EventData(batch=[event for event in devices_data])) 
            logger.info("100 events sent!")
            print(".", end='', flush=True)

        except Exception as e:
            logger.error(e)
    
    client.stop()

def start_event_generation_sync():

    cancellation_token = Event()
    cancellation_token.set()
    worker = Thread(target=start_event_generation_sync_impl, args=(cancellation_token,))
    worker.start()

    input()
    cancellation_token.clear()
    worker.join()

if __name__ == "__main__":
    
    print("Starting wind turbine generator. Press <ENTER> to exit")

    if use_async:
        start_event_generation_async()
    else:
        start_event_generation_sync()

