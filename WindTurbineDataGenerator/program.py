#----------------------------------------------------
# Based on Python 3.6.8 64bit
#----------------------------------------------------
# Done:
# 1. sync send event data
# 2. async send event data
#------------------------------------------------------
# TODO:
# 1. in c# version there is a cancellationtoken
# 2. optimization, error handling, argument validation
#------------------------------------------------------

import datetime
import time
import json
import random
import logging
import asyncio
from threading import Thread
from concurrent.futures import ThreadPoolExecutor

from azure.eventhub import EventHubClient, Sender, EventData

from windturbine_measure import WindTurbineMeasure

#-------------------- START OF GLOBAL VARIABLES --------------------*

EVENTHUB_CONNECTION_STRING = "[provide the EH connection string]"
EVENTHUB_NAME = "[provide the EH name]"

# set true to use async method, false to use sync method
use_async = True

# logger setting
logger = logging.getLogger('WindTurbineDataGenerator')
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('generator.log')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

logger.addHandler(fh)

# execution thread pool
# one thread for listening to the user input, which is IO-block. Another for continuously sending data
thread_pool = ThreadPoolExecutor(2)

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

    return EventData(message_bytes)

# the real sending part
def batch_send(ev_datas, sender):
    try:
        for ev_data in ev_datas:
            sender.send(ev_data)

        print(".", end='', flush=True)

    except Exception as e:
        logger.error(e)
    return

# async wrapper for the batch_send function
async def batch_send_async(loop, ev_datas, sender):
    await loop.run_in_executor(thread_pool, batch_send, ev_datas, sender)

# input() is io-blocked
def user_input():
    val = input()
    return val

# async wrapper for the input
async def user_input_async(loop):
    await loop.run_in_executor(thread_pool, user_input)

async def user_input_monitor():
    loop = asyncio.get_event_loop()
    await user_input_async(loop)
    global cancel_requested
    cancel_requested = True
    return

async def start_event_generation_async_impl():

    loop = asyncio.get_event_loop()
    
    random.seed(int(time.time())) # use ticks as seed
    
    client = EventHubClient.from_connection_string(conn_str=EVENTHUB_CONNECTION_STRING, eventhub=EVENTHUB_NAME)
    sender = client.add_sender()
    client.run()
    
    while not cancel_requested:
        # Simulate sending data from 100 weather sensors
        devices_data = []
        
        for i in range(0, 100):
            scale_factor = random.randrange(0,25)
            windturbine_measure = generate_turbine_measure("Python_Turbine_" + str(i), scale_factor)
            ev_data = serialize_windturbine_to_eventdata(windturbine_measure)
            devices_data.append(ev_data)
            
        send_res = await batch_send_async(loop, devices_data, sender)
        logger.info(send_res)

    client.stop()

def start_event_generation_async():

    global cancel_requested
    cancel_requested = False
    
    loop = asyncio.get_event_loop()
    tasks = [
        loop.create_task(user_input_monitor()),
        loop.create_task(start_event_generation_async_impl())
    ]
    
    loop.run_until_complete(asyncio.wait(tasks))
    thread_pool.shutdown()

def start_event_generation_sync_impl():

    random.seed(int(time.time())) # use ticks as seed
    
    client = EventHubClient.from_connection_string(conn_str=EVENTHUB_CONNECTION_STRING, eventhub=EVENTHUB_NAME)
    sender = client.add_sender()
    client.run()

    while not cancel_requested:
        try:
            # Simulate sending data from 100 weather sensors
            #devices_data = []
        
            for i in range(0, 100):
                scale_factor = random.randrange(0,25)
                windturbine_measure = generate_turbine_measure("Python_Turbine_" + str(i), scale_factor)
                ev_data = serialize_windturbine_to_eventdata(windturbine_measure)
                send_res = sender.send(ev_data) # this is a block func
                logger.info(windturbine_measure.device_id + ' ' + str(send_res))
                #devices_data.append(ev_data)
            
            print(".", end='', flush=True)

        except Exception as e:
            logger.error(e)
    
    client.stop()

def start_event_generation_sync():

    global cancel_requested
    cancel_requested = False

    worker = Thread(target=start_event_generation_sync_impl)
    worker.start()

    input()
    cancel_requested = True

    worker.join()

if __name__ == "__main__":
    
    print("Starting wind turbine generator. Press <ENTER> to exit")

    if use_async:
        start_event_generation_async()
    else:
        start_event_generation_sync()

