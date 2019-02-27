#----------------------------
# Based on Python 3.6.8 64bit
#----------------------------

import datetime
import time
import json
import random
import logging
from threading import Thread

from azure.eventhub import EventHubClient, Sender, EventData

from windturbine_measure import WindTurbineMeasure

EVENTHUB_CONNECTION_STRING = "[provide the EH connection string]"

EVENTHUB_NAME = "[provide the EH name]"

# logger setting
logger = logging.getLogger('WindTurbineDataGenerator')
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('generator.log')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

logger.addHandler(fh)


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


# TODO: async, cancellationToken
def start_event_generation_async():

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


if __name__ == "__main__":

    print("Starting wind turbine generator. Press <ENTER> to exit")

    global cancel_requested
    cancel_requested = False

    worker = Thread(target=start_event_generation_async)
    worker.start()

    var = input()
    cancel_requested = True

    worker.join()
