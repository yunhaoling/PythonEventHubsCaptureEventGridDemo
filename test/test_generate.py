import os
import datetime
import random
import time
import sys
import asyncio
from threading import Thread, Event

sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/..'))
from WindTurbineDataGenerator.program import start_event_generation_sync_impl, start_event_generation_async_impl


def cancel(cancellation_token, wait_seconds):
    time.sleep(wait_seconds)
    cancellation_token.clear()

async def cancel_async(cancellation_token, wait_seconds):
    await asyncio.sleep(wait_seconds)
    cancellation_token.clear()

def test_send_sync():
    wait_seconds = 10
    
    cancellation_token = Event()
    cancellation_token.set()

    worker = Thread(target=start_event_generation_sync_impl, args=(cancellation_token,))
    waiter = Thread(target=cancel, args=((cancellation_token,wait_seconds)))
    worker.start()
    waiter.start()

    waiter.join()
    worker.join()


def test_send_async():
    wait_seconds = 10

    cancellation_token = Event()
    cancellation_token.set()
    
    loop = asyncio.get_event_loop()
    tasks = [
        loop.create_task(cancel_async(cancellation_token, wait_seconds)),
        loop.create_task(start_event_generation_async_impl(cancellation_token))
    ]

    loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()
