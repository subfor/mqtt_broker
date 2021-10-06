import logging
import asyncio

import aiohttp
import os
from aiohttp import ClientConnectorError
from amqtt.client import MQTTClient, ClientException, ConnectException
from amqtt.mqtt.constants import QOS_1
from models import time_now, write_to_db


URL = os.getenv('URL')
MQTT_USER = os.getenv('MQTT_USER')
MQTT_PASS = os.getenv('MQTT_PASS')
MQTT_HOST = os.getenv('MQTT_HOST')
MQTT_PORT = os.getenv('MQTT_PORT')

logger = logging.getLogger(__name__)


async def make_request(data: dict, table_name: str, time: str):
    params = {"date": time,
              "id": data['client_name'],
              "string": data['data'],
              "type": table_name
              }

    async with aiohttp.request('POST', URL, json=params) as response:
        return response


async def push_data(data: dict, table_name: str, time: str):
    try:
        response = await make_request(data, table_name, time)
    except ClientConnectorError:
        print('Connection error')


async def uptime_coro():
    config = {
        'keep_alive': 30000,
    }
    client = MQTTClient(config=config)

    await client.connect(f"mqtt://{MQTT_USER}:{MQTT_PASS}@{MQTT_HOST}:{MQTT_PORT}")
    await client.subscribe(
        [
            ("/+/telemetry", QOS_1),
            ("/+/event", QOS_1),
            ("/+/receipt", QOS_1)
        ]
        )

    logger.info("Subscribed")
    try:
        while True:
            message = await client.deliver_message()
            packet = message.publish_packet
            table_name = packet.variable_header.topic_name.split("/")[-1].capitalize()
            data = {
                'client_name': packet.variable_header.topic_name.split("/")[1],
                'data': packet.payload.data.decode("utf-8")
            }
            time = str(time_now())
            await write_to_db(data, table_name)
            await push_data(data, table_name, time)

    except ClientException as ce:
        logger.error("Client exception: %s" % ce)

if __name__ == "__main__":
    formatter = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=formatter)
    asyncio.get_event_loop().run_until_complete(uptime_coro())

