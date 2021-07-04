import os
import re
import sys
import json
import logging
from urllib.parse import urlparse

import aiohttp
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from aiohttp import web, request
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError, KafkaTimeoutError

from utils import json_extract, serializer
from json_schema import request_schema


logging.basicConfig(filename='./logs/worker.log', level=logging.INFO, format='%(asctime)s %(levelname)s | %(message)s')


def load_settings() -> dict:
    """Loads settings from env variables

    Returns:
        dict: [dict of settings]
    """
    try:
        conf = dict(
            # ROW_MESSAGE_TOPIC=os.environ['ROW_MESSAGE_TOPIC'],
            # BOOTSTRAP_SERVERS=os.environ['BOOTSTRAP_SERVERS'].split(';'),
        )
        return conf
    except KeyError as e:
        logging.error(f'Provide required key! {e}')
        print(f'Provide required settings key! {e}')
        sys.exit(1)


conf = load_settings()


async def handle(request: request) -> web.json_response:
    """WB API processor and kafka sender

    Args:
        request (request): request object
    Returns:
        [web.json_response]: [json response]
    """
    try:
        body = await request.json()
    except json.decoder.JSONDecodeError as e:
        logging.error('Request body is not a valid JSON')
        return web.json_response(dict(success=False, message='Request body is not a valid JSON'), status=400)

    try:
        validate(body, schema=request_schema)
    except ValidationError as e:
        wrong_key = re.findall('(\'.+\') is a required property', e.__str__())
        if wrong_key:
            logging.warning(f'{str(*wrong_key)} key is required')
            return web.json_response(dict(success=False, message=f'{str(*wrong_key)} key is required'), status=400)
        logging.warning('url data type is not a string')
        return web.json_response(dict(success=False, message='url data type is not a string'), status=400)

    url = body['url']
    parsed_url = urlparse(url)
    path = parsed_url.path

    if '/' not in path:
        logging.warning(f'Can\'t parse url: {url}')
        return web.json_response(dict(success=False, message='Incorrect url'), status=400)

    logging.info(f'Successfully parsed url: {url}')

    params = {
        'locale': 'ru',
        'lang': 'ru'
    }
    # get catalog via wb api
    async with session.get('http://wbxmenu-ru.wildberries.ru/v2/api', params=params) as response:
        if response.status != 200:
            logging.error('Can\'t receive catalog')
            return web.json_response(dict(success=False, message='WB API error during receiving catalog'), status=503)
        _json = await response.json()
        query = json_extract(_json, path)
        if not query:
            logging.warning(f'Can\t find provided category {path} in catalog')
            return web.json_response(dict(
                success=False,
                message=f'Can\t find provided category {path} in catalog'
            ), status=400)


    logging.info('Received catalog')

    # start kafka producing
    producer = AIOKafkaProducer(bootstrap_servers=conf['BOOTSTRAP_SERVERS'], value_serializer=serializer)
    # get 5 product pages and sent them to wb-category topic
    for num in range(1, 6):
        async with session.get(
                f'http://wbxcatalog-ru.wildberries.ru/electronic/catalog?{query}&lang=ru&locale=ru&page={num}'
        ) as response:
            if response.status != 200:
                logging.error('Can\'t receive products')
                return web.json_response(dict(
                    success=False,
                    message='WB API error during receiving list of products'
                ), status=503)
            _json = json.loads(await response.text())
            logging.info(f'Received {num} product page')
            await producer.start()
            try:
                # sending messages to kafka
                await producer.send_and_wait(conf['ROW_MESSAGE_TOPIC'], _json)
                logging.info(f'Successfully sent message to {conf["ROW_MESSAGE_TOPIC"]}')
            except KafkaTimeoutError as e:
                logging.error('Kafka broker timeout')
                return web.json_response(dict(success=False, message='Kafka broker timeout'), status=503)
            except KafkaError as e:
                logging.error(f'Kafka broker error: {e}')
                return web.json_response(dict(success=False, message='Kafka broker error'), status=503)
    await producer.stop()
    logging.info(f'All messages successfully sent to kafka topic {conf["ROW_MESSAGE_TOPIC"]}')
    return web.json_response(
        dict(
            success=True,
            message=f'All messages successfully sent to kafka topic {conf["ROW_MESSAGE_TOPIC"]}'
        )
    )


app = web.Application()
app.add_routes([web.post('/catalog', handle)])

if __name__ == '__main__':
    session = aiohttp.ClientSession()
    web.run_app(app)
