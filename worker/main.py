import os
import sys
import json
import logging
from urllib.parse import urlparse

import aiohttp
from aiohttp import web, request
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError, KafkaTimeoutError

from utils import json_extract, serializer


logging.basicConfig(filename='./logs/worker.log', level=logging.INFO, format='%(asctime)s %(levelname)s | %(message)s')


def load_settings() -> dict:
    """Loads settings from env variables

    Returns:
        dict: [dict of settings]
    """
    try:
        conf = dict(
            ROW_MESSAGE_TOPIC=os.environ['ROW_MESSAGE_TOPIC'],
            BOOTSTRAP_SERVERS=os.environ['BOOTSTRAP_SERVERS'].split(';'),
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
    body = await request.json()
    try:
        url = body['url']
        logging.info(f'Received url: {url}')
    except KeyError:
        logging.warning('Incorrect JSON sent: no "url" key')
        return web.json_response(dict(success=False, message='Incorrect JSON sent: no "url" key'), status=400)

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
