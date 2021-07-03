import os
import sys
import uuid
import asyncio
import logging
from datetime import datetime

import uvloop
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaTimeoutError

from utils import deserializer, serializer


logging.basicConfig(filename='./logs/parser.log', level=logging.INFO, format='%(asctime)s %(levelname)s | %(message)s')


def load_settings() -> dict:
    """Loads settings from env variables

    Returns:
        dict: [dict of settings]
    """
    try:
        conf = dict(
            ROW_MESSAGE_TOPIC=os.environ['ROW_MESSAGE_TOPIC'],
            GROUP_ID=os.environ['GROUP_ID'],
            PROCESSED_MESSAGE_TOPIC=os.environ['PROCESSED_MESSAGE_TOPIC'],
            BOOTSTRAP_SERVERS=os.environ['BOOTSTRAP_SERVERS'].split(';'),
            AUTO_OFFSET_RESET=os.environ['AUTO_OFFSET_RESET'],
            POLL_TIMEOUT=int(os.environ['POLL_TIMEOUT'])
        )
        return conf
    except KeyError as e:
        logging.error(f'Provide required key! {e}')
        print(f'Provide required settings key! {e}')
        sys.exit(1)


conf = load_settings()


async def transactional_process() -> None:
    """Reads raw messages from kafka wb-category topic extract
    id, name, price, sale, add timestamp and send them to wb-products topic
        Returns:
            [None]: []
        """
    # init consumer
    consumer = AIOKafkaConsumer(
        conf['ROW_MESSAGE_TOPIC'],
        bootstrap_servers=conf['BOOTSTRAP_SERVERS'],
        enable_auto_commit=False,
        value_deserializer=deserializer,
        group_id=conf['GROUP_ID'],
        auto_offset_reset=conf['AUTO_OFFSET_RESET'],
        isolation_level="read_committed"
    )
    await consumer.start()

    # init producer
    producer = AIOKafkaProducer(
        bootstrap_servers=conf['BOOTSTRAP_SERVERS'],
        transactional_id=f"{str(uuid.uuid4())}",
        value_serializer=serializer
    )
    await producer.start()

    try:
        while True:
            try:
                # start reading messages
                msg_batch = await consumer.getmany(timeout_ms=conf['POLL_TIMEOUT'])
                logging.info(f'Successfully received a batch of messages from {conf["ROW_MESSAGE_TOPIC"]}')
            except KafkaTimeoutError:
                logging.error(f'Kafka timeout')
                continue
            async with producer.transaction():
                for _, msgs in msg_batch.items():
                    for msg in msgs:
                        for product in msg.value['data']['products']:
                            out_msg = dict()
                            # creating message for wb-products topic
                            try:
                                out_msg['timestamp'] = int(datetime.timestamp(datetime.now()))
                                out_msg['id'] = product['id']
                                out_msg['name'] = product['name']
                                out_msg['price'] = product['priceU'] / 100
                                out_msg['sale'] = product['sale']
                            except KeyError as e:
                                logging.error(f'No required key {e} in received message {product}')
                            # send it
                            await producer.send(
                                conf["PROCESSED_MESSAGE_TOPIC"],
                                value=out_msg
                            )
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(transactional_process())
