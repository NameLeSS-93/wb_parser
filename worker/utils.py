import json
from typing import Dict, List, Union


def serializer(value: dict) -> bytes:
    """Serialize json data before send it to kafka

    Args:
        value (dict): [python dict]

    Returns:
        bytes: [encoded message]
    """
    return json.dumps(value).encode('utf-8')


def deserializer(serialized: bytes) -> dict:
    """Deserialize message from kafka

    Args:
        serialized (bytes): [serialized message]

    Returns:
        dict: [python dict]
    """
    return json.loads(serialized)


def json_extract(obj: Union[Dict, List], path: str) -> str:
    """Recursively fetch values from nested JSON.

    Args:
        obj (Union[Dict, List]): [description]
        path (str): [description]

    Returns:
        str: [description]
    """
    arr = []

    def extract(obj, arr):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr)
                elif k == 'pageUrl' and v == path:
                    arr.append(obj.get('query'))
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr)
        return arr

    values = extract(obj, arr)
    return values[0]
