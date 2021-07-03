import json


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
