schema = {
    "root": {
        "title": "data-pack",
        "description": "shattuckite data packet format",
        "type": "object",
        "properties": {
            "type": {
                "description": "type of this data packet",
                "type": "string"
            },
            "payload": {
                "description": "more data of this data packet",
                "type": "object"
            },
        },
        "required": ["type", "payload"]
    },
    "sensor": {
        "title": "data-pack",
        "description": "shattuckite data packet format",
        "type": "object",
        "properties": {
            "timestamp": {
                "type": "string"
            },
            "value": {
                "type": "string"
            },
            "sensorId": {
                "type": "string"
            }
        },
        "required": ["datetime", "value","sensorId"]
    },
}
