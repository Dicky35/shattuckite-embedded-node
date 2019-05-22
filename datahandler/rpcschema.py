
schema = {
    "request": {
        "title": "data-pack",
        "description": "shattuckite data packet format",
        "type": "object",
        "properties": {
            "id":{
                "description": "id",
                "type": "string",
            },
            "type": {
                "description": "type of rpc command",
                "type": "string",
                "pattern": "(external)|(inner)"
            },
            "name": {
                "description": "The name of exectuable which is needed to be invoke",
                "type": "string",
                "maxLength": 64
            },
            "parameter": {
                "description": "list of parameter pass to exectuable if rpc type is outer",
                "type": "array",
                "items":{
                    "type":"object",
                    "properties":{
                        "key":{
                            "type":"string"
                        },
                        "value":{
                            "type":"string"
                        }
                    }
                }
            },
        },
        "required": ["type", "name"]
    },
}
