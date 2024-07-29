# Constraint Validation
Most of the constraints are written in the wording like:   
 - entity_type can be a descendant of entity_type 
 - entity_type [sub_type] can be a descendant of entity_type [subtype]
 - *can be a descendant* of is denoted by `--->` in comments
 - Look at the sample code to write new constraints

## The REST API
### Endpoint:
`/constraints`
### Request params:
- `match`: Set to true to match entries. Default is `false`
- `order`: `ancestors` or `descendants`. Determines how to retrieve/match against. The opposite property constraints will be returned.
- `filter`: Use to call a specific function within the code. Currently, only `search` is available.

### Payload Request Format
Each request must be an array of objects with `ancestors` and `descendants` properties. At least 1 of these properties must be
present when retrieving the other, and both must be present when validating/matching.
Each of these two properties are also arrays.
```
[
    {
        "ancestors": [
            {
                "entity_type": Entities.strEnum,
                "sub_type": array,
                "sub_type_val": array
            }
        ],
        # example
        "descendants": [
            {
                "entity_type": "Sample",
                "sub_type": ["suspension"],
                "sub_type_val": null
            }
        ]
    }
]
```
### Example Requests:
The following request will fail because the request
does not match the constraint requirements. It will result in 
a 400 Bad Request.
The `description` property returns the valid constraints for the given relationship.
If you do not specify the `order` request param, the request will be made against
the given `ancestors` and the `response.description` will return the valid `descendants`.  
#### Request 1a:
`/constraints?match=true`
```
[
    {
        "ancestors": [
            {
                "entity_type": "Sample",
                "sub_type": ["block"],
                "sub_type_val": null
            }
        ],
        "descendants": [
          	{
                "entity_type": "Sample",
                "sub_type": ["suspension"],
                "sub_type_val": null
        	}
        ]
    }
]
```
#### Response 1a:
```
{
    "code": 400,
    "description": [
        {
            "code": 404,
            "description": [
                {
                    "entity_type": "Sample",
                    "sub_type": [
                        "block",
                        "section",
                        "suspension"
                    ],
                    "sub_type_val": null
                },
                {
                    "entity_type": "Dataset",
                    "sub_type": [
                        "Lightsheet"
                    ],
                    "sub_type_val": null
                }
            ],
            "name": "Match not found. Valid `descendants` in description."
        }
    ],
    "name": "Bad Request"
}
```
The **currently** correct match for an ancestor sample block is: 
#### Request 1b:
`/constraints?match=true`
```
[
    {
        "ancestors": [
            {
                "entity_type": "Sample",
                "sub_type": ["block"],
                "sub_type_val": null
            }
        ],
        "descendants": [
            {
                "entity_type": "Sample",
                "sub_type": ["block", "section", "suspension"],
                "sub_type_val": null
            }
        ]
    }
]
```
#### Response 1b:
```
{
    "code": 200,
    "description": [
        {
            "code": 200,
            "description": [
                {
                    "entity_type": "Sample",
                    "sub_type": [
                        "block",
                        "section",
                        "suspension"
                    ],
                    "sub_type_val": null
                },
                {
                    "entity_type": "Dataset",
                    "sub_type": [
                        "Lightsheet"
                    ],
                    "sub_type_val": null
                }
            ],
            "name": "OK"
        }
    ],
    "name": "OK"
}

```
You can reverse the order and the `response.description` will give you valid ancestors in return. 
#### Request 1c:
`/constraints?match=true&order=descendants`
(Payload request is same as `1b` above.)

#### Response 1c:
```
{
    "code": 200,
    "description": [
        {
            "code": 200,
            "description": {
                "entity_type": "Sample",
                "sub_type": [
                    "block"
                ],
                "sub_type_val": null
            },
            "name": "OK"
        }
    ],
    "name": "OK"
}

```

### Getting the descendants given a particular ancestor:
Remove the `match` param from the request url:
#### Request 2a:
`/constraints`
```
[
    {
        "ancestors": [
            {
                "entity_type": "Sample",
                "sub_type": ["block"],
                "sub_type_val": null
            }
        ]
    }
]
```
The response will be same as **1b** above.
You can retrieve the `ancestors` given a particular descendant:
#### Request 2b:
`/constraints?order=descendants`
```
[
    {
        "descendants": [
            {
                "entity_type": "Sample",
                "sub_type": ["block", "section", "suspension"],
                "sub_type_val": null
            }
        ]
    }
]
```
The response will be same as **1c** above.


### The `filter` request param:
The following  makes a special use case filter.  
#### Request 3a:
`/constraints?filter=search&order=descendants`  
```
[
    {
        "descendants": [
            {
                "entity_type": "Dataset",
                "sub_type": null,
                "sub_type_val": null
            }
        ]
    }
]
```
#### Response 3b:
```
{
    "code": 200,
    "description": [
        {
            "code": 200,
            "description": [
                {
                    "keyword": "entity_type.keyword",
                    "value": "Dataset"
                },
                {
                    "keyword": "sample_category.keyword",
                    "value": "block"
                },
                {
                    "keyword": "sample_category.keyword",
                    "value": "section"
                },
                {
                    "keyword": "sample_category.keyword",
                    "value": "suspension"
                }
            ],
            "name": "OK"
        }
    ],
    "name": "OK"
}
```

### Retrieving and validating multiple rows:
#### Request 4a:
`/constraints`
```
[
  {
    "ancestors": [
      {
        "entity_type": "Sample",
        "sub_type": [
          "block"],
        "sub_type_val": null
      }
    ]
  },
  {
    "ancestors": [
      {
        "entity_type": "Source",
        "sub_type": null,
        "sub_type_val": null
      }
    ]
  }
]
```
#### Response 4a:
```
{
    "code": 200,
    "description": [
        {
            "code": 200,
            "description": [
                {
                    "entity_type": "Sample",
                    "sub_type": [
                        "block",
                        "section",
                        "suspension"
                    ],
                    "sub_type_val": null
                },
                {
                    "entity_type": "Dataset",
                    "sub_type": [
                        "Lightsheet"
                    ],
                    "sub_type_val": null
                }
            ],
            "name": "OK"
        },
        {
            "code": 200,
            "description": [
                {
                    "entity_type": "Sample",
                    "sub_type": [
                        "organ"
                    ],
                    "sub_type_val": null
                }
            ],
            "name": "OK"
        }
    ],
    "name": "OK"
}

```

## Disclaimer:
This library will not match or retrieve for given multiple `ancestors` in one row, or if `order=descendants`, will not match multiple `descendants` in the same row. The first item
is taken as unit. So the following will result in just the response for the first ancestor (sample block).
#### Request 5a:
`/constraints`
```
[
    {
        "ancestors": [
            {
                "entity_type": "Sample",
                "sub_type": ["block"],
                "sub_type_val": null
            },
            {
                "entity_type": "Dataset",
                "sub_type": null,
                "sub_type_val": null
            }
        ]
    }
]
```