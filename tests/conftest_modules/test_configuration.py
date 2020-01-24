

CONFIGURATION = {
    "version": "1",
    "credpath": "tests/assets/integration/credentials.yml",
    "name": "integration trail path",
    "short_description": "this is a sample with LIVE CREDS for integration",
    "long_description": "this is for testing against a live db",
    "threads": 15,
    "source": {
        "profile": "default",
        "default_sampling": {
            "databases": [
                {
                    "name": "SNOWSHU_DEVELOPMENT",
                    "schemas": [
                        {
                            "name": ".*",
                            "relations": [
                                "^(?!.+_VIEW).+$"
                            ]
                        }
                    ]
                }
            ]
        },
        "include_outliers": True,
        "sample_method": "bernoulli",
        "probability": 30,
        "specified_relations": [
            {
                "database": "SNOWSHU_DEVELOPMENT",
                "schema": "SOURCE_SYSTEM",
                "relation": "ORDERS",
                "unsampled": True
            },
            {
                "database": "SNOWSHU_DEVELOPMENT",
                "schema": "SOURCE_SYSTEM",
                "relation": "ORDER_ITEMS",
                "relationships": {
                    "bidirectional": [
                        {
                            "local_attribute": "PRODUCT_ID",
                            "database": "SNOWSHU_DEVELOPMENT",
                            "schema": "SOURCE_SYSTEM",
                            "relation": "PRODUCTS",
                            "remote_attribute": "ID"
                        }
                    ],
                    "directional": [
                        {
                            "local_attribute": "ORDER_ID",
                            "database": "SNOWSHU_DEVELOPMENT",
                            "schema": "SOURCE_SYSTEM",
                            "relation": "ORDERS",
                            "remote_attribute": "ID"
                        }
                    ]
                }
            }
        ]
    },
    "target": {
        "adapter": "postgres"
    },
    "storage": {
        "profile": "default"
    }
}
