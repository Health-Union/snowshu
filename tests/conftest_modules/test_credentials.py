from io import StringIO
import yaml

CREDENTIALS = {
    "version": "1",
    "sources": [
        {
            "name": "default",
            "adapter": "snowflake",
            "account": "nwa1992.us-east-1",
            "password": "P@$$w0rD!",
            "database": "SNOWSHU_DEVELOPMENT",
            "user": "hanzgreuber"
        }
    ],
    "targets": [
        {
            "name": "default",
            "adapter": "postgres",
            "host": "localhost",
            "password": "postgres",
            "port": "5432",
            "user": "postgres"
        }
    ],
    "storages": [
        {
            "name": "default",
            "adapter": "aws-ecr",
            "access_key": "aosufipaufp",
            "access_key_id": "aiosfuaoifuafuiosf",
            "account": "sasquach.us-east-1"
        }
    ]
}

def credentials_as_dict():
    return CREDENTIALS

def credentials_as_file_obj():
    return StringIO(yaml.dump(CONFIGURATION))
