from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote_plus

ACCOUNT = 'account'
DATABASE = 'database'
HOST = 'host'
PASSWORD = 'password'
PORT = 'port'
ROLE = 'role'
SCHEMA = 'schema'
USER = 'user'
WAREHOUSE = 'warehouse'
CONFIG_FILE_PATH= 'config_file_path'

@dataclass
class Credentials:
    """Represents every accepted type of credential, attempting to bring some
    sanity to how configs are defined for disparate adapters."""
    account: Optional[str] = None
    database: Optional[str] = None
    host: Optional[str] = None
    password: Optional[str] = None
    port: Optional[int] = None
    role: Optional[str] = None
    schema: Optional[str] = None
    user: Optional[str] = None
    warehouse: Optional[str] = None
    config_file_path: Optional[str] = None # for interfaces like ecr where the user likely has a complex config file
    

    def urlencode(self) -> None:
        """quote-plus encoding of all attributes good for sql urls."""
        for key in vars(self).keys():
            if isinstance(self.__dict__[key], str):
                self.__dict__[key] = quote_plus(self.__dict__[key])
