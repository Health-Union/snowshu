from dataclasses import dataclass
from typing import Optional


USER='user'
PASSWORD='password'
HOST='host'
ACCOUNT='account'
DATABASE='database'
SCHEMA='schema'
ROLE='role'
WAREHOUSE='warehouse'


@dataclass
class Credentials:
    """ Represents every accepted type of credential,
        attempting to bring some sanity to how configs are defined for disparate adapters.
        
    """
    user:Optional[str]=None
    password:Optional[str]=None
    host:Optional[str]=None
    account:Optional[str]=None
    database:Optional[str]=None
    schema:Optional[str]=None
    role:Optional[str]=None
    warehouse:Optional[str]=None       

    
    
