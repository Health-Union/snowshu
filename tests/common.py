from string import ascii_lowercase
from random import randrange
from datetime import datetime,timedelta
from random import randrange

def rand_string(length: int) -> str:
    result = list()
    for _ in range(length):
        char = ascii_lowercase[randrange(len(ascii_lowercase))]
        result.append(char)
    return ''.join(result)

def random_date():
    window = datetime(2020,1,1) - datetime(1990,1,1)
    return datetime(1990,1,1) + timedelta(seconds=(randrange(window.days * 24 * 60 * 60)))


def query_equalize(query: str) -> str:
    """removes whitespace/newline deltas from sql"""
    return ' '.join(query.replace('\n', ' ').split())
