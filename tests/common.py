from string import ascii_lowercase
from random import randrange

def rand_string(length:int)->str:
    result=list()
    for _ in range(length):
        char=ascii_lowercase[randrange(len(ascii_lowercase))]   
        result.append(char)
    return ''.join(result)

def query_equalize(query:str)->str:
    """removes whitespace/newline deltas from sql"""
    return ' '.join(query.replace('\n',' ').split())
    
