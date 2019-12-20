from snowshu.source_adapters import BaseSourceAdapter
from concurrent.futures import ThreadPoolExecutor
from snowshu.logger import Logger
logger=Logger().logger
class Catalog:

    catalog:list=list()   

    def __init__(self,adapter:BaseSourceAdapter, threads:int=4):
        self.adapter=adapter
        self.threads=threads

    def load_full_catalog(self)->None:
            
        def accumulate_relations(db,accumulator):
            accumulator+=self.adapter.get_relations_from_database(db)
        
        catalog=list()
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            for database in self.adapter.get_all_databases():
                executor.submit(accumulate_relations,database,catalog)

        self.catalog=tuple(catalog)
