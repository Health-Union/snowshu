from typing import Type
from snowshu.adapters.source_adapters import BaseSourceAdapter
from concurrent.futures import ThreadPoolExecutor
import time
from snowshu.logger import Logger, duration
logger = Logger().logger


class Catalog:

    @staticmethod
    def load_full_catalog(adapter:Type['BaseSourceAdapter'],
                          threads:int = 4) -> None:
        catalog = list()
        def accumulate_relations(db, accumulator):
            try:
                accumulator += adapter.get_relations_from_database(db)
            except Exception as e:
                logger.critical(e)
                raise e

        catalog = list()
        logger.info('Assessing full catalog...')
        start_timer = time.time()
        with ThreadPoolExecutor(max_workers=20) as executor:
            {executor.submit(accumulate_relations, database, catalog)
             for database in adapter.get_all_databases()}

        logger.info(
            f'Done assessing catalog. Found a total of {len(catalog)} relations from the source in {duration(start_timer)}.')
        return tuple(catalog)
