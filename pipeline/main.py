import asyncio
import logging

import pandas as pd
from dotenv import load_dotenv

from pipeline.etl_service import ETLService

logging.basicConfig(
    level=logging.INFO,  # Define o nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Define o formato da mensagem
    datefmt="%Y-%m-%d %H:%M:%S",  # Formato da data
)


class App:

    def __init__(self):
        load_dotenv()
        self._service = ETLService()

    async def load_data_in_bronze_layer(self):
        data = self._service.read_data()
        res = await self._service.load_data_in_bigquery(data, "teste-tecnico-449001.bronze_layer.used_cars")
        return res

    async def load_data_in_silver_layer(self, data: pd.DataFrame):
        res = await self._service.load_data_in_bigquery(data, "teste-tecnico-449001.silver_layer.used_cars")
        return res

    async def execute(self):
        bronze_layer = await self.load_data_in_bronze_layer()

        if isinstance(bronze_layer, str):
            logging.info(bronze_layer)
        elif isinstance(bronze_layer, Exception):
            logging.error(bronze_layer)

        silver_data = self._service.transform_to_silver_layer()
        silver_layer = await self._service.load_data_in_bigquery(silver_data,
                                                                 'teste-tecnico-449001.silver_layer.used_cars')
        if isinstance(silver_layer, str):
            logging.info(silver_layer)
        elif isinstance(silver_layer, Exception):
            logging.error(silver_layer)
            return

        gold_layer = await self._service.create_tables_in_gold_layer()

        if isinstance(gold_layer, None):
            logging.info('Gold Layer Data Available!')
        else:
            logging.error(gold_layer)


if __name__ == '__main__':
    asyncio.run(App().execute())
