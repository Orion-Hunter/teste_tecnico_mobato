import datetime
import logging
import os
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from pandas_gbq import to_gbq
from concurrent.futures import ThreadPoolExecutor


class ETLService:

    def __init__(self):
        load_dotenv()
        self._credentials = service_account.Credentials.from_service_account_file(
            os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
        self._client = bigquery.Client(credentials=self._credentials, project=self._credentials.project_id)

    def read_data(self):
        data = pd.read_csv(r"./sources/dataset.csv", index_col='id')
        data['last_update'] = datetime.datetime.now()
        return data

    def transform_to_silver_layer(self):
        data = self.read_data().reset_index()
        ## Fill Null values
        data = data.fillna({
            'fuel': 'other',
            'manufacturer': 'other',
            'model': 'not informed',
            'condition': 'not informed',
            'title_status': 'not informed',
            'transmission': 'other',
            'VIN': '-',
            'drive': 'not informed',
            'size': 'not informed',
            'type': 'other',
            'paint_color': 'not informed',
            'county': '-'
        })

        ## Cast vars to correct type
        data['id'] = data['id'].astype('str')
        data['price'] = data['price'].astype('float64')
        data['url'] = data['url'].astype('str')
        data['region'] = data['region'].astype('str')
        data['region_url'] = data['region_url'].astype('str')
        data['fuel'] = data['fuel'].astype('str')
        data['manufacturer'] = data['manufacturer'].astype('str')
        data['model'] = data['model'].astype('str')
        data['condition'] = data['condition'].astype('str')
        data['cylinders'] = data['cylinders'].astype('str')
        data['title_status'] = data['title_status'].astype('str')
        data['transmission'] = data['transmission'].astype('str')
        data['VIN'] = data['VIN'].astype('str')
        data['drive'] = data['drive'].astype('str')
        data['size'] = data['size'].astype('str')
        data['type'] = data['type'].astype('str')
        data['paint_color'] = data['paint_color'].astype('str')
        data['image_url'] = data['image_url'].astype('str')
        data['description'] = data['description'].astype('str')
        data['posting_date'] = pd.to_datetime(data['posting_date'])

        return data

    async def run_query(self, query):
        job = self._client.query(query)
        job.result()
        logging.log(logging.INFO, f"Executado: {query} in Gold Layer")

    async def create_tables_in_gold_layer(self):
        query_average_mileage_by_fuel_type = '''
         CREATE OR REPLACE TABLE `teste-tecnico-449001.gold_layer.avg_mileage_by_fuel_type` AS
        SELECT
        fuel,
        round(AVG(odometer), 2) average_mileage
        FROM `teste-tecnico-449001.silver_layer.used_cars`
        group by 
        fuel
        '''
        query_average_price_by_manufacturer = '''
               CREATE OR REPLACE TABLE `teste-tecnico-449001.gold_layer.avg_price_by_manufacturer` AS
SELECT
manufacturer,
count(*) ads,
round(AVG(price), 2) average_mileage
FROM `teste-tecnico-449001.silver_layer.used_cars`
group by 
manufacturer
order by 2 desc
limit 5
                '''
        query_localization = '''
            CREATE OR REPLACE TABLE `teste-tecnico-449001.gold_layer.localization` AS
SELECT
lat,
long,
count(*) quantity
FROM `teste-tecnico-449001.silver_layer.used_cars`
group by lat,long
        '''
        query_price_by_mileage = '''
           CREATE OR REPLACE TABLE `teste-tecnico-449001.gold_layer.price_by_mileage` AS
SELECT
price,
odometer mileage
FROM `teste-tecnico-449001.silver_layer.used_cars`

        '''
        prices_for_year = '''
            CREATE OR REPLACE TABLE `teste-tecnico-449001.gold_layer.prices_for_year` AS
SELECT
price,
year
FROM `teste-tecnico-449001.silver_layer.used_cars`

        '''

        queries = [query_average_mileage_by_fuel_type, query_average_price_by_manufacturer,
                   query_localization, query_price_by_mileage, prices_for_year]

        with ThreadPoolExecutor() as executor:
            executor.map(self.run_query, queries)

    async def load_data_in_bigquery(self, data: pd.DataFrame, table_id: str):
        try:
            to_gbq(data, table_id, project_id=self._credentials.project_id, credentials=self._credentials,
                   if_exists="replace")
            return f"Enviado {len(data)} linhas para a tabela {table_id}."
        except Exception as e:
            return e
