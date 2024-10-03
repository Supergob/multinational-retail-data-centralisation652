from sqlalchemy import inspect
import pandas as pd
import utilities
import requests
import tabula
class DataExtractor:
    @staticmethod
    def list_db_tables(engine):
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables
    @staticmethod
    def list_number_of_stores(stores_endpoint, headers):
        response = requests.get(stores_endpoint, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            return f"Error: {response.status_code}"
    @staticmethod
    def retrieve_stores_data(retrieve_stores_endpoint, headers):
        response = requests.get(retrieve_stores_endpoint, headers=headers)
        if response.status_code == 200:
            raw_stores_returned = response.json()
            stores_returned = pd.DataFrame(raw_stores_returned)
            return stores_returned
        else:
            if response.status_code != 200:
                return f"Error: {response.status_code}"
            
    def read_rds_table(self, engine, table_name):
        query = f"SELECT * FROM legacy_users"
        read_data = pd.read_sql_query(query, engine)
        return read_data
    @staticmethod
    def retrieve_pdf_data(link):
        data_to_convert =  tabula.read_pdf(link, pages = 'all')
        if isinstance(data_to_convert, list) and all(isinstance(df, pd.DataFrame) for df in data_to_convert):
            data_to_convert = pd.concat(data_to_convert, ignore_index= True)
        else:
            raise ValueError('Not everything in here is a list')
        return data_to_convert


if __name__== "__main__":
    engine = utilities.engine
    user_data_table = 'legacy_users'  # Replace with the actual table name for user data
    extractor = DataExtractor()
    user_data_df = extractor.read_rds_table(engine, user_data_table)
    gets = DataExtractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    #print(gets)
    #print(user_data_df)
    headers = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    retrieve_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{451}"
    #print(DataExtractor.list_number_of_stores(number_of_stores_endpoint,headers))
    print(DataExtractor.retrieve_stores_data(retrieve_stores_endpoint,headers))