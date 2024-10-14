import numpy as np
import re
import pandas as pd 
from sqlalchemy import engine
import utilities # our shared utilities module
import data_extraction #Data extraction module
import database_utils
class DataCleaning: 
    @staticmethod
    def clean_user_data(user_data_df):
        
        df_to_clean = data_extraction.DataExtractor().read_rds_table(utilities.engine, user_data_df)
        dropped_rows = pd.DataFrame(df_to_clean).dropna(axis= 0, subset=['first_name', 'last_name'], how = 'any')
        dropped_rows = dropped_rows.dropna(axis= 1, how= 'any').drop_duplicates()
        dtformat = '%Y-%m-%d'
        dropped_rows['date_of_birth'] = pd.to_datetime(dropped_rows['date_of_birth'], errors='coerce', format=dtformat)
        filtered_country_codes = dropped_rows['country_code'].apply(lambda x: isinstance(x, str) and len(x) ==2 )        
        dropped_rows[dropped_rows['country_code'].isin(filtered_country_codes)].dropna()
        regex_expression = r'^(\(?\+?[0-9]*\)?)?[0-9_\- \(\)]*$'
        dropped_rows.loc[~dropped_rows['phone_number'].str.match(regex_expression), 'phone_number'] = np.nan
        dropped_rows = dropped_rows.dropna()
        dropped_rows['address'] = dropped_rows['address'].apply(lambda x: ''.join(c for c in x if c.isalnum()))
        dropped_rows['email_address'] = dropped_rows['email_address'].apply(lambda x: x.strip() if isinstance(x,str) else np.nan)
        dropped_rows['email_address'].dropna()
        cleaned_user_df = dropped_rows
        return cleaned_user_df
    @staticmethod
    def clean_card_data():
        card_df =data_extraction.DataExtractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
        dropped_na_card = card_df.dropna()
        dropped_duplicates = dropped_na_card.drop_duplicates()
        cc_regex = r'^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})$'
        dropped_duplicates['card_number'] = dropped_duplicates['card_number'].apply(lambda x: x if re.match(cc_regex, str(x)) else np.nan)
        cleaned_card_df = dropped_duplicates.dropna(subset=['card_number'])
        return cleaned_card_df
    @staticmethod
    def clean_store_data(num_stores, retrieve_stores_endpoint, headers):
        unclean_store_data = data_extraction.DataExtractor().retrieve_stores_data(num_stores, retrieve_stores_endpoint, headers)   # Pull the data 
        raw_data = pd.DataFrame(unclean_store_data)                                                                                # Convert to dataframe
        #print("Raw data", raw_data.info())                                                                                        # Debugging steps
        raw_data.drop_duplicates(inplace=True)                                                                                     # Removes duplicates
        #print("after dropping duplicates",raw_data.info())                                                                        # Debugging steps
        raw_data['opening_date'] = pd.to_datetime(raw_data['opening_date'], errors = 'coerce', format = '%Y-%m-%d')                # Convert dates to correct format
        raw_data['address'] = raw_data['address'].str.replace(r'^\s+|\s+$', '', regex=True)                                        # Replaces whitespaces while keeping spaces between house numbers and roads
        raw_data['locality'] = raw_data['locality'].str.replace(r'^\s+|\s+$', '', regex=True)                                      # Same as above
        address_pattern = re.compile(r'^[a-zA-Z0-9\s,.-]+$')                                                                       # Compiles regex for in filtering later  
        raw_data = raw_data[raw_data['address'].str.match(address_pattern)]                                                        # Filters through the adress only keeping what matches our compiled regex                          
        valid_continents= ["Africa", "Asia", "Europe","America", "North America", "South America", "Oceania", "Antarctica"]        # List of valid contintents 
        raw_data = raw_data[raw_data['continent'].isin(valid_continents)]                                                          # Filters through to keep only valid continents
        raw_data.drop('lat', axis=1, inplace = True)                                                                               # Removes 'lat' column 
        #print('Data after removing lat column',raw_data)                                                                          # Debugging step
        raw_data.dropna(inplace=True)                                                                                              # Drop missing data
        cleaned_store_data = raw_data.reset_index(drop=True)
        return cleaned_store_data
    @staticmethod
    def convert_product_weights(extracted_s3_data):
        weights = extracted_s3_data['weight']
        #print(weights)
        weights_in_kg = []
        for i in weights:
            if isinstance(i, float):                                            # If the value is a float, assume it's already in kg
                weights_in_kg.append(f"{i}kg")
            elif isinstance(i, str):                                            # If the value is a string, process it
                                                                                # Clean up the string to extract the numeric value
                match = re.findall(r'\d+\.?\d*', i)
                if match:
                    value_of_weights = float(match[0])  
                
                if 'kg' in i:
                    weights_in_kg.append(f"{value_of_weights}kg")
                elif 'g' in i:
                    weights_in_kg.append(f"{value_of_weights / 1000}kg")
                elif 'ml' in i:
                    weights_in_kg.append(f"{value_of_weights / 1000}kg")
                else:
                    weights_in_kg.append(np.nan)
        
        extracted_s3_data['weight'] = weights_in_kg
        extracted_s3_data.dropna(subset = ['weight'], inplace = True)
        cleaned_product_weights = extracted_s3_data
        return cleaned_product_weights
          
    @staticmethod
    def clean_products_data(extracted_s3_data):
        raw_data = DataCleaning.convert_product_weights(extracted_s3_data)
        raw_data['date_added'] = pd.to_datetime(raw_data['date_added'], errors = 'coerce', format = '%Y-%m-%d')
        print('number of rows left ', raw_data.info())
        raw_data.dropna(inplace = True)
        print('number after dropping na ', raw_data.info())
        raw_data.drop_duplicates(inplace = True)
       
        raw_data.drop('Unnamed: 0', axis = 1 , inplace = True)
        cleaned_products_data = raw_data
        return cleaned_products_data
                                                                                                                                            
            
if __name__ == "__main__":
   
    engine = utilities.engine                                                                                                                   # INITIALIZE THE ENGINE USING UTILS MODULE
    user_data_table = 'legacy_users'                                                                                                            # TABLE NAME TO EXTRACT USER DATA
    extractor = data_extraction.DataExtractor()                                                                                                 # EXTRACTOR INSTANCE
    user_data_df = extractor.read_rds_table(engine, user_data_table)                                                                            # READ DATA FROM THE SPECIFIED TABLE
    
    # Upload the cleaned user details
    
    # cleaned__user_df = DataCleaning.clean_user_data(user_data_df)                                                                               # CLEAN THE EXTRACTED DATA
    # db_connector = database_utils.DatabaseConnector()        
    # db_connector.upload_to_db(cleaned__user_df, 'dim_users','sales_db_creds.yaml')
    # print("Cleaned user data uploaded successfully!")
    
    # # Upload the cleaned card details
    
    # cleaned_card_df = DataCleaning.clean_card_data()
    # db_connector = database_utils.DatabaseConnector()
    # db_connector.upload_to_db(cleaned_card_df, 'dim_card_details', 'sales_db_creds.yaml')
    # print('Cleaned card details uploaded succesfully!')
    
    # # Upload the cleaned store details

    # headers = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    # number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    # retrieve_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
    # number_of_stores = 450
    # cleaned_store_df = DataCleaning.clean_store_data(number_of_stores, retrieve_stores_endpoint, headers)         
    # db_connector = database_utils.DatabaseConnector()
    # db_connector.upload_to_db(cleaned_store_df, 'dim_store_details','sales_db_creds.yaml')
    # print('Cleaned store details uploaded succesfully!')

    # # Upload the cleaned products data
    
    address = 's3://data-handling-public/products.csv'
    extracted_s3_data = data_extraction.DataExtractor.extract_from_s3(address)
    cleaned_product_data = DataCleaning.clean_products_data(extracted_s3_data)
    db_connector = database_utils.DatabaseConnector()
    db_connector.upload_to_db(cleaned_product_data, 'dim_products','sales_db_creds.yaml')
    print('Cleaned product details uploaded succesfully')
    
    
    #print(DataCleaning.clean_store_data(number_of_stores, retrieve_stores_endpoint, headers))