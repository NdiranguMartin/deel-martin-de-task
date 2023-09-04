import requests
import logging
import os
from dotenv import load_dotenv
import pandas as pd
import db_ops, models, output_queries

load_dotenv()

# Configure logging
logs_dir = '.logs'
if not os.path.exists(logs_dir):
   os.makedirs(logs_dir)
logging.basicConfig(filename=".logs/api_requests.log", level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s")

def get_data_from_api(api_url: str, params: dict):
    """
    Make a GET request and return a requests item while logging the results (or errors) 
    """
    try:
        # Make an API request
        response = requests.get(api_url, params=params)

        # Check if the response status code is successful (200 OK)
        if response.status_code == 200:
            data = response.json()
            filtered_parameters = {k: v for k, v in params.items() if k != 'APIkey' }
            logging.info(f"Successfully retrieved data from {api_url} with parameters {filtered_parameters}")
            print(f"Successfully retrieved data from api")
            return data
        else:
            logging.error(f"Failed to retrieve data from {api_url}. Status code: {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred while making the API request: {str(e)}")
        return None

def extract_nested_keys(my_dict:dict, parent_key:str="", separator:str=".") -> dict:
    """
    traverses through the dict and nest the keys for dict values. Returns a dict with 
    the nested keys as the key and data type of the value as value.
    """
    nested_keys = {}
    for key, value in my_dict.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        if isinstance(value, dict):
            nested_keys.update(extract_nested_keys(value, new_key, separator))
        else:
            nested_keys[new_key] = type(value)
    return nested_keys

def extract_load():
    """
    extracts data from apifootball and loads it to the DB 
    """
    # get parameters required to piece up the url and make API request
    base_url = 'https://apiv3.apifootball.com/'
    date_ranges = [
        {"from": "2022-08-05", "to": "2022-11-11"},
        {"from": "2022-11-12", "to": "2023-05-29"},
    ]
    data = []
    for date_range in date_ranges:
        params = {
            "action": "get_events",
            "APIkey": os.getenv("API_KEY"),
            "from": date_range["from"],
            "to": date_range["to"],
            "league_id": 152,
        }
        single_call_data = get_data_from_api(base_url, params)
        data += single_call_data

    # normalize the data, unnest json
    df = pd.json_normalize(data)

    nested_keys = extract_nested_keys(data[0])

    all_dfs = {}
    all_dfs["matches"] = df[df.columns.intersection([k for k,v in nested_keys.items() if v != list ])]

    list_cols = [k.split(".") for k,v in nested_keys.items() if v == list]
    for record_path in list_cols:
        df_name = "_".join(record_path)
        df = pd.json_normalize(data, record_path=record_path, meta=["match_id"])
        all_dfs[df_name] = df

    schema = 'apifootball'
    db_ops.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for df_name, df in all_dfs.items():
        # replace empty strings with None to ensure postgres reads it as NULL
        df.replace('', None, inplace=True)

        cols_datatypes = {col: "VARCHAR(255)" for col in df.columns}
        db_ops.create_table(schema=schema, table=df_name, columns=cols_datatypes, drop_first=True)
        db_ops.df_to_table(df=df, schema=schema, table=df_name, incremental=True, unique_col='match_id')



    
def transform():
    """
    runs the queries that create fact and dimension tables
    """
    schema = 'analytics'
    db_ops.execute_sql(f"DROP SCHEMA {schema} CASCADE", message=f"schema {schema} dropped")
    db_ops.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {schema}", message=f"schema {schema} created")
    queries = models.model_queries()

    for table, query in queries.items():
        db_ops.execute_sql(f"CREATE TABLE IF NOT EXISTS {schema}.{table} AS ({query})", message=f"Table {table} created")


def analytics():
    """
    execute analytics queries and store output in a csv
    """
    # make data output directory
    output_dir = 'data'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    queries = output_queries.analytics_queries()
    for df_name, query in queries.items():
        #  run queries
        df = db_ops.execute_sql(query=query, has_results=True)

        output_file_path = f"{output_dir}/{df_name}.csv"
    
        # Write the DataFrame to a CSV file
        df.to_csv(output_file_path, index=False)
    
    print(f"queries run and csvs written in {output_dir} folder")