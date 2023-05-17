import requests
import pandas as pd
from sqlalchemy import create_engine

'''This is a basic ETL pipeline where the user can input a state and 
   then the pipeline filters for that states universites'''


#extracts data from a url as a JSON dict
def extract_data():
    """this will return a dictionary"""
    API_URL = "http://universities.hipolabs.com/search?country=United+States"
    data = requests.get(API_URL).json()
    return data

#transforms data into a pandas df then filters
def transform_data(data:dict, state):
    '''transforms data into desired structure'''
    df = pd.DataFrame(data)
    print(f'Total Number of universities from API {len(data)}')
    df = df[df['name'].str.contains(state)]
    print(f'Number of universities in {state} {len(df)}')
    df['domains'] = [','.join(map(str, domain)) for domain in df['domains']]
    df['web_pages'] = [','.join(map(str, web)) for web in df['web_pages']]
    df = df.reset_index(drop=True)
    return df[['domains', 'country', 'web_pages', 'name']]

#loads transformed data into a sqlite database
def load_data(df:pd.DataFrame):
    disk_engine = create_engine('sqlite:///my_lite_store.db')
    df.to_sql('missouri', disk_engine, if_exists='replace')

state = input('Pick a state ').capitalize()
data = extract_data()
df = transform_data(data, state)
load_data(df)


