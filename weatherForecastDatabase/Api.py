import urllib

import matplotlib
import pandas as pd
import requests
import sqlalchemy
from sqlalchemy import create_engine

## fetching data from website to program
def fetch_json_data(url):
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data);
    for record in data:
        print(record)
    return df


##changing columns names
def map_json_to_db_columns(df, mapping):
    df = df.rename(columns=mapping)
    return df

##DO ZMIANY!!
def process_data(df_mapped):
    df_mapped['atmospheric_pressure'] = df_mapped['atmospheric_pressure'].fillna(-1).astype(float)
    df_mapped['wind_speed'] = df_mapped['wind_speed'].fillna(0)
    df_mapped['wind_direction'] = df_mapped['wind_direction'].fillna(-1)
    return df_mapped


## TRZEBA SKMINIC
def insert_data_to_db(df, table_name, engine):
    df.to_sql(table_name, engine, if_exists='append',index=False)


mapping = {
    'id_stacji': 'id_station',
    'stacja': 'name_station',
    'data_pomiaru': 'date',
    'godzina_pomiaru': 'time',
    'temperatura': 'temperature',
    'predkosc_wiatru': 'wind_speed',
    'kierunek_wiatru': 'wind_direction',
    'wilgotnosc_wzgledna': 'relative_humidity',
    'suma_opadu': 'total_rainfall',
    'cisnienie': 'atmospheric_pressure'
}

connection_string = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};SERVER=db-mssql16.pjwstk.edu.pl;DATABASE=2019SBD;Trusted_Connection=yes;")

C2 = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER=db-mssql16.pjwstk.edu.pl;DATABASE=2019SBD;Trusted_Connection=yes;'

##creating a connecttion to a database
engine = create_engine(connection_string)

##query = "Select * from Measurement"
##df = pd.read_sql(query, engine)
##print(df)

# fetching data from API
df_from_api = fetch_json_data("https://danepubliczne.imgw.pl/api/data/synop/")
df_mapped = map_json_to_db_columns(df_from_api, mapping)
df_mapped = process_data(df_mapped)

print(df_mapped)

insert_data_to_db(df_mapped, 'Measurement', engine)

## MUSIMY DODAC WYJATEK PODCZAS WSTAWIANIA DANYCH O TEJ SAMEJ GODZINIE STACJI I DACIE
##moj kom