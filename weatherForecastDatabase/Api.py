import urllib

import matplotlib
import pandas as pd
import requests
import sqlalchemy
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.expand_frame_repr', False)

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

## wyswietlenie pogody dla danego miasta w parametrze
def display_forecast_for_station_name(df,station_name):
    station_data = df[df['name_station'] == station_name]
    if not station_data.empty:
        for index, row in station_data.iterrows():
            print(f"Station: {row['name_station']} ({row['id_station']}):")
            print(f"Temperature: {row['temperature']}°C")
            print(f"Wind Speed: {row['wind_speed']} m/s")
            print(f"Wind direction: {row['wind_direction']}°")
            print(f"Relative humidity: {row['relative_humidity']}%")
            print(f"Total rainfall: {row['total_rainfall']} mm")
            print(f"Atmospheric pressure: {row['atmospheric_pressure']} hPa")
            print(f"Date: {row['date']}")
            print(f"time: {row['time']}\n")

            print("WEATHER ALERTS: ")
            if (float(row['temperature']) < 0):
                print("Temperature is below zero!")
            if (float(row['temperature']) >28):
                    print("It's hot outside. Remember to hydrate.")
            if(float(row['wind_speed']) > 1):
                print("The wind speed is high. Remain careful.")
    else:
        print(f"No data found for station: {station_name}")


## wyswietlenie wykresu dla (w tym momencie tylko temperatury) dla danego miasta w parametrze.
def display_chart_with_historical_data(station_name):
    query = f"Select * from measurement where name_station = '{station_name}'"
    df = pd.read_sql_query(query,engine)

    df['atmospheric_pressure'] = df['atmospheric_pressure'].fillna(-1).astype(float)
    df['wind_speed'] = df['wind_speed'].fillna(0).astype(float)
    df['wind_direction'] = df['wind_direction'].astype(float)
    df['temperature'] = df['temperature'].astype(float)
    df['relative_humidity'] = df['relative_humidity'].astype(float)
    df['total_rainfall'] = df['total_rainfall'].astype(float)


    df['datetime'] = df['date'].astype(str) + '\n ' + df['time'].astype(str) + ":00"
    df = df.set_index('datetime')

    ##tworzenie wykresu
    plt.figure(figsize=(10, 6))
    plt.scatter(df.index, df['temperature'], label='Temperature', color='red')
    plt.xlabel('Date and Time')
    plt.ylabel('Temperature (°C)')
    plt.title(f'Temperature for {station_name}')
    plt.legend()
    plt.grid(True)

    plt.tight_layout()
    plt.show()


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

##ręczne wywyołanie metody
display_forecast_for_station_name(df_mapped,"Warszawa")

display_chart_with_historical_data("Kraków")





try:
    insert_data_to_db(df_mapped, 'Measurement', engine)
except:
    print("Current forecast is already stored in Database.")

## MUSIMY DODAC WYJATEK PODCZAS WSTAWIANIA DANYCH O TEJ SAMEJ GODZINIE STACJI I DACIE

