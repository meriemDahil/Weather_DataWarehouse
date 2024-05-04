import numpy as np
import pandas as pd
from sqlalchemy import create_engine

# MySQL connection parameters
host = 'localhost'
user = 'root'
database = 'testdatabase'

# Establish a connection to your MySQL database using SQLAlchemy
engine = create_engine(f"mysql+mysqlconnector://{user}@{host}/{database}")

# Read the CSV file into a DataFrame
dataframes = pd.read_csv('Weather_1920-1959_MOROCCO.csv') 

# Corrected drop statement
columns_to_drop = ['WSFG', 'WSFG_ATTRIBUTES', 'WT01', 'WT01_ATTRIBUTES', 'WT03', 'WT03_ATTRIBUTES', 'WT05', 'WT05_ATTRIBUTES', 'WT07', 'WT07_ATTRIBUTES', 'WT08', 'WT08_ATTRIBUTES', 'WT09', 'WT09_ATTRIBUTES', 'WT16', 'WT16_ATTRIBUTES']
dataframes.drop(columns=columns_to_drop, inplace=True)

# Clean data
dataframes['STATION'] = dataframes['STATION'].str.strip()
dataframes['NAME'] = dataframes['NAME'].str.strip()
dataframes['STATION'].fillna('', inplace=True)
dataframes['NAME'].fillna('', inplace=True)
dataframes['STATION'] = dataframes['STATION'].astype(str)
dataframes['NAME'] = dataframes['NAME'].astype(str)
dataframes['DATE'] = pd.to_datetime(dataframes['DATE'])

# Create Dimension Temporelle DataFrame
dimension_temporelle = dataframes[['DATE']].copy()

dimension_temporelle.loc[:, 'mois'] = dimension_temporelle['DATE'].dt.month
dimension_temporelle.loc[:, 'année'] = dimension_temporelle['DATE'].dt.year
dimension_temporelle.loc[:, 'saison'] = ''  # You can define the season based on the month if needed
dimension_temporelle.loc[:, 'trimestre'] = dimension_temporelle['DATE'].dt.quarter

# Write DataFrame to MySQL database for Dimension Temporelle
dimension_temporelle.to_sql('dimension_temporelle', engine, if_exists='append', index=False)

# Create Dimension Géographique DataFrame
dimension_geographique = dataframes[['LATITUDE', 'LONGITUDE', 'ELEVATION']].copy()


# Write DataFrame to MySQL database for Dimension Géographique
dimension_geographique.to_sql('dimension_geographique', engine, if_exists='append', index=False)

# Create Dimension Station DataFrame
dimension_station = dataframes[['STATION', 'NAME']].copy()

# Write DataFrame to MySQL database for Dimension Station
dimension_station.to_sql('dimension_station', engine, if_exists='append', index=False)

# Create Table des Faits MesuresMétéorologiques DataFrame
table_des_faits = dataframes[['DATE', 'LATITUDE', 'LONGITUDE', 'ELEVATION', 'STATION', 'NAME', 'PRCP', 'TMAX', 'TMIN']].copy()


# Write DataFrame to MySQL database for Table des Faits MesuresMétéorologiques
table_des_faits.to_sql('mesuresmeteorologiques', engine, if_exists='append', index=False)

# Close the connection
engine.dispose()
