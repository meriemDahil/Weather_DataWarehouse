import pandas as pd
from sqlalchemy import create_engine, text

# Read the CSV file into a Pandas DataFrame
dataframes = pd.read_csv('Weather_1960-1989_MOROCCO.csv', low_memory=False)

# Drop unused columns
columns_to_keep = ["STATION", "NAME", "LATITUDE", "LONGITUDE", "ELEVATION", "DATE", "PRCP", "PRCP_ATTRIBUTES", "TAVG", "TAVG_ATTRIBUTES", "TMAX", "TMAX_ATTRIBUTES", "TMIN", "TMIN_ATTRIBUTES"]

dataframes = dataframes[columns_to_keep]
dataframes['STATION'] = dataframes['STATION'].str.strip()
dataframes['NAME'] = dataframes['NAME'].str.strip()
dataframes['STATION'].fillna('', inplace=True)
dataframes['NAME'].fillna('', inplace=True)
dataframes['STATION'] = dataframes['STATION'].astype(str)
dataframes['NAME'] = dataframes['NAME'].astype(str)
dataframes['DATE'] = pd.to_datetime(dataframes['DATE'])

# Connect to MySQL database using SQLAlchemy
# MySQL connection parameters
host = 'localhost'
user = 'root'
database = 'testdatabase'

# Establish a connection to your MySQL database using SQLAlchemy
engine = create_engine(f"mysql+mysqlconnector://{user}@{host}/{database}")
print(dataframes.dtypes)

# Define table schemas
star_schema_modified = {
    'Dimension Temporelle': {
        'id_date': 'INT PRIMARY KEY AUTO_INCREMENT',
        'date': 'DATE',
        'mois': 'VARCHAR(20)',
        'année': 'INT',
        'saison': 'VARCHAR(20)',
        'trimestre': 'VARCHAR(20)'
    },
    'Dimension Géographique': {
        'id_geographie': 'INT PRIMARY KEY AUTO_INCREMENT',
        'pays': 'VARCHAR(50)',
        'ville': 'VARCHAR(50)',
        'latitude': 'FLOAT',
        'longitude': 'FLOAT',
        'elevation': 'INT'
    },
    'Dimension Station': {
        'id_station': 'INT PRIMARY KEY AUTO_INCREMENT',
        'code_station': 'VARCHAR(50)',
        'nom_station': 'VARCHAR(50)'
    },
    'Table des Faits MesuresMétéorologiques': {
        'ID_Mesure': 'INT PRIMARY KEY AUTO_INCREMENT',
        'id_date': 'INT',
        'id_geographie': 'INT',
        'id_station': 'INT',
        'precipitation': 'FLOAT',
        'epaisseur_neige': 'FLOAT',
        'chute_neige': 'FLOAT',
        'temperature_moyenne': 'FLOAT',
        'temperature_max': 'FLOAT',
        'temperature_min': 'FLOAT',
        'direction_vent_max': 'FLOAT',
        'heure_pointe_rafale': 'FLOAT',
        'vitesse_vent_max': 'FLOAT',
        
    }
}

# Create tables if they don't exist with modified schema
with engine.connect() as conn:
    # Begin a transaction
    with conn.begin():
        for table_name, columns in star_schema_modified.items():
            column_defs = ', '.join([f'{col} {dtype}' for col, dtype in columns.items()])
            sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({column_defs})"  # Enclose table name in backticks
            conn.execute(text(sql))

# Load data into tables
dataframes.to_sql('Table des Faits MesuresMétéorologiques', engine, if_exists='append', index=False)

# Close the engine
engine.dispose()
