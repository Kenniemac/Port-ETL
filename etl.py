import os
from datetime import datetime
#import re
import pandas as pd
from sqlalchemy import create_engine
import ast # Abstract syntax tree used for obtaining actual python objects from string
from dotenv import dotenv_values
dotenv_values()

def get_database_conn():
    # Get database credentials from environment variable
    config = dict(dotenv_values('.env'))
    db_user_name = config.get('DB_USER_NAME')
    db_password = config.get('DB_PASSWORD')
    db_name = config.get('DB_NAME')
    port = config.get('PORT')
    host = config.get('HOST')
    # Create and return a postgresql database connection object
    return create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@{host}:{port}/{db_name}')


# Data loading layer
def load_data():
    con = get_database_conn()
    data = pd.read_csv(f'data\Wpi Data.csv')
    data.to_sql('port_data', con = con, if_exists= 'replace', index= False)
    print('Data successfully written to PostgreSQL Database')


load_data()
