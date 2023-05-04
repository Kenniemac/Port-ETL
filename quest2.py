import psycopg2
import pandas as pd
from math import radians, cos, sin, asin, sqrt
from sqlalchemy import create_engine
from dotenv import dotenv_values
dotenv_values()


# Connect to PostgreSQL database
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


def second_func():
    conn = get_database_conn()

    
    trans = conn.begin()

    # Execute the CREATE TABLE command
    conn.execute('''CREATE TABLE cargoc_table AS 
    SELECT "Wpi_country_code", count("Load_offload_wharves") 
    from port_data
    WHERE "Load_offload_wharves" = 'Y'
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 1
    ;''')


    
    # Commit the changes
    trans.commit()

    # Close the cursor and connection
    conn.close()
    # Close the cursor and connection

    
    
