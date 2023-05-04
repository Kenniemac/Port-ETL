
import psycopg2
from math import radians, cos, sin, asin, sqrt
from sqlalchemy import create_engine
from dotenv import dotenv_values
dotenv_values()

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # Convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # Haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r * 1000

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


def first_func():
    conn = get_database_conn()
    # Define the latitude and longitude of the JURONG ISLAND  and country='SG'
    given_port_lat = 1
    given_port_lon = 144

    # Query the database for all ports and their latitude/longitude
    cur = conn.cursor()
    cur.execute("SELECT Main_port_name, Latitude_degrees, Longitude_degrees FROM ports")
    ports = cur.fetchall()

    # Calculate the distance between the given port and all other ports
    distances = []
    for port in ports:
        Main_port_name, Latitude_degrees, Longitude_degrees = port
        distance = haversine(given_port_lon, given_port_lat, Longitude_degrees, Latitude_degrees)
        distances.append((Main_port_name, distance))

    # Sort the distances in ascending order
    distances.sort(key=lambda x: x[1])

    # Print the five closest ports and their distances in meters
    for port, distance in distances[1:6]:
        print(f"{port}: {distance:.2f} meters")

    # Create a table in the PostgreSQL database to store the results
    cur.execute("""
        CREATE TABLE closest_ports (
            port_name VARCHAR(255),
            distance_meters FLOAT
        )
    """)

    # Insert the closest ports and their distances into the table
    for port, distance in distances[1:6]:
        cur.execute("INSERT INTO closest_ports VALUES (%s, %s)", (port, distance))

    # Commit the changes to the database and close the cursor and connection
    conn.commit()
    cur.close()
    conn.close()

first_func()
