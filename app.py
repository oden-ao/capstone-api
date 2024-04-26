#Libraries#
from flask import Flask, request
import sqlite3
import requests
from tqdm import tqdm
import json 
import numpy as np
import pandas as pd



app = Flask(__name__) 


#Routes#
@app.route('/')
@app.route('/home')
def home():
    return 'Nothing here yet!'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips= get_all_trips(conn)
    return trips.to_json()

@app.route('/stations/<station_id>') #dynamic route
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

@app.route('/trips/<trip_id>')
def route_trips_id(trip_id):
    conn= make_connection()
    trip = get_trip_id(trip_id, conn)
    return trip.to_json()

# parse and transform incoming data into a tuple as we need 
@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

@app.route('/trips/add', methods=['POST']) 
def route_add_trips():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

@app.route('/json', methods=['POST']) 
def json_example():
    
    req = request.get_json(force=True) # Parse the incoming json data as Dictionary
    
    name = req['name']
    age = req['age']
    address = req['address']
    
    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

#mine
@app.route('/stations/popular') 
def route_most_popular_stations():
    conn= make_connection()
    most_popular = get_most_popular_start(conn)
    return most_popular.to_json()

@app.route('/trips/total_duration/<bike_id>') 
def route_bike_duration(bike_id):
    conn= make_connection()
    total_duration = get_total_duration(bike_id, conn)
    return total_duration.to_json()

@app.route('/subscriberdata', methods=['POST']) 
def route_subscriber_data():
    input_data = request.get_json()
    subscriber = input_data['Type']
    conn = make_connection()
    result = get_subscriber_data(subscriber, conn)
    return result.to_json()

#Functions# 
def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id IS {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def get_all_trips(conn):
    query= f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def get_trip_id(trip_id, conn):
    query= f"""SELECT * FROM trips WHERE id IS {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

#my own
def get_most_popular_start(conn):
    query = f"""SELECT start_station_name AS StationName, COUNT(start_station_id) AS TotalVisits
            FROM trips GROUP BY start_station_name ORDER BY COUNT(start_station_id) DESC
            LIMIT 10"""
    result = pd.read_sql_query(query, conn)
    return result

def get_total_duration(bike_id, conn):
    query = f"""SELECT SUM(duration_minutes) AS TotalDuration
    FROM trips WHERE bikeid IS {bike_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def get_subscriber_data(data, conn):
    query = f"""SELECT trips.start_station_id AS StationId, trips.start_station_name AS StationName, 
    stations.address AS Address, AVG(duration_minutes) AS AverageDuration, SUM(duration_minutes) AS TotalDuration,
    COUNT(subscriber_type LIKE ('%{data}%')) AS TripCount
    FROM trips
    LEFT JOIN stations
    ON trips.start_station_id = stations.station_id
    WHERE subscriber_type LIKE ('%{data}%')
    GROUP BY start_station_id """
    result = pd.read_sql_query(query, conn)
    return result

#connection function
def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

if __name__ == '__main__':
    app.run(debug=True, port=5000)

