#!/usr/bin/python3
'''
Purpose: The contents of this file seek to meet the requirements of Assignment 1 in
         Projects in Programming and Data Science (Fall 2017).

Author:  Nick DeMasi
'''
import csv
import googlemaps
import os
import requests
import MySQLdb as mdb

from datetime import datetime
from urllib.error import URLError

# global API key vairables
MAPS_API_KEY = open('/home/ubuntu/jupyter/Student_Notebooks/assignment_1/GoogleMaps_API_Key.txt', 'r').read()
WEATHER_API_KEY = open('/home/ubuntu/jupyter/Student_Notebooks/assignment_1/DarkSky_API_Key.txt', 'r').read()


def check_table_rows(cnxn, dbname, table):
        '''Checks number of rows in a specified table'''
        cursor = cnxn.cursor()
        query = '''SELECT COUNT(*) FROM {db}.{table}'''.format(db=dbname, table=table)        
        cursor.execute(query)
        num_rows = cursor.fetchone()[0]
        cursor.close()

        return num_rows


def execute_sql_statement(cnxn, query, query_params=None):
    '''Wrapper for executing SQL queries'''
    cursor = cnxn.cursor()

    # check for query_params:
    if query_params:
        cursor.execute(query, query_params)
    else:
        cursor.execute(query)
    cnxn.commit()
    cursor.close()


def get_weather(location):
    '''Collects weather data at geographic coordinate using DarkSky API'''

    # unpack location tupel
    lat = location[0]
    lon = location[1]
    # format url for DarkSky API request
    url = 'https://api.darksky.net/forecast/{API_KEY}/{lat},{lon}'.format(API_KEY=WEATHER_API_KEY,
                                                                          lat=lat, lon=lon)

    # request data
    try:
        r = requests.get(url)
    except:
        return 0
    else:
        weather_json = r.json()['currently']

        # extract pertinent information from API into dictionary
        weather_dict = {
            'lat':        lat,
            'lon':        lon,
            'time':       datetime.fromtimestamp(weather_json['time']),
            'summary':    weather_json['summary'],
            'temp':       weather_json['temperature'],
            'rainProb':   weather_json['precipProbability'],
            'humidity':   weather_json['humidity'],
            'windSpeed':  weather_json['windSpeed'],
            'cloudCover': weather_json['cloudCover'],
            'visibility': weather_json['visibility']
        }

        return weather_dict


class USMountain(object):
    '''Mountain object populated with data from suite of Google
    Maps APIs'''
    def __init__(self, location):
        self.client = googlemaps.Client(MAPS_API_KEY)
        self.location = location
        self.lat = location[0]
        self.lon = location[1]
        self.__googleStateData()
        self.__googlePlaceData()
        self.__googleElevationData()

    def __googleStateData(self):
        '''Collects state data from Google Geocode API'''
        geocode_json = self.client.reverse_geocode(self.location)

        # assume first result
        geocode_data = geocode_json[0]
        # unpack results from json object returned by Geocode API
        for component in geocode_data['address_components']:
            if 'administrative_area_level_1' in component['types']:
                self.state = component['long_name']

    def __googlePlaceData(self):
        '''Collects place data from Google Places API'''
        places_json = self.client.places_nearby(location=self.location, radius=100,
                                                keyword='mountain', language='en-AU',
                                                type='nature_feature')

        # unpack results from json object returned by Places API
        places_data = places_json['results']

        # check if results were returned before variable assignment
        if places_data:
            # assume first result is correct
            result = places_data[0]
            # place  data in mountains dict
            self.name = result['name']
            self.place_id = result['place_id']
            # in case there is no rating set key to None
            try:
                self.rating = result['rating']
            except:
                self.rating = None
        else:
            # this code is for states SC and FL which do not reutrn anything with above params
            places_json = self.client.places_nearby(location=self.location, radius=500,
                                                    language='en-AU', type='point_of_interest')
            result = places_json['results'][0]
            # place  data in mountains dict
            self.name = result['name']
            self.place_id = result['place_id']
            # in case there is no rating set key to None
            try:
                self.rating = result['rating']
            except:
                self.rating = None

    def __googleElevationData(self):
        '''Collects elevation data from Google Elevation API'''
        elevation_data = self.client.elevation(self.location)

        # unpack results form list object reutrned by Elevation API
        result = elevation_data[0]
        self.elevation = result['elevation']


def main():
    '''Main scripting function for API data retrieval and SQL database
    insertion per assignment 1 of Projects in Programming and Data Science -
    Fall 2017'''

    # set-up parameters for local host
    params = {
        'SERVER': '34.226.52.95',    # whatever the local host is
        'UID':    'root',            # your username
        'PWD':    'dwdstudent2015'   # your password
    }
    # connect to MySQL db using pyodbc module
    cnxn = mdb.connect(params['SERVER'], params['UID'], params['PWD'],
                       charset='utf8', use_unicode=True)

    # ncreate peaks database if it does not exist
    dbname = 'peaks'
    create_peaks_database_query = '''CREATE DATABASE IF NOT EXISTS {db}'''.format(db=dbname)
    execute_sql_statement(cnxn, create_peaks_database_query)

    # create table for peak information if it doesn't exist
    create_peak_table_query = '''CREATE TABLE IF NOT EXISTS {db}.{table}
                                 (place_id varchar(255),
                                  name varchar(255),
                                  state varchar(255),
                                  lat float,
                                  lon float,
                                  elevation float,
                                  rating float,
                                  PRIMARY KEY(lat, lon)
                                  )'''.format(db=dbname, table='peaks_information')
    execute_sql_statement(cnxn, create_peak_table_query)

    # create table for weather information if it doesn't exist
    create_weather_table_query = '''CREATE TABLE IF NOT EXISTS {db}.{table}
                                    (lat float,
                                     lon float,
                                     time datetime,
                                     summary varchar(255),
                                     temp float,
                                     rainProb float,
                                     humidity float,
                                     windSpeed float, 
                                     cloudCover float,
                                     visibility float,
                                     PRIMARY KEY(lat, lon, time)
                                     )'''.format(db=dbname, table='peak_weather')
    execute_sql_statement(cnxn, create_weather_table_query)

    # iterate through longitude and latitude data from states.csv file
    with open('/home/ubuntu/jupyter/Student_Notebooks/assignment_1/states.csv', 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # unpack coordinates as a tuple
            location = (float(row['lat']), float(row['lon']))

            # only collect Google Maps data if first time running script
            num_rows = check_table_rows(cnxn, dbname, 'peaks_information')
            if num_rows < 50:
                # create USMountain object
                mountain = USMountain(location)

                # fill table with attributes of mountain object
                peak_info_query = '''INSERT IGNORE INTO {db}.{table}
                                     (place_id, name, state, lat, lon, elevation, rating)
                                     VALUES (%s, %s, %s, %s, %s, %s, %s)'''.format(db=dbname, table='peaks_information')
                query_params = (mountain.place_id, mountain.name, mountain.state, mountain.lat,
                                mountain.lon, mountain.elevation, mountain.rating)
                print(query_params)
                execute_sql_statement(cnxn, peak_info_query, query_params=query_params)

            # get weather data
            weather = get_weather(location)
            if weather == 0:
                raise URLError('DarkSky API is down')

            # fill table with weather data
            weather_info_query = '''INSERT IGNORE INTO {db}.{table}
                                    (lat, lon, time, summary, temp, rainProb, humidity, windSpeed, cloudCover, visibility)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''.format(db=dbname, table='peak_weather')
            query_params = (weather['lat'], weather['lon'], weather['time'], weather['summary'],
                            weather['temp'], weather['rainProb'], weather['humidity'],
                            weather['windSpeed'], weather['cloudCover'], weather['visibility'])
            print(query_params)
            execute_sql_statement(cnxn, weather_info_query, query_params=query_params)


if __name__ == '__main__':
    main()
