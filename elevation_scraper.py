'''
Purpose: This script was used to scrape the geographic coordinates (latitude, longitdue)
         of the highest point in each of the 50 states. The source of information was a Wikipedia
         article titled "List of U.S. states by elevation" and the data is stored in the states.csv file.
         
Author:  Nick DeMasi
'''
import csv
import logging
import re
import requests

from bs4 import BeautifulSoup

LOG_FORMAT = "%(levelname)s %(asctime)s: %(message)s"


def geo_tools(href):
    '''only returns hrefs which mathc the regex r"^//tools"'''
    return href and re.compile(r"^//tools").search(href)


def scrape_elevations():
    '''scrape the latitiude and longitude of the highest pint in
    all 50 states'''

    # navigate to wikipedia page of state elevations
    r = requests.get('https://en.wikipedia.org/wiki/List_of_U.S._states_by_elevation')
    soup = BeautifulSoup(r.text, 'html.parser')

    # collect data from the first table
    table = soup.find_all('table', class_='wikitable')[0]
    rows = table.find_all('tr')
    geo = []

    # iterate through table skipping header row
    for row in rows[1:]:
        # create temporary dictionary to store information
        temp_dict = {}

        # collect state name and link to highes point
        h_point = row.find_all('td')

        # skip "United States" and "District of Columbia" entries
        skip = ["District of Columbia", "United States"]
        state = h_point[0].string
        if state in skip:
            continue

        # navigate to page of highest point
        h_point_link = h_point[1].a['href']
        h_point_r = requests.get('http://en.wikipedia.org'+h_point_link)
        h_point_soup = BeautifulSoup(h_point_r.text, 'html.parser')

        # collect lat and lon in decimal form
        try:
            logger.debug(h_point_soup.find(href=geo_tools))
            temp_r = requests.get('http:'+h_point_soup.find(href=geo_tools)['href'])
        except:
            temp_dict['lat'] = None
            temp_dict['lon'] = None
            continue
        else:
            temp_soup = BeautifulSoup(temp_r.text, 'html.parser')
            temp_dict['lat'] = temp_soup.find("span", class_='latitude').string
            logger.debug(temp_dict['lat'])
            temp_dict['lon'] = temp_soup.find("span", class_='longitude').string
            logger.debug(temp_dict['lon'])
            # append to geo list for csv conversion
            geo.append(temp_dict)

    # write data to csv from geo list of dictionaries
    with open('states.csv', 'w', encoding='utf-8') as file:
        fields = ['lat', 'lon']
        writer = csv.DictWriter(file, fieldnames=fields, lineterminator='\n')
        writer.writeheader()
        writer.writerows(geo)


if __name__ == '__main__':
    # set up file handler for log
    fh = logging.FileHandler(filename='states.log', mode='w', encoding='UTF-8')
    # set up log format
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(LOG_FORMAT))
    # get logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    # add a handlers
    logger.addHandler(fh)
    logger.addHandler(ch)
    # call function
    scrape_elevations()
