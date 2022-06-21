from netCDF4 import Dataset
import numpy as np
import pandas as pd
from datetime import timedelta
import os
import csv
# [pr, time, lat, lon]
# time = days since Jan 1, 1850

def add_days_to_date(date, days):
    # Calculates and returns a date by adding days to given date
    added_date = pd.to_datetime(date) + timedelta(days=days)
    added_date = added_date.strftime("%Y-%m-%d")

    return added_date

ncDir = 'Downloaded Files'
outDir = 'Output Files'

if not os.path.exists(outDir):
    # create output directory
    os.mkdir(outDir)
    print('[output directory created]')



# create and populate data structure for locations csv data
locations = pd.read_csv('locations_sub.csv', names=['Lat', 'Lon', 'Name'])

for inFileName in os.listdir(ncDir): # iterate through all netCDF files
    inFilePath = os.path.join(ncDir, inFileName)
    # create new file and data structure to output to

    outDF = pd.DataFrame()
    year = pd.DataFrame()
    doy = pd.DataFrame()
    date = pd.DataFrame()

    outFileName =  str(year) + 'pr.csv' # TODO: replace 'pr' with var variable
    outFilePath = os.path.join(outDir, outFileName)

    # read .nc file
    data = Dataset(inFilePath, 'r')
    pr = data.variables['pr']
    baseDate = data.variables['time'].units[11:21]

    # -------------------------------------------------------------------
    # add each date to date DataFrame
    utilList = [] # TODO: turn utilList into a Series
    for dayNum in data.variables['time']:
        d = add_days_to_date(baseDate, int(dayNum))
        utilList.append(d)
    date = pd.concat([date.reset_index(drop=True), pd.Series(utilList)], axis=1, ignore_index=False)
    utilList.clear()
    # -------------------------------------------------------------------
    # add each year to year Series
    #utilList.append('YEAR')
    for row in date[0]:
        # split current day date and get year
        y = row.split('-')[0]
        utilList.append(y)
    year = pd.concat([year.reset_index(drop=True), pd.Series(utilList)], axis=1, ignore_index=False)
    utilList.clear()
    # -------------------------------------------------------------------
    # generate doy
    utilList = pd.Series(range(1, len(year) + 1))
    doy = pd.concat([doy.reset_index(drop=True), pd.Series(utilList).reset_index(drop=True)], axis=0, ignore_index=False)
    doy.rename(columns={doy.columns[0] : 'DOY'}, inplace=True)
    print(doy)

    for loc in locations: # iterate through locations
        listLat = loc[0]
        listLon = loc[1]

        # store lat and lon
        lat = data.variables['lat'][:]
        lon = data.variables['lon'][:]
        # squared differences of lat and lon
        #sq_diff_lat = (lat - listLat)**2
        #sq_diff_lon = (lon - listLon)**2
        # get index of minimum distance
        #min_index_lat = sq_diff_lat.argmin()
        #min_index_lon = sq_diff_lon.argmin()
