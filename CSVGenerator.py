import os
from netCDF4 import Dataset
import pandas as pd
import numpy as np


def add_days_to_date(date, days):
    # Calculates and returns a date by adding days to given date
    added_date = pd.to_datetime(date) + timedelta(days=days)
    added_date = added_date.strftime("%Y-%m-%d")

    return added_date

def anim(animation):
    end = False
    idx = 0
    num = 0
    while not end:
        print(animation[idx % len(animation)], end="\r")
        idx += 1
        num += 1
        time.sleep(0.005)
        if (idx > len(animation)):
            end = True

ncDir = 'Downloaded Files'
outDir = 'Output Files'

if not os.path.exists(outDir):
    # create output directory
    os.mkdir(outDir)
    print('[output directory created]')


all_years = []

for inFileName in os.listdir(ncDir): # iterate through all netCDF files
    inFilePath = os.path.join(ncDir, inFileName)
    data = Dataset(inFilePath, 'r')
    time = data.variables['time']
    year = inFileName.split('.')[0]
    all_years.append(year)

# create empty dataframe covering the whole range of data
year_start = min(all_years)
year_end = max(all_years)
date_range = pd.date_range(start=str(year_start)+'-01-01', end=str(year_end)+'-12-31', freq='D')

# create and populate data structure for locations csv data
locations = pd.read_csv('locations_sub.csv')


all_years.sort()

for yr in all_years:
    # read data
    inFilePath = os.path.join(ncDir, str(yr)+'.nc')
    data = Dataset(inFilePath, 'r')

    lat = data.variables['lat'][:]
    lon = data.variables['lon'][:]

    # Access precipitation data
    pr = data.variables['pr']

    start = str(yr) + '-01-01'
    end = str(yr) + '-12-31'

    df = pd.DataFrame(0.0, columns=['pr'], index=date_range)

    d_range = pd.date_range(start=start, end=end, freq='D')

    for index, row in locations.iterrows():
        locLat = row[0]
        locLon = row[1]
        name = row[2]
        column = pd.DataFrame(0.0, columns=[name], index=date_range)
        # squared differences of lat and lon
        sq_diff_lat = (lat - locLat)**2
        sq_diff_lon = (lon - locLon)**2
        # get index of minimum distance
        min_index_lat = sq_diff_lat.argmin()
        min_index_lon = sq_diff_lon.argmin()

        for t_index in np.arange(0, len(d_range)):
            print('Recording value for: ' + str(d_range[t_index]) + ' @ ' + name)
            column.loc[d_range[t_index]]['pr'] = pr[t_index, min_index_lat, min_index_lon]

        # concat df with column
        df = pd.concat([df, column], axis=1, ignore_index=False)
    outFileName = 'pr_'+str(year_start)+'_'+str(year_end)+'.csv'
    outFilePath = os.path.join(outDir, outFileName)


    df.to_csv(outFilePath)
