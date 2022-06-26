import os
from netCDF4 import Dataset
import pandas as pd
import numpy as np


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

df = pd.DataFrame(columns=['YEAR', 'DOY', 'Date'])

for model in os.listdir(ncDir): # go through each model folder
    modDir = os.path.join(ncDir, model)
    for type in os.listdir(modDir): # go through each type folder
        typeDir = os.path.join(modDir, type)
        for var in os.listdir(typeDir): # go through each var folder
            varDir = os.path.join(typeDir, var)
            # read each file in var folder
            for yr in all_years:
                # read data
                inFilePath = os.path.join(varDir, str(yr)+'.nc')
                data = Dataset(inFilePath, 'r')

                start = str(yr) + '-01-01'
                end = str(yr) + '-12-31'

                d_range = pd.date_range(start=start, end=end, freq='D')

                yearChunk = pd.DataFrame(0.0, columns=['YEAR', 'DOY', 'Date'], index=d_range)

                lat = data.variables['lat'][:]
                lon = data.variables['lon'][:]

                # Access data for current variable
                varData = data.variables[var]

                doy = np.arange(1, len(d_range) + 1)

                # populate doy and year column
                for x in d_range:
                    yearChunk.at[x, 'DOY'] = str(doy[d_range.get_loc(x)])
                    yearChunk.at[x, 'YEAR'] = str(yr)

                for index, row in locations.iterrows():
                    locLat = row[0]
                    locLon = row[1]
                    name = row[2]
                    column = pd.DataFrame(0.0, columns=[name], index=d_range)
                    # squared differences of lat and lon
                    sq_diff_lat = (lat - locLat)**2
                    sq_diff_lon = (lon - locLon)**2
                    # get index of minimum distance
                    min_index_lat = sq_diff_lat.argmin()
                    min_index_lon = sq_diff_lon.argmin()

                    for t_index in np.arange(0, len(d_range)):
                        print('Recording value for: ' + str(d_range[t_index]) + ' @ ' + name)
                        column.at[d_range[t_index], name] = varData[t_index, min_index_lat, min_index_lon]

                    # concat df with column
                    yearChunk = pd.concat([yearChunk, column], axis=1, ignore_index=False).copy()
                df = pd.concat([df, yearChunk], axis=0, ignore_index=False).copy()

                outFileName = 'pr_'+str(year_start)+'_'+str(year_end)+'.csv'
                outFilePath = os.path.join(outDir, outFileName)

                df['Date'] = df.index
                df.rename(columns={'index':'Date'})

                df.to_csv(outFilePath, index=False)
