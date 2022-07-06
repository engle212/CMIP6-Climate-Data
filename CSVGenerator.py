import os
from netCDF4 import Dataset
import pandas as pd
import numpy as np
import datetime

def add_days_to_date(date, days):
    # Calculates and returns a date by adding days to given date
    added_date = pd.to_datetime(date) + datetime.timedelta(days=days)
    added_date = added_date.strftime("%Y-%m-%d")
    return added_date

def convert_units(var, val):
    converted = float(val) # convert to float type
    if var == 'hurs':
        # no change required
        converted = converted
    elif var == 'pr':
        # (kg m-2s-1) * 86400
        # reference: https://www.researchgate.net/post/How-do-I-convert-ERA-Interim-precipitation-estimates-from-kg-m2-s-to-mm-day
        converted = converted * 86400
    elif var == 'rlds':
        # Wm-2 * 0.0036
        converted = converted * 0.0036
    elif var == 'rsds':
        # Wm-2 * 0.0036
        converted = converted * 0.0036
    elif var == 'sfcWind':
        # no change required
        converted = converted
    elif var == 'tasmax':
        # Kelvin - 273.15
        converted = converted - 273.15
    elif var == 'tasmin':
        # Kelvin - 273.15
        converted = converted - 273.15
    else:
        # retain original value
        converted = converted
    return converted

ncDir = 'Downloaded Files'
outDir = 'Output Files'

if not os.path.exists(outDir):
    # create output directory
    os.mkdir(outDir)
    print('[output directory created]')

all_years = []

for model in os.listdir(ncDir): # iterate through all model folders
    modPath = os.path.join(ncDir, model)
    for type in os.listdir(modPath):
        # when historical add to histYears instead
        histLength = 0
        typePath = os.path.join(modPath, type)
        for var in os.listdir(typePath):
            varPath = os.path.join(typePath, var)
            for inFileName in os.listdir(varPath):
                filePath = os.path.join(varPath, inFileName)
                data = Dataset(filePath, 'r')
                time = data.variables['time']
                baseYear = time.units[11:15]
                year = add_days_to_date(baseYear, int(time[0])).split('-')[0]
                if not year in all_years:
                    all_years.append(year)
                    if type == 'historical':
                        histLength += 1

# create empty dataframe covering the whole range of data
year_start = min(all_years)
year_end = max(all_years)

date_range = pd.date_range(start=str(year_start)+'-01-01 00:00:00+00:00', end=str(year_end)+'-12-31 00:00:00+00:00', freq='D')

# create and populate data structure for locations csv data
locations = pd.read_csv('locations_sub.csv')

all_years.sort()

for model in os.listdir(ncDir): # go through each model folder
    modDir = os.path.join(ncDir, model)
    for type in os.listdir(modDir): # go through each type folder
        typeDir = os.path.join(modDir, type)
        for var in os.listdir(typeDir): # go through each var folder
            df = pd.DataFrame(columns=['YEAR', 'DOY', 'Date'])
            varDir = os.path.join(typeDir, var)

            yrStart = all_years[histLength+2]
            yrEnd = year_end
            if type == 'historical':
                yrStart = year_start
                yrEnd = all_years[histLength+1]
            yRange = range(int(yrStart), int(yrEnd)+1)
            # read each file in var folder
            for yr in yRange:
                # read data
                fileName = str(var) + '_day_' + model + '_' + str(type) + '_r1i1p1f1_gn_' + str(yr) + '.nc'
                inFilePath = os.path.join(varDir, fileName)
                if os.path.exists(inFilePath): # check if file exists
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
                            column.at[d_range[t_index], name] = convert_units(var, varData[t_index, min_index_lat, min_index_lon])

                        # concat df with column
                        yearChunk = pd.concat([yearChunk, column], axis=1, ignore_index=False).copy()
                    df = pd.concat([df, yearChunk], axis=0, ignore_index=False).copy()

                    outFileName = str(var)+'_'+str(model)+'_'+str(type)+'_'+str(yrStart)+'_'+str(yrEnd)+'.csv'
                    outFilePath = os.path.join(outDir, outFileName)

                    df['Date'] = df.index
                    df.rename(columns={'index':'Date'})

                    df.to_csv(outFilePath, index=False)
