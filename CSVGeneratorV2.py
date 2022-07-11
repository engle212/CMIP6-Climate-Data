import os
from netCDF4 import Dataset
import pandas as pd
import numpy as np
import datetime
import xarray as xr
import time

def add_days_to_date(date, days):
    # Calculates and returns a date by adding days to given date
    added_date = pd.to_datetime(date) + datetime.timedelta(days=days)
    added_date = added_date.strftime("%Y-%m-%d")
    return added_date

def convert_df(var, df):
    # apply correct conversion function (temps require add instead of multiply)
    dataframe = pd.DataFrame()
    if (var == 'tasmax') or (var == 'tasmin'):
        dataframe = df.add(returnConvertFactor(var)).copy()
    else:
        dataframe = df.multiply(returnConvertFactor(var)).copy()
    return dataframe

def returnConvertFactor(var):
    # specify the correct numbers for conversion function
    num = 1
    if var == 'hurs':
        # no change required
        num = num
    elif var == 'pr':
        # (kg m-2s-1) * 86400
        # reference: https://www.researchgate.net/post/How-do-I-convert-ERA-Interim-precipitation-estimates-from-kg-m2-s-to-mm-day
        num = 86400
    elif var == 'rlds':
        # Wm-2 * 0.0036
        num = 0.0036
    elif var == 'rsds':
        # Wm-2 * 0.0036
        num = 0.0036
    elif var == 'sfcWind':
        # no change required
        num = num
    elif var == 'tasmax':
        # Kelvin - 273.15
        num = -273.15
    elif var == 'tasmin':
        # Kelvin - 273.15
        num = -273.15
    else:
        # retain original value
        num = num
    return num

def xr_to_df(data):
    data = data.to_dataframe()
    data.reset_index(inplace=True)
    return data

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

crd_idx = locations.set_index('loc').to_xarray()

all_years.sort()

for model in os.listdir(ncDir): # go through each model folder
    print('Processing...')
    modDir = os.path.join(ncDir, model)
    for type in os.listdir(modDir): # go through each type folder
        typeDir = os.path.join(modDir, type)
        for var in os.listdir(typeDir): # go through each var folder
            df = pd.DataFrame(columns=['YEAR', 'DOY', 'Date'])
            varDir = os.path.join(typeDir, var)
            path = os.path.join(varDir, '*.nc')

            yrStart = all_years[histLength+2]
            yrEnd = year_end
            if type == 'historical':
                yrStart = year_start
                yrEnd = all_years[histLength+1]

            # use xr.open_mfdataset to open multiple files at once and utilize dask
            nc = xr.open_mfdataset(path, coords='all')

            # Selects only coordinates nearest to locations
            subset = nc.sel(lat=crd_idx.lat, lon=crd_idx.lon, method='nearest')

            # convert units, and convert xarray to dataframe
            varDat = convert_df(var, subset[var].to_pandas())

            # YEAR DOY Date
            year = pd.Series(varDat.copy().index.year).rename('YEAR', inplace=True)
            doy = pd.Series(varDat.copy().index.dayofyear).rename('DOY', inplace=True)
            date = pd.Series(varDat.copy().index.tz_localize(None).date).rename('Date', inplace=True)
            varDat.reset_index(drop=True, inplace=True)

            varDat = pd.concat([year, doy, date, varDat], axis=1, ignore_index=False).copy()

            # Write to the correct file
            outFileName = str(var)+'_'+str(model)+'_'+str(type)+'_'+str(yrStart)+'_'+str(yrEnd)+'.csv'
            outFilePath = os.path.join(outDir, outFileName)

            varDat.to_csv(outFilePath, index=False)
            print(outFileName)
print('Complete!')
