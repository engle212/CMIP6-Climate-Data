import requests
import os
import pandas as pd

def requestFromServer(reqURL):
    try:
        print('Requesting from server...')
        r = requests.get(reqURL)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        # retry
        print('Retrying server request...')
        r = requestFromServer(reqURL)
    return r

def downloadDataRange(directory, model, type, var, startYear, endYear, north, west, east, south):
    yRange = range(int(startYear), int(endYear) + 1)

    for year in yRange:
        # assemble request URL using info
        url = ''.join(['https://ds.nccs.nasa.gov/thredds/ncss/AMES/NEX/GDDP-CMIP6/', str(model), '/', str(type), '/r1i1p1f1/', str(var), '/', str(var), '_day_', str(model), '_', str(type), '_r1i1p1f1_gn_', str(year), '.nc?var=', str(var), '&north=', str(north), '&west=', str(west), '&east=', str(east), '&south=', str(south), '&disableProjSubset=on&horizStride=1&time_start=', str(year), '-01-01T12%3A00%3A00Z&time_end=', str(year), '-12-31T12%3A00%3A00Z&timeStride=1&addLatLon=true'])

        req = requestFromServer(url)

        fileName = str(var) + '_day_' + model + '_' + str(type) + '_r1i1p1f1_gn_' + str(year) + '.nc'

        filePath = os.path.join(directory, model, type, var, fileName)

        with open(filePath, 'wb') as f:
            f.write(req.content)
        print('[DOWNLOAD COMPLETED]......' + fileName)


# Create all directories based on info from DataSpecifications.csv
specs = pd.read_csv('DataSpecifications.csv', header=0)
specs.fillna(0, inplace=True)

outDir = 'Downloaded Files'
if not os.path.exists(outDir):
    # create output directory
    os.mkdir(outDir)
    print('[output directory created]')

for model in specs.loc[:]['Models']:
    if model != 0:
        modelDir = os.path.join(outDir, model)
        if not os.path.exists(modelDir):
            # create model directory
            os.mkdir(modelDir)
        for type in specs.loc[:]['Types']:
            if type != 0:
                typeDir = os.path.join(modelDir, type)
                if not os.path.exists(typeDir):
                    # create type directory
                    os.mkdir(typeDir)
                for var in specs.loc[:]['Variables']:
                    if var != 0:
                        varDir = os.path.join(typeDir, var)
                        if not os.path.exists(varDir):
                            # create var directory
                            os.mkdir(varDir)
                        # request corresponding data
                        if type == 'historical':
                            start = specs.at[0, 'hStart']
                            end = specs.at[0, 'hEnd']
                        else:
                            start = specs.at[0, 'StartYear']
                            end = specs.at[0, 'EndYear']
                        downloadDataRange(outDir, model, type, var, start, end, specs.at[0, 'north'], specs.at[0, 'west'], specs.at[0, 'east'], specs.at[0, 'south'])
