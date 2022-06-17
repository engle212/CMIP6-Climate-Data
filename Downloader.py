import requests
import os

def requestFromServer(reqURL):
    try:
        r = requests.get(reqURL)
    except requests.exceptions.Timeout:
        # retry
        print('[Retrying server request]')
        r = requestFromServer(reqURL)
    return r

outDir = 'Downloaded Files'
if not os.path.exists(outDir):
    # create output directory
    os.mkdir(outDir)
    print('[output directory created]')

north = 42.337
west = -85.301
east = -82.654
south = 40.368

print('Welcome!')
print('...')
print('')
print('')
# get info for request URL from user
model = input('Enter the model: ')

type = input('Enter the type of climate data: ')

var = input('Enter the variable name: ')

start = input('Enter start year: ')
end = input('Enter end year: ')

print('')
print('- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -')
print('')

yRange = range(int(start), int(end) + 1)

for year in yRange:
    # assemble request URL using info
    url = ''.join(['https://ds.nccs.nasa.gov/thredds/ncss/AMES/NEX/GDDP-CMIP6/', str(model), '/', str(type), '/r1i1p1f1/', str(var), '/', str(var), '_day_', str(model), '_', str(type), '_r1i1p1f1_gn_', str(year), '.nc?var=pr&north=', str(north), '&west=', str(west), '&east=', str(east), '&south=', str(south), '&disableProjSubset=on&horizStride=1&time_start=', str(year), '-01-01T12%3A00%3A00Z&time_end=', str(year), '-12-31T12%3A00%3A00Z&timeStride=1&addLatLon=true'])

    req = requestFromServer(url)

    fileName = str(var) + '_day_' + model + '_' + str(type) + '_r1i1p1f1_gn_' + str(year) + '.nc'

    filePath = os.path.join(outDir, fileName)

    with open(filePath, 'wb') as f:
        f.write(req.content)

    print('[DOWNLOAD COMPLETED]......' + fileName)
