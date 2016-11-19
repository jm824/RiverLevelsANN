import simplejson
import urllib3


def printStationDetails(stationId):
    req2 = http.request('GET', 'http://environment.data.gov.uk/flood-monitoring/id/stations')
    json = simplejson.loads(req2.data.decode('utf-8'))

    for i in json:
        print('--------------------------------------')
        print('Station ID = ' + i.get('items').get('stationReference'))
        print(i.get('items').get('catchmentName'))
        print(i.get('items').get('eaRegionName'))
        print(i.get('items').get('eaAreaName'))
        print(i.get('items').get('label'))
        print(i.get('items').get('riverName'))
        print(i.get('items').get('town'))
        print(i.get('items').get('lat'))
        print(i.get('items').get('long'))
        print(i.get('items').get('easting'))
        print(i.get('items').get('northing'))
        try:
            print(i.get('items').get('stageScale').get('datum'))
            print(i.get('items').get('stageScale').get('highestRecent').get('dateTime'))
            print(i.get('items').get('stageScale').get('highestRecent').get('value'))
            print(i.get('items').get('stageScale').get('maxOnRecord').get('dateTime'))
            print(i.get('items').get('stageScale').get('maxOnRecord').get('value'))
            print(i.get('items').get('stageScale').get('minOnRecord').get('dateTime'))
            print(i.get('items').get('stageScale').get('minOnRecord').get('value'))
            print(i.get('items').get('stageScale').get('scaleMax'))
            print(i.get('items').get('stageScale').get('typicalRangeHigh'))
            print(i.get('items').get('stageScale').get('typicalRangeLow'))
        except AttributeError:
            print('NO STAGESCALE DATA')
        print(i.get('items').get('dateOpened'))
        print(i.get('items').get('RLOIid'))
        print(i.get('items').get('wiskiID'))
        print('--------------------------------------')


http = urllib3.PoolManager()
req = http.request('GET', 'http://environment.data.gov.uk/flood-monitoring/id/stations')
json = simplejson.loads(req.data.decode('utf-8'))
items = json.get('items')

count = 0
theError = 'empty'
errorCount = 0
for i in items:
    count = count + 1
    if count < -1:
        print('nope')
    else:
        try:
            printStationDetails(i.get('notation'))
        except:
            continue

print(count)
print(theError)
print(errorCount)
