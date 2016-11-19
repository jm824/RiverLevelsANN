import simplejson
import urllib3
import urllib.request

def printStationDetails(stationId):
	http = urllib3.PoolManager()
	req = http.request('GET','http://environment.data.gov.uk/flood-monitoring/id/stations/' + stationId)
	opener = urllib3.build_opener()

	f = opener.open(req)
	json = simplejson.load(f)

	print('--------------------------------------')
	print('Station ID = ' + json.get('items').get('stationReference'))
	print(json.get('items').get('catchmentName'))
	print(json.get('items').get('eaRegionName'))
	print(json.get('items').get('eaAreaName'))
	print(json.get('items').get('label'))
	print(json.get('items').get('riverName'))
	print(json.get('items').get('town'))
	print(json.get('items').get('lat'))
	print(json.get('items').get('long'))
	print(json.get('items').get('easting'))
	print(json.get('items').get('northing'))
	try:
		print(json.get('items').get('stageScale').get('datum'))
		print(json.get('items').get('stageScale').get('highestRecent').get('dateTime'))
		print(json.get('items').get('stageScale').get('highestRecent').get('value'))
		print(json.get('items').get('stageScale').get('maxOnRecord').get('dateTime'))
		print(json.get('items').get('stageScale').get('maxOnRecord').get('value'))
		print(json.get('items').get('stageScale').get('minOnRecord').get('dateTime'))
		print(json.get('items').get('stageScale').get('minOnRecord').get('value'))
		print(json.get('items').get('stageScale').get('scaleMax'))
		print(json.get('items').get('stageScale').get('typicalRangeHigh'))
		print(json.get('items').get('stageScale').get('typicalRangeLow'))
	except AttributeError:
		print('NO STAGESCALE DATA')
	print(json.get('items').get('dateOpened'))
	print(json.get('items').get('RLOIid'))
	print(json.get('items').get('wiskiID'))
	print('--------------------------------------')


req = urllib3.Request('http://environment.data.gov.uk/flood-monitoring/id/stations?_limit=10000')
opener = urllib3.build_opener()
f = opener.open(req)
json = simplejson.load(f)

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



