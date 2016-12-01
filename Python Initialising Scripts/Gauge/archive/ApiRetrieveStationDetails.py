import simplejson
import urllib3
import psycopg2

#Script to query the EA real time flood monitoring API and grab details for known stations
#Script uses pre-collected station IDs which at the time of writing were known to be valid

connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
try:
    dbconn = psycopg2.connect(connection)
    cur = dbconn.cursor()
except:
    print('Connection to the database could not be established')

http = urllib3.PoolManager()
file = open('data/GaugeStationsIDs.txt', 'r')
for line in file:
    temp = line.strip() #the station URL
    print(line)
    http = urllib3.PoolManager()
    stationReq = http.request('GET', temp)
    try:
        json = simplejson.loads(stationReq.data.decode('utf-8'))
    except simplejson.scanner.JSONDecodeError: #else we right the station ID to the naughty list as it is not valid
        file = open('data/ghostStationIDs.txt', 'a')
        file.write(temp)
        file.write('\n')
        continue

    #Start reading in station data into the data dict
	station = json.get('items')
    data = {}
    data['id'] = station.get('@id').split('/')[-1]
    data['lat'] = station.get('lat')
    data['long'] = station.get('long')
    data['stationReference'] = station.get('stationReference')
    data['notation'] = station.get('notation')
    data['label'] = station.get('label')
    data['typ'] = station.get('type')[0].split('/')[-1]

    #Initialize all optional  values to NULL so that they are passed in as NULL by default (as these can be NULL in the database)
    data['RLOIid'] = None
    data['catchmentName'] = None
    data['dateOpened'] = None
    data['datumOffset'] = None
    data['riverName'] = None
    data['riverName'] = None
    data['town'] = None
    data['wiskiID'] = None
    data['easting'] = None
    data['northing'] = None
    data['status'] = None
    data['statusReason'] = None
    data['statusDate'] = None
    #stageScale data below
    data['highestRecentDateTime'] = None
    data['highestRecent'] = None
    data['maxOnRecordDateTime'] = None
    data['maxOnRecord'] = None
    data['minOnRecordDateTime'] = None
    data['minOnRecord'] = None
    data['scaleMax'] = None
    data['typicalRangeHigh'] = None
    data['typicalRangeLow'] = None

    #For all the values which are optional (possibly not present). These have alrady been set to NULL
    if station.get('RLOIid'): data['RLOIid'] = station.get('RLOIid')
    if station.get('catchmentName'): data['catchmentName'] = station.get('catchmentName')
    if station.get('dateOpened'): data['dateOpened'] = station.get('dateOpened')
    if station.get('datumOffset') or station.get('datumOffset') == 0: data['datumOffset'] = station.get('datumOffset')
    if station.get('riverName'): data['riverName'] = station.get('riverName')
    if station.get('town'): data['town'] = station.get('town')
    if station.get('wiskiID'): data['wiskiID'] = station.get('wiskiID')
    if station.get('easting'): data['easting'] = station.get('easting')
    if station.get('northing'): data['northing'] = station.get('northing')
    if station.get('status'): data['status'] = station.get('status').split('/')[-1]
    if station.get('statusReason'): data['statusReason'] = station.get('statusReason')
    if station.get('statusDate'): data['statusDate'] = station.get('statusDate')

    #Check if the station is type stage or downStage
    if station.get('stageScale'):
        scaleData = station.get('stageScale')
    elif station.get('downstageScale'):
        scaleData = station.get('downstageScale')
    else: scaleData = None

    #For the nested sacleData and it's nested parts - code to grab the right data
    if scaleData:
        if scaleData.get('highestRecent'):
            if scaleData.get('highestRecent').get('dateTime'): data['highestRecentDateTime'] = scaleData.get('highestRecent').get('dateTime')
            if scaleData.get('highestRecent').get('value') or scaleData.get('highestRecent').get('value') == 0: data['highestRecent'] = scaleData.get('highestRecent').get('value')

        if scaleData.get('maxOnRecord'):
            if scaleData.get('maxOnRecord').get('dateTime'): data['maxOnRecordDateTime'] =scaleData.get('maxOnRecord').get('dateTime')
            if  scaleData.get('maxOnRecord').get('value') or scaleData.get('maxOnRecord').get('value') == 0: data['maxOnRecord'] = scaleData.get('maxOnRecord').get('value')

        if scaleData.get('minOnRecord'):
            if scaleData.get('minOnRecord').get('dateTime'): data['minOnRecordDateTime'] = scaleData.get('minOnRecord').get('dateTime')
            if  scaleData.get('minOnRecord').get('value') or   scaleData.get('minOnRecord').get('value') == 0: data['minOnRecord'] =  scaleData.get('minOnRecord').get('value')

        if scaleData.get('scaleMax') or scaleData.get('scaleMax') == 0: data['scaleMax'] = scaleData.get('scaleMax')
        if scaleData.get('typicalRangeHigh') or scaleData.get('typicalRangeHigh') == 0: data['typicalRangeHigh'] = scaleData.get('typicalRangeHigh')
        if scaleData.get('typicalRangeLow') or scaleData.get('typicalRangeLow') == 0: data['typicalRangeLow'] = scaleData.get('typicalRangeLow')
        if scaleData.get('datum') or scaleData.get('datum') == 0: data['datum'] = scaleData.get('datum')

	# Finally enter the collected data into the database
    SQL = "INSERT INTO GaugeStation" \
          "(id, lat, long, stationReference, notation, type, label, RLOIid, catchmentName," \
          "dateOpened, datumOffset, riverName, town, wiskiID, easting, northing, status," \
          "statusReason, statusDate, highestRecentDateTime, highestRecent, maxOnRecordDateTime, maxOnRecord," \
          "minOnRecordDateTime, minOnRecord, scaleMax, typicalRangeHigh, typicalRangeLow, datum)" \
          "VALUES (%(id)s, %(lat)s, %(long)s, %(stationReference)s, %(notation)s, %(typ)s, %(label)s," \
          "%(RLOIid)s, %(catchmentName)s, %(dateOpened)s, %(datumOffset)s, %(riverName)s, %(town)s," \
          "%(wiskiID)s, %(easting)s, %(northing)s, %(status)s, %(statusReason)s, %(statusDate)s, %(highestRecentDateTime)s," \
          "%(highestRecent)s, %(maxOnRecordDateTime)s, %(maxOnRecord)s, %(minOnRecordDateTime)s, %(minOnRecord)s," \
          "%(scaleMax)s, %(typicalRangeHigh)s, %(typicalRangeLow)s, %(datum)s" \
          ");"
    cur.execute(SQL, data)
    dbconn.commit()

file.close()


