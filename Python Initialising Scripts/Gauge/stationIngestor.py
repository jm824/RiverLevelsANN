import simplejson
import urllib3
import psycopg2

"""
This script is for ingesting stations and their measures.
This script can and should be run periodically to ingest new stations.

This script can be run at any time. It will retrieve all gauge stations from the EA API and
check if they are already stored in the local database. If they are not it will add the station and its measures
along with all the details of both. If the station is listed by the API but is in valid for any reason it
will be entered into the appropriate database table.

This script does not check if the details of previously entered station or measures have changed since
originally being entered into the database. Likewise the current validity of previously entered stations is not checked.
This should probably be handled from another script as this is for ingesting new stations only.
"""

connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
try:
    dbconn = psycopg2.connect(connection)
    cur = dbconn.cursor()
except:
    print('Connection to the database could not be established')

http = urllib3.PoolManager()
#load list of stations from API
stationReq = http.request('GET','http://environment.data.gov.uk/flood-monitoring/id/stations')
json = simplejson.loads(stationReq.data.decode('utf-8'))
#For each of these stations
for i in json.get('items'):
    #Check if the station is already entered into the local database
    apiStationID = str(i.get('@id').split('/')[-1])
    cur.execute("SELECT * FROM( "\
                  "SELECT id FROM gaugestation "\
                  "UNION "\
                  "SELECT id FROM ghoststation "\
                  "UNION "\
                  "SELECT id FROM naughtystation "\
                  "UNION "\
                  "SELECT id FROM failedstation) "\
                  "A "\
              "WHERE id = %s;", (apiStationID,))

    localStations = cur.fetchall()
    if localStations: #If already present then skip to next station
        continue
    #If we reach here the station has not yet been entered into the database
    #Load it's details and load it into the database to the appropriate table (if it is valid or invalid)
    stationReq = http.request('GET', 'http://environment.data.gov.uk/flood-monitoring/id/stations/' + apiStationID)
    try:
        json = simplejson.loads(stationReq.data.decode('utf-8'))
    #else we put the station in the GhostStation tables as it is invalid
    #This is where the station is listed by the API but the station URL does not work
    except simplejson.scanner.JSONDecodeError:
        cur.execute("INSERT INTO GhostStation VALUES (%s)", (apiStationID,))
        dbconn.commit()
        print('Station ' + apiStationID + ' added as ghost station.')
        continue

    station = json.get('items')
    try:
        #For all station data that must be present
        stationData = {}
        stationData['id'] = station.get('@id').split('/')[-1]
        stationData['lat'] = station.get('lat')
        stationData['long'] = station.get('long')
        stationData['stationReference'] = station.get('stationReference')
        stationData['notation'] = station.get('notation')
        stationData['label'] = station.get('label')
        #For the annoying stations that go against the documentation and only have one value in the type[]
        if isinstance(station.get('type'), list):
            stationData['typ'] = station.get('type')[0].split('/')[-1]
        else:
            stationData['typ'] = station.get('type').split('/')[-1]

        #Initialize all optional  values to NULL so that they are passed in as NULL by default
        stationData['RLOIid'] = None
        stationData['catchmentName'] = None
        stationData['dateOpened'] = None
        stationData['datumOffset'] = None
        stationData['riverName'] = None
        stationData['riverName'] = None
        stationData['town'] = None
        stationData['wiskiID'] = None
        stationData['easting'] = None
        stationData['northing'] = None
        stationData['status'] = None
        stationData['statusReason'] = None
        stationData['statusDate'] = None
        #stageScale data
        stationData['highestRecentDateTime'] = None
        stationData['highestRecent'] = None
        stationData['maxOnRecordDateTime'] = None
        stationData['maxOnRecord'] = None
        stationData['minOnRecordDateTime'] = None
        stationData['minOnRecord'] = None
        stationData['scaleMax'] = None
        stationData['typicalRangeHigh'] = None
        stationData['typicalRangeLow'] = None
        stationData['datum'] = None

        #For all the values which are optional (possibly not present)
        if station.get('RLOIid'): stationData['RLOIid'] = station.get('RLOIid')
        if station.get('catchmentName'): stationData['catchmentName'] = station.get('catchmentName')
        if station.get('dateOpened'): stationData['dateOpened'] = station.get('dateOpened')
        if station.get('datumOffset') or station.get('datumOffset') == 0: stationData['datumOffset'] = station.get('datumOffset')
        if station.get('riverName'): stationData['riverName'] = station.get('riverName')
        if station.get('town'): stationData['town'] = station.get('town')
        if station.get('wiskiID'): stationData['wiskiID'] = station.get('wiskiID')
        if station.get('easting'): stationData['easting'] = station.get('easting')
        if station.get('northing'): stationData['northing'] = station.get('northing')
        if station.get('status'): stationData['status'] = station.get('status').split('/')[-1]
        if station.get('statusReason'): stationData['statusReason'] = station.get('statusReason')
        if station.get('statusDate'): stationData['statusDate'] = station.get('statusDate')

        # check the values are not lists (as expected to be single values)
        for key, value in stationData.items():
            if isinstance(value, list):
                raise AttributeError

        #Check if the station is stage or downStage
        if station.get('stageScale'):
            scaleData = station.get('stageScale')
        elif station.get('downstageScale'):
            scaleData = station.get('downstageScale')
        else: scaleData = None

        #For the nested sacleData and it's nested parts
        if scaleData:
            if scaleData.get('highestRecent'):
                if scaleData.get('highestRecent').get('dateTime'): stationData['highestRecentDateTime'] = scaleData.get('highestRecent').get('dateTime')
                if scaleData.get('highestRecent').get('value') or scaleData.get('highestRecent').get('value') == 0: stationData['highestRecent'] = scaleData.get('highestRecent').get('value')

            if scaleData.get('maxOnRecord'):
                if scaleData.get('maxOnRecord').get('dateTime'): stationData['maxOnRecordDateTime'] = scaleData.get('maxOnRecord').get('dateTime')
                if  scaleData.get('maxOnRecord').get('value') or scaleData.get('maxOnRecord').get('value') == 0: stationData['maxOnRecord'] = scaleData.get('maxOnRecord').get('value')

            if scaleData.get('minOnRecord'):
                if scaleData.get('minOnRecord').get('dateTime'): stationData['minOnRecordDateTime'] = scaleData.get('minOnRecord').get('dateTime')
                if  scaleData.get('minOnRecord').get('value') or   scaleData.get('minOnRecord').get('value') == 0: stationData['minOnRecord'] =  scaleData.get('minOnRecord').get('value')

            if scaleData.get('scaleMax') or scaleData.get('scaleMax') == 0: stationData['scaleMax'] = scaleData.get('scaleMax')
            if scaleData.get('typicalRangeHigh') or scaleData.get('typicalRangeHigh') == 0: stationData['typicalRangeHigh'] = scaleData.get('typicalRangeHigh')
            if scaleData.get('typicalRangeLow') or scaleData.get('typicalRangeLow') == 0: stationData['typicalRangeLow'] = scaleData.get('typicalRangeLow')
            if scaleData.get('datum') or scaleData.get('datum') == 0: stationData['datum'] = scaleData.get('datum')

            #check the values are not lists (as expected to be single values)
            for key, value in scaleData.items():
                if isinstance(value, list):
                    raise AttributeError

    #Reached here if the station has some of its values in a list instead of the expected single value
    except AttributeError:
        cur.execute("INSERT INTO NaughtyStation VALUES (%s)", (stationData['id'],))
        dbconn.commit()
        print('Station ' + apiStationID + ' added as naughty station.')
        continue

    #This should never be reached, but here to log the error and prevent the script from exiting
    except Exception:
        cur.execute("INSERT INTO FailedStation VALUES (%s)", (stationData['id'],))
        dbconn.commit()
        continue

    try:
        stationSQL = "INSERT INTO GaugeStation" \
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
        cur.execute(stationSQL, stationData)
        dbconn.commit()
    except psycopg2.IntegrityError:
        dbconn.rollback()
        cur.execute("INSERT INTO NaughtyStation VALUES (%s)", (stationData['id'],))
        dbconn.commit()
        print('Station ' + apiStationID + ' added as valid station.')
        continue

    #Now for each measure the station has
    measureList = None
    if isinstance(station.get('measures'), dict):
        measureList = [station.get('measures')]
    else:
        measureList = station.get('measures')

    for measure in measureList:
        measureData = {}
        URL = measure.get('@id')

        measureReq = http.request('GET', URL)
        try:
            json = simplejson.loads(measureReq.data.decode('utf-8'))
        except simplejson.scanner.JSONDecodeError:
            print('There was a problem with ' + URL)

        # For all measure data that must be present
        measureData['id'] = measure.get('@id').split('/')[-1]
        measureData['label'] = measure.get('label')
        measureData['notation'] = measure.get('notation')
        measureData['parameter'] = measure.get('parameter')
        measureData['parameterName'] = measure.get('parameterName')
        measureData['qualifier'] = measure.get('qualifier')
        measureData['station'] = stationData['id']
        measureData['stationReference'] = measure.get('stationReference')

        # Initialize all optional measure values to NULL so that they are passed in to the
        # query as NULL by default
        measureData['datumType'] = None
        measureData['period'] = None
        measureData['unit'] = None
        measureData['unitName'] = None
        measureData['valueType'] = None

        # For all measure values which are optional (possibly not present)
        if measure.get('datumType'): measureData['datumType'] = measure.get('datumType').split('/')[-1]
        if measure.get('period'): measureData['period'] = measure.get('period')
        if measure.get('unit'): measureData['unit'] = measure.get('unit').split('/')[-1]
        if measure.get('unitName'): measureData['unitName'] = measure.get('unitName')
        if measure.get('valueType'): measureData['valueType'] = measure.get('valueType')

        try:
            measureSQL = "INSERT INTO GaugeMeasure" \
                "(id, datumType, label, notation, parameter, parameterName, period, qualifier, station, stationReference," \
                "unit, unitName, valueType)" \
                "VALUES(%(id)s, %(datumType)s, %(label)s, %(notation)s, %(parameter)s, %(parameterName)s, %(period)s," \
                "%(qualifier)s, %(station)s, %(stationReference)s, %(unit)s, %(unitName)s, %(valueType)s" \
                ");"
            cur.execute(measureSQL, measureData)
            dbconn.commit()
            print('Measure entered for ' + apiStationID)
        except psycopg2.IntegrityError:
            #If we get to here it should only be becuase the measure page does not exsist
            #Or becuase the measure's data was invalid in some way
            dbconn.rollback()




