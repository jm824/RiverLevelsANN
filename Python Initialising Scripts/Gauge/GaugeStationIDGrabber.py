import simplejson
import urllib3

"""
One time script to grab all the station references (unique id's) from the EA API and save them in
a text file. This us useful for testing and saving making too many HTTP requests
"""
http = urllib3.PoolManager()
stationReq = http.request('GET', 'http://environment.data.gov.uk/flood-monitoring/id/stations')
json = simplejson.loads(stationReq.data.decode('utf-8'))
json.get('items')


def save_station_ids():
    file = open('data/newGaugeStationsIDs.txt', 'w')
    for i in json.get('items'):
        file.write(i.get('@id'))
        file.write('\n')
    file.close()


# Check to see if an id is in the list returned by the API
def check_for_id(id):
    for i in json.get('items'):
        if i.get('@id') == 'http://environment.data.gov.uk/flood-monitoring/id/stations/' + id:
            return True
    return False

save_station_ids()
