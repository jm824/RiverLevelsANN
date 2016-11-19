import simplejson
import urllib3

"""
One time script to grab all the station references (unique id's) from the EA API and save them in a file
"""

http = urllib3.PoolManager()
stationReq = http.request('GET','http://environment.data.gov.uk/flood-monitoring/id/stations')
json = simplejson.loads(stationReq.data.decode('utf-8'))
json.get('items')

file = open('data/newGaugeStationsIDs.txt','w')
for i in json.get('items'):
    file.write(i.get('@id'))
    file.write('\n')
file.close()




