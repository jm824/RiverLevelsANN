import urllib3
import shutil
import datetime

"""
One time script to download all 15min archived river level readings from the EA online archive (API).
Simply specify the start and end date.
"""

http = urllib3.PoolManager()
prefix = 'riverLevelArchive_' #The prefix chosen for files names
url = 'http://environment.data.gov.uk/flood-monitoring/archive?date='
startDate = datetime.date(2016, 11, 17)
endDate = datetime.date(2016, 11, 18)
while startDate != endDate:
    currentDate = startDate.strftime('%Y-%m-%d')
    print(startDate.strftime('%Y-%m-%d'))
    with http.request('GET', url + currentDate, preload_content=False) as r, open('data/RiverGauges/archivedRiverLevelRecordings/' + prefix + currentDate + '.csv', 'wb') as out_file:
        shutil.copyfileobj(r, out_file)
    #+1 day
    startDate = startDate + datetime.timedelta(1)
