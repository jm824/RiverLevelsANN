import urllib3
import shutil
import datetime


http = urllib3.PoolManager()
prefix = 'riverLevelArchive_'
url = 'http://environment.data.gov.uk/flood-monitoring/archive?date='
date = datetime.date(2016,11,27)
while(date.strftime('%Y-%m-%d') != '2017-01-21'):

    currentDate = date.strftime('%Y-%m-%d')

    print(date.strftime('%Y-%m-%d'))
    with http.request('GET', url + currentDate, preload_content=False) as r, open('data/RiverGauges/archivedRiverLevelRecordings/' + prefix + currentDate + '.csv', 'wb') as out_file:
        shutil.copyfileobj(r, out_file)
    date = date + datetime.timedelta(1)
