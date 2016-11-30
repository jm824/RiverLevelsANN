import urllib3
import datetime
from PIL import Image
import io
import os
import sys

http = urllib3.PoolManager()


#Get the latest available time
def get_latest_available_time():

    latestAvailableTime = http.request('GET', 'http://rtime.nwstatic.co.uk/radar/rt5.js')
    latestAvailableTime = str(latestAvailableTime.data)
    returnedTime = ''
    for i in range(4,16):
        returnedTime += latestAvailableTime[i]

    return datetime.datetime.strptime(returnedTime, '%Y%m%d%H%M')


def find_latest_dir(latesttime):
    path = 'data/RainRadarImages/' + latesttime.strftime('%Y') + '/' + latesttime.strftime('%m') + '/' + latesttime.strftime('%d') + '/' + latesttime.strftime('%H%M')
    if not os.path.exists(path):
        #If we get here we have at least another 5 mins worth to download
        #See how many records are missing by looking at the current time and then going back 5 by 5 mins
        #untill a record is found
        nextdate = latesttime
        while(nextdate > latesttime - datetime.timedelta(hours=23)):
            path = 'data/RainRadarImages/' + nextdate.strftime('%Y') + '/' + nextdate.strftime('%m') + '/' + nextdate.strftime('%d') + '/' + nextdate.strftime('%H%M')
            if os.path.exists(path):
                return nextdate + datetime.timedelta(minutes=5)
            nextdate = nextdate - datetime.timedelta(minutes=5)


        return nextdate + datetime.timedelta(minutes=5)
    else: sys.exit('up to date')

#For the given time slot, download all the rain radar tiles, creating the directory first.
#Only save tiles which show some rainfall occuring
def recursive_download_images(time):
    path = 'data/RainRadarImages/' + time.strftime('%Y') + '/' + time.strftime('%m') + '/' + time.strftime('%d') + '/' + time.strftime('%H%M')
    os.makedirs(path)
    baseurl = 'http://max.nwstatic.co.uk/tiles3/'
    zoom = '9/'
    url = baseurl + time.strftime('%Y%m%d') + '/' + time.strftime('%H%M') + '/' + zoom
    print(url)
    for y in range(69, 95):
        for x in range(115, 135):
            png = http.request('GET', url + str(x) + '/' + str(y) + '.png')
            img = Image.open(io.BytesIO(png.data))
            if img.getbbox():
                f = open(path + '/' + str(x) + '_' + str(y) + '.png', 'wb')
                f.write(png.data)
                f.close()

    recursive_download_images(find_latest_dir(get_latest_available_time()))

recursive_download_images(find_latest_dir(get_latest_available_time()))








