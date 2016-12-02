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
    nextdate = latesttime
    while not os.path.exists(path):
        nextdate = nextdate - datetime.timedelta(minutes=5)
        path = 'data/RainRadarImages/' + nextdate.strftime('%Y') + '/' + nextdate.strftime('%m') + '/' + nextdate.strftime('%d') + '/' + nextdate.strftime('%H%M')
        if nextdate == latesttime - datetime.timedelta(hours=23):
            break

    if os.path.isfile(path + '/incomplete.tmp'): #check finished
        return nextdate
    elif nextdate != latesttime:
        nextdate = nextdate + datetime.timedelta(minutes=5)
        return nextdate
    # TODO at this point delete the file creating to show the script is running. Perhaps use atexit module
    sys.exit('up to date')

#For the given time slot, download all the rain radar tiles, creating the directory first.
#Only save tiles which show some rainfall occuring
def recursive_download_images(time):
    path = 'data/RainRadarImages/' + time.strftime('%Y') + '/' + time.strftime('%m') + '/' + time.strftime('%d') + '/' + time.strftime('%H%M')
    if not os.path.exists(path): os.makedirs(path)
    open(path + '/incomplete.tmp', 'a').close()
    baseurl = 'http://max.nwstatic.co.uk/tiles3/'
    zoom = '6/'
    url = baseurl + time.strftime('%Y%m%d') + '/' + time.strftime('%H%M') + '/' + zoom
    print(url)
    for y in range(8, 12): #script starts in top left and mows the lawn left to right
        for x in range(14, 17):
            png = http.request('GET', url + str(x) + '/' + str(y) + '.png')
            img = Image.open(io.BytesIO(png.data))
            if img.getbbox(): #If the image actually contains any rainfall
                f = open(path + '/' + str(x) + '_' + str(y) + '.png', 'wb')
                f.write(png.data)
                f.close()
    os.remove(path + '/incomplete.tmp')
    recursive_download_images(find_latest_dir(get_latest_available_time()))

# TODO implment method to see if script is already running
#Get latest avaliable tile time and kick off the download
recursive_download_images(find_latest_dir(get_latest_available_time()))








