import urllib3
import shutil
import datetime
import os

"""
Script to download all 15min archived river level readings from the EA online archive (API).
Simply specify the start and end date of the files to download.
A directory to download to can be specified.
"""

class DownloadArchiveFiles:
    def __init__(self, start, end, path=""):
        try:
            start = datetime.datetime.strptime(start, '%d-%m-%Y')
            end = datetime.datetime.strptime(end, '%d-%m-%Y')
        except ValueError:
            exit('The year was not entered in the correct format of yyyy')
        if path:
            if not os.path.exists(path):
                exit('The specified path does not exist: ' + path)
        else:
            path = ''

        http = urllib3.PoolManager()
        url = 'http://environment.data.gov.uk/flood-monitoring/archive?date='
        #For each day archive file
        while start != end + datetime.timedelta(1):
            currentDate = start.strftime('%Y-%m-%d')
            file_path = os.path.join(path, 'riverLevelArchive_' + currentDate + '.csv')
            #Download archive file as .csv
            with http.request('GET', url + currentDate, preload_content=False) as r:
                if r.status != 200: #Check the file is valid by checking HTTP response code
                    exit('Invalid archive file: ' + currentDate)
                outfile = open(file_path, 'wb')
                shutil.copyfileobj(r, outfile)
            print('Downloaded', start.strftime('%d-%m-%Y'))
            start = start + datetime.timedelta(1)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='argument handler')
    parser.add_argument('start', help='Start year to download from, dd-mm-yyyy format')
    parser.add_argument('end', help='Start year to download to, dd-mm-yyyy format')
    parser.add_argument('--path', '-p', required=False, help='The directory to download the files to')
    args = parser.parse_args()

    DownloadArchiveFiles(args.start, args.end, args.path)
