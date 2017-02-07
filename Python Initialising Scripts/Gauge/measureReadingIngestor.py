import datetime
import psycopg2
import csv

"""
Script to read in data from the archived river level recordings csv files.
Specify the start and end dates (inclusive)
Optional - specify the directory containing the archive files
"""

def read_in_archive_data(startDate, endDate, path):
    connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
    try:
        dbconn = psycopg2.connect(connection)
        cur = dbconn.cursor()
    except:
        print('Connection to the database could not be established')

    #for each day/file
    while(startDate != endDate):
        currentDate = startDate.strftime('%Y-%m-%d')

        countEntered = 0
        countIgnored = 0
        if path == None:
            path = ''
        with open(path + 'riverLevelArchive_' + currentDate +'.csv', 'r') as csvFile:
            data = csv.reader(csvFile, delimiter=',')
            reading = {}
            for row in data:
                if row[0] == 'dateTime': continue #skip heading row
                reading['dateTime'] = row[0]
                reading['measureId'] = row[1].split('/')[-1]
                reading['value'] = row[2]

                try:
                    readingSQL = "INSERT INTO GaugeReading" \
                                 "(dateTime, measureId, value)" \
                                 "VALUES (%(dateTime)s, %(measureId)s, %(value)s);"
                    cur.execute(readingSQL, reading)
                    countEntered +=1
                    dbconn.commit()
                except psycopg2.DataError:
                    #Reach here if the format of the reading is not correct
                    countIgnored +=1
                    dbconn.rollback()
                except psycopg2.IntegrityError:
                    #Reach here if either the reading does not have a station or if it is already entered
                    countIgnored += 1
                    dbconn.rollback()

        print('Number of entries: ' + countEntered)
        print('Number ignored: ' + countIgnored)
        print('Reached end of file for ' + currentDate)
        startDate = startDate + datetime.timedelta(1)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='argument handler')
    parser.add_argument('-start', '-s', required= True, help='Start date in dd-mm-yyyy format')
    parser.add_argument('-end', '-e', required=True, help='End date in dd-mm-yyyy format')
    parser.add_argument('-measure', '-m', required=False, help='The measure to ingest readings for')
    parser.add_argument('-path', '-p', required=False, help='The directory containing the data file ')
    args = parser.parse_args()

    try:
        startDate = datetime.datetime.strptime(args.start, '%d-%m-%Y')
        endDate = datetime.datetime.strptime(args.end, '%d-%m-%Y')
    except ValueError:
        print('The dates were not entered in the correct format of dd-mm-yyyy')
        exit()

    try:
        read_in_archive_data(startDate, endDate, args.path)
    except FileNotFoundError:
        print('No such file or directory')



