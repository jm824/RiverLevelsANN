import datetime
import psycopg2
import csv
import os

"""
Script to read in data from the archived river level recordings csv files and put into database.
Specify the start and end dates (inclusive)
Optional - specify the directory containing the archive files
"""

def read_in_archive_data(startDate, endDate, measure, path=None):
    try:
        startDate = datetime.datetime.strptime(args.start, '%d-%m-%Y')
        endDate = datetime.datetime.strptime(args.end, '%d-%m-%Y')
    except ValueError:
        exit('The dates were not entered in the correct format of dd-mm-yyyy')
    if path:
        if not os.path.exists(path):
            exit('The specified path does not exist')
        elif os.path.isfile(path):
            exit('Specify the directory containing the file(s), not a file itself')
    else:
        path = ''

    #Establish a connection to the database
    connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
    try:
        dbconn = psycopg2.connect(connection)
        cur = dbconn.cursor()
    except:
        exit('Connection to the database could not be established')

    # Check if the measure provided is in the database
    cur.execute("SELECT measureid FROM hourlygaugereading WHERE measureid = (%s);", (measure,))
    if cur.fetchone() is None:
        exit(measure + " is not a valid station")

    #for each file (day)
    while(startDate != endDate):
        currentDate = startDate.strftime('%Y-%m-%d')
        countEntered = 0
        countIgnored = 0
        file_path = os.path.join(path, 'riverLevelArchive_' + currentDate +'.csv')
        if not os.path.isfile(file_path):
            exit('No such file ' + file_path)
        with open(file_path, 'r') as csvFile:
            data = csv.reader(csvFile, delimiter=',')
            reading = {}
            for row in data:
                if row[0] == 'dateTime': continue #skip heading row
                if row[1].split('/')[-1] == measure: #Check the row is for the measure specified
                    reading['dateTime'] = row[0]
                    reading['measureId'] = row[1].split('/')[-1]
                    reading['value'] = row[2]

                    try:
                        readingSQL = "INSERT INTO hourlyGaugeReading" \
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

        print('Reached end of file for', currentDate)
        print('Number of entries:', str(countEntered))
        print('Number ignored:',str(countIgnored))
        startDate = startDate + datetime.timedelta(1)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='argument handler')
    parser.add_argument('start', help='Start date in dd-mm-yyyy format')
    parser.add_argument('end', help='End date in dd-mm-yyyy format')
    parser.add_argument('measure', help='The measure to ingest readings for')
    parser.add_argument('--path', '-p', required=False, help='The directory containing the data file(s)')
    args = parser.parse_args()

    read_in_archive_data(args.start, args.end, args.measure, args.path)




