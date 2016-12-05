import datetime
import psycopg2
import csv

"""
Script to read in data from the archived SREW recordings csv files from CEDA.
Specify the year to read in to the database
Optional - specify the directory containing the archive files
"""

def read_in_archive_data(year, path):
    connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
    try:
        dbconn = psycopg2.connect(connection)
        cur = dbconn.cursor()
    except:
        print('Connection to the database could not be established')

#for each day/fil
    countEntered = 0
    countIgnored = 0
    if path == None:
        path = ''
    with open(path + 'midas_rainhrly_' + datetime.datetime.strftime(year, '%Y') +'.txt', 'r') as csvFile:
        data = csv.reader(csvFile, delimiter=',')
        reading = {}
        for row in data:
            reading['dateTime'] = row[0]
            reading['stationid'] = row[1]
            reading['value'] = row[8]

            try:
                readingSQL = "INSERT INTO hourlysrewreading" \
                             "(dateTime, stationid, value)" \
                             "VALUES (%(dateTime)s, %(stationid)s, %(value)s);"
                cur.execute(readingSQL, reading)
                countEntered +=1
                dbconn.commit()
            except psycopg2.DataError:
                #Reach here if the format of the reading is not correct
                countIgnored +=1
                dbconn.rollback()
            except psycopg2.IntegrityError:
                #Reach here if the station id given is not in the database
                countIgnored += 1
                dbconn.rollback()

    print('Number of entries: ' + str(countEntered))
    print('Number ignored: ' + str(countIgnored))
    print('Reached end of file for ' + datetime.datetime.strftime(year, '%Y'))

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='argument handler')
    parser.add_argument('-year', '-y', required= True, help='Year to read in: yyyy format')
    parser.add_argument('-path', '-p', required=False, help='The directory containing the data file')
    args = parser.parse_args()

    try:
        year = datetime.datetime.strptime(args.year, '%Y')
    except ValueError:
        print('The year was not entered in the correct format of yyyy')
        exit()

    try:
        read_in_archive_data(year, args.path)
    except FileNotFoundError:
        print('No such file or directory')