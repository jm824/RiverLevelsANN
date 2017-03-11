"""
Script to read in data from the archived SREW recordings csv files (downloaded from from CEDA).
Specify the year to read in to the database as well as the station id
Optional - specify the directory containing the archive files as well as the end year if you want to read
            multiple years at once
"""

import datetime
import psycopg2
import csv
import os

class IngestSrew:
    def __init__(self, year, station, endyear=None, path=""):
        try:
            datetime.datetime.strptime(year, '%Y')
            year = int(year)
            if not endyear:
                endyear = year
            else:
                datetime.datetime.strptime(endyear, '%Y')
                endyear = int(endyear)
        except ValueError:
            exit('The year was not entered in the correct format of yyyy')
        if path:
            if not os.path.exists(path):
                exit('The specified path does not exist')
            elif os.path.isfile(path):
                path = os.path.split(path)[0]

        origin = year
        while int(year) <= int(endyear):
            self.read_in_archive_file(year, path, station)
            self.fill_in_blanks(year, endyear, station)
            year += 1

    def read_in_archive_file(self, year, path, station):
        connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
        try:
            dbconn = psycopg2.connect(connection)
            cur = dbconn.cursor()
        except:
            exit('Connection to the database could not be established')

        cur.execute("SELECT id FROM srewstations WHERE id = (%s);", (station,))
        if cur.fetchone() is None:
            exit(station + " is not a valid station")

    #for each day/fil
        countEntered = 0
        countIgnored = 0
        file_path = os.path.join(path, 'midas_rainhrly_' + str(year) +'01-' + str(year) +'12' + '.txt')
        if not os.path.isfile(file_path):
            exit("The directory specified does not contain the expected midas SREW file. Expecting file named " + os.path.basename(file_path))

        with open(file_path, 'r') as csvFile:
            data = csv.reader(csvFile, delimiter=',')
            reading = {}

            for row in data:
                if row[1].lstrip() == station:
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

        print('Reached end of file for ' + str(year))
        print('Number of entries: ' + str(countEntered))
        print('Number ignored: ' + str(countIgnored))

    def fill_in_blanks(self, start, end, station):
        connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
        try:
            dbconn = psycopg2.connect(connection)
            cur = dbconn.cursor()
        except:
            exit('Connection to the database could not be established')

        #get the last recording in the database for the station
        cur.execute("SELECT datetime FROM hourlysrewreading WHERE date_part('year', datetime) = (%s) AND stationid = (%s) ORDER BY datetime DESC LIMIT 1;", (start,station))
        endpoint = cur.fetchone()[0]
        #get the first recording for the given year and station
        startpoint = datetime.datetime.strptime(str(start), '%Y')
        cur.execute("SELECT datetime FROM hourlysrewreading WHERE stationid = (%s) AND datetime >= (%s) AND datetime <= (%s) ORDER BY datetime ASC ",(station, startpoint, endpoint))
        result = cur.fetchall()

        for p in result:
            if p[0] != startpoint:
                reading = {}
                reading['id'] = station
                reading['datetime'] = startpoint
                reading['value'] = None
                try:
                    sql = "INSERT INTO hourlysrewreading" \
                          "(datetime, stationid, value)" \
                          "VALUES (%(datetime)s, %(id)s, %(value)s);"
                    cur.execute(sql, reading)
                    dbconn.commit()
                    print("Entered a null val", reading['datetime'])
                except psycopg2.IntegrityError:
                    dbconn.rollback()

            startpoint += datetime.timedelta(hours=1)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='argument handler')
    parser.add_argument('year', help='Start year to read in, yyyy format')
    parser.add_argument('station', help='The station for which to read in the readings for')
    parser.add_argument('--endyear', '-e', required=False, help='Last year to read in, yyyy format')
    parser.add_argument('--path', '-p', required=False, help='The directory containing the data file(s)')
    args = parser.parse_args()

    IngestSrew(args.year, args.station, args.endyear, args.path)




