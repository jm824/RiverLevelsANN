import datetime
import psycopg2
import csv
import os

"""
Script to read in data from the archived river level recordings csv files and put into database.
Specify the start and end dates (inclusive)
Optional - specify the directory containing the archive files
"""

class MeasureReadingIngestor:
    def __init__(self, startDate, endDate, measure, path=None):
        #check provided dates are in correct format
        try:
            startDate = datetime.datetime.strptime(startDate, '%d-%m-%Y')
            endDate = datetime.datetime.strptime(endDate, '%d-%m-%Y')
        except ValueError:
            exit('The dates were not entered in the correct format of dd-mm-yyyy')
        #check the path provided exsists and that it is not pointing to a file
        if path:
            if not os.path.exists(path):
                exit('The specified path does not exist')
            elif os.path.isfile(path):
                exit('Specify the directory containing the file(s), not a file itself')
        else: #else use script directory as default
            path = ''

        #establish a connection to the database
        connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
        try:
            self.dbconn = psycopg2.connect(connection)
            self.cur = self.dbconn.cursor()
        except:
            exit('Connection to the database could not be established')

        # Check if the measure provided is in the database
        self.cur.execute("SELECT id FROM gaugemeasure WHERE id = (%s);", (measure,))
        if self.cur.fetchone() is None:
            exit(measure + " is not a valid measure")

        # for each file (day)
        while (startDate != endDate + datetime.timedelta(1)):
            self.read_in_file(startDate, path, measure)
            self.fill_in_blanks(startDate, measure)
            startDate = startDate + datetime.timedelta(1)

    #method to open a file and enter select contents into the database
    def read_in_file(self, startDate, path, measure):
        currentDate = startDate.strftime('%Y-%m-%d')
        countEntered = 0
        countIgnored = 0
        file_path = os.path.join(path, 'riverLevelArchive_' + currentDate +'.csv')
        if not os.path.isfile(file_path):
            exit('No such file ' + file_path)
        with open(file_path, 'r') as csvFile:
            data = csv.reader(csvFile, delimiter=',')
            reading = {}
            for row in data: #go through each data row in the file
                if row[0] == 'dateTime': continue #skip heading row
                if row[1].split('/')[-1] == measure: #Check the row is for the measure specified
                    reading['dateTime'] = row[0]
                    reading['measureId'] = row[1].split('/')[-1]
                    reading['value'] = row[2]

                    try:
                        readingSQL = "INSERT INTO hourlyGaugeReading" \
                                     "(dateTime, measureId, value)" \
                                     "VALUES (%(dateTime)s, %(measureId)s, %(value)s);"
                        self.cur.execute(readingSQL, reading)
                        countEntered +=1
                        self.dbconn.commit()
                    except psycopg2.DataError:
                        #Reach here if the format of the reading is not correct
                        countIgnored +=1
                        self.dbconn.rollback()
                    except psycopg2.IntegrityError:
                        #Reach here if either the reading does not have a station or if it is already entered
                        countIgnored += 1
                        self.dbconn.rollback()

        print('Reached end of file for', currentDate)
        print('Number of entries:', str(countEntered))
        print('Number ignored:',str(countIgnored))

    #method to go back through past days worth of entered data and enter NULL values for any missing readings
    def fill_in_blanks(self, dt, measure):
        end = dt + datetime.timedelta(1)
        while dt < end:
            self.cur.execute("SELECT value FROM hourlygaugereading WHERE measureid = (%s) AND datetime = (%s)", (measure, dt,))
            result = self.cur.fetchall()
            if not result:
                reading = {}
                reading['id'] = measure
                reading['datetime'] = dt
                reading['value'] = None
                try:
                    sql = "INSERT INTO hourlygaugereading" \
                          "(datetime, measureid, value)" \
                          "VALUES (%(datetime)s, %(id)s, %(value)s);"
                    self.cur.execute(sql, reading)
                    self.dbconn.commit()
                    print('Added NULL entry for', dt)
                except psycopg2.IntegrityError:
                    self.dbconn.rollback()
                    exit('Error while entering null reading ' + dt)

            dt += datetime.timedelta(minutes=15)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='argument handler')
    parser.add_argument('start', help='Start date in dd-mm-yyyy format')
    parser.add_argument('end', help='End date in dd-mm-yyyy format')
    parser.add_argument('measure', help='The measure to ingest readings for')
    parser.add_argument('--path', '-p', required=False, help='The directory containing the data file(s)')
    args = parser.parse_args()

    MeasureReadingIngestor(args.start, args.end, args.measure, args.path)




