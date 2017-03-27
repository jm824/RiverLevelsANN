import nimrod
import datetime
import psycopg2
import os

"""
Script to read data from NIMROD files and store in the database.
This script takes start and end dates to read in data for.
Each nimrod file is 5 mins worth of data.

Currently this script first looks in the database at all the catchments currently 'registered'.
Then it will read in the data for each of these catchments between the dates specified.

This scrip relies on the fact that the data files follow the naming convention yyy-mm-ddhh:mm e.g. 201606010000'
"""

class ReadInNimrod:
    def __init__(self, startdatetime, enddatetime, path):
        #Check the datetimes are provided in the correct format
        try:
            self.startdatetime = datetime.datetime.strptime(startdatetime, '%d-%m-%Y %H:%M')
            self.enddatetime = datetime.datetime.strptime(enddatetime, '%d-%m-%Y %H:%M')
        except:
            exit('One or more given date was not in the correct format or dd-mm-yyyy hh:mm')
        #Check the directory given exsists
        if path:
            if not os.path.exists(path):
                exit('The directory specified does not exist')
            self.path = path
        else: self.path = ''

        # Try to establish handle to the database
        connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
        try:
            self.dbconn = psycopg2.connect(connection)
            self.cur = self.dbconn.cursor()
        except:
            exit('Connection to the database could not be established')

        #Currently retrieve all catchments we currently have listed in the database
        # TODO Make it so we are reading in the nimrod data for a specified catchment only
        self.cur.execute("SELECT id, raincells FROM catchments")
        self.datapoints = self.cur.fetchall()
        self.ingest()

    #Read in a nimrod file for a given datetime and read the data into the database for the given catchments
    def ingest(self):
        while self.startdatetime.time() <= self.enddatetime.time() and self.startdatetime.date() <= self.enddatetime.date():
            try:
                file = os.path.join(self.path, datetime.datetime.strftime(self.startdatetime,'%Y%m%d%H%M'))
                radar = nimrod.Nimrod(file)
                for catchment in self.datapoints:  # for each catchment
                    data = []
                    for p in catchment[1]:
                        x, y = p.split(' ', 1)
                        data.append(radar.get_cell_data(int(x), int(y)))
                    # insert into database
                    reading = {}
                    reading['catchment'] = catchment[0]
                    reading['datetime'] = self.startdatetime
                    reading['readings'] = data

                    try:
                        sql = "INSERT INTO nimrodreading" \
                              "(id, datetime, data)" \
                              "VALUES (%(catchment)s, %(datetime)s, %(readings)s);"
                        self.cur.execute(sql, reading)
                        self.dbconn.commit()
                    except psycopg2.IntegrityError:
                        self.dbconn.rollback()
                        print(self.startdatetime, 'for', reading['catchment'], 'is already in the database.')

            #If the file is missing then enter the data as NULL for each catchment
            #WARNING If the wrong directory is pointed to NULL values will still be entered into the database
            except FileNotFoundError:
                for catchment in self.datapoints:
                    reading = {}
                    reading['catchment'] = catchment[0]
                    reading['datetime'] = self.startdatetime
                    reading['readings'] = None

                    try:
                        sql = "INSERT INTO nimrodreading" \
                                     "(id, datetime, data)" \
                                     "VALUES (%(catchment)s, %(datetime)s, %(readings)s);"
                        self.cur.execute(sql, reading)
                        self.dbconn.commit()
                        print('Entered null reading for', reading['catchment'], self.startdatetime)
                    except psycopg2.IntegrityError:
                        print('Reading', self.startdatetime, 'for', reading['catchment'], 'is already in the database.')
                        self.dbconn.rollback()

            self.startdatetime += datetime.timedelta(minutes=5)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='argument handler')
    parser.add_argument('startdatetime', help='The start datetime to start reading in readings for. dd-mm-yyyy hh:mm format')
    parser.add_argument('enddatetime', help='The end datetime to stop reading in readings for. dd-mm-yyyy hh:mm format')
    parser.add_argument('--path', '-p', required=False, help='The directory containing the data file(s)')
    args = parser.parse_args()

    ReadInNimrod(args.startdatetime, args.enddatetime, args.path)




