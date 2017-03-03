import nimrod
import datetime
import psycopg2

"""
Script to read data from NIMROD files and store in the database.
This script takes start and end dates to read in data for.
"""

class ReadInNimrod:
    def __init__(self, startdatetime, enddatetime):
        try:
            self.startdatetime = datetime.datetime.strptime(startdatetime, '%d-%m-%Y %H:%M')
            self.enddatetime = datetime.datetime.strptime(enddatetime, '%d-%m-%Y %H:%M')
        except:
            exit('One or more provided date was not in the correct format or dd-mm-yyyy hh-mm')

        # Try to establish handle to the database
        connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
        try:
            self.dbconn = psycopg2.connect(connection)
            self.cur = self.dbconn.cursor()
        except:
            print('Connection to the database could not be established')

        #Currently retrieve all catchments we currently have listed in the database
        # TODO Make it so we are reading in the nimrod data for a specified catchment only
        self.cur.execute("SELECT id, raincells FROM catchments")
        self.datapoints = self.cur.fetchall()
        self.ingest()

    def ingest(self):
        while self.startdatetime.time() <= self.enddatetime.time() and self.startdatetime.date() <= self.enddatetime.date():
            try:
                radar = nimrod.Nimrod(datetime.datetime.strftime(self.startdatetime,'%Y%m%d%H%M'))
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
            except psycopg2.IntegrityError:
                self.dbconn.rollback()

            self.startdatetime += datetime.timedelta(minutes=5)


obj = ReadInNimrod('01-01-2017 00:00', '31-01-2017 23:55')




