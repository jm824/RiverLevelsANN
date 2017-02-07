import nimrod_reader
import datetime
import psycopg2

"""
This script reads through the database and creates an entry for readings that are missing, setting
the value to NULL
"""

class ReadInNimrod:
    def __init__(self, startdatetime, enddatetime, srewstation):
        self.station = srewstation
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

        self.fill_in_blanks()

    def fill_in_blanks(self):
        while self.startdatetime.time() <= self.enddatetime.time() and self.startdatetime.date() <= self.enddatetime.date():
            self.cur.execute("SELECT value FROM hourlysrewreading WHERE stationid = (%s) AND datetime = (%s)",(self.station,self.startdatetime,))
            result = self.cur.fetchall()
            if not result:
                reading = {}
                reading['id'] = self.station
                reading['datetime'] = self.startdatetime
                reading['value'] = None
                try:
                    sql = "INSERT INTO hourlysrewreading" \
                          "(datetime, stationid, value)" \
                          "VALUES (%(datetime)s, %(id)s, %(value)s);"
                    self.cur.execute(sql, reading)
                    self.dbconn.commit()
                except psycopg2.IntegrityError:
                    self.dbconn.rollback()

            self.startdatetime += datetime.timedelta(hours=1)


obj = ReadInNimrod('17-11-2015 00:00', '31-12-2016 23:00', '457096')




