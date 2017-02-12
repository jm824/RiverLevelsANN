import nimrod_reader
import datetime
import csv
import psycopg2

"""
This python module attempts to build a training set in csv format ready to train an artificial neural
network. It does this by readings in the specific catchment info for a individual neural network
problem and then pulls the data from the database and nimrod files.

This is done by this scrip in an attempt to better separate out the process and so the neural network
does not need to be pulling data from the database and disk while training. Instead the network can
be trained from a single csv file. This also vastly reduces training time. This script can better
handle the training data which contains missing values and is not always consistent. Ultimately this
gives us better control over the training data.
"""

class DatasetBuilder:
    def __init__(self, starttime, endtime, catchmenfile, outputfile):
        self.catchmentDetails = catchmenfile
        self.startTime = datetime.datetime.strptime(starttime, '%d-%m-%Y-%H%M')
        self.endTime = datetime.datetime.strptime(endtime,'%d-%m-%Y-%H%M')
        self.read_config()
        self.gaugemeasure = None
        self.srewstation = None
        self.catchment = None
        self.inputrow = []

        #Try to establish handle to the database
        connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
        try:
            dbconn = psycopg2.connect(connection)
            self.cur = dbconn.cursor()
        except:
            print('Connection to the database could not be established')

        self.read_config()
        with open(outputfile, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')

            count = 0
            while self.startTime < self.endTime:
                pastriver = self.ingest_past_measure_readings(self.startTime)
                if pastriver:
                    srew = self.ingest_srew(self.startTime)
                    rain = self.ingest_nimrod(self.startTime)
                    futureriver = self.ingest_future_measure_readings(self.startTime)
                if srew and rain and futureriver:
                    self.inputrow = pastriver + srew + rain + futureriver
                    count += 1
                    writer.writerow(self.inputrow)
                self.startTime += datetime.timedelta(hours=1)
                print(count)

            print(count)

    #read in the csv file which contains info about the catchment
    def read_config(self):
        with open('data/' + self.catchmentDetails) as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',')
            row = next(csvreader)
            self.gaugemeasure = row[0]
            self.srewstation = row[1]
            self.catchment = row[2]

    def ingest_nimrod(self, start):
        values = []
        bottomtimewindow = start - datetime.timedelta(hours=336)

        self.cur.execute("SELECT data FROM nimrodreading " \
                         "WHERE id = %s" \
                         "AND datetime < %s" \
                         "AND datetime >= %s" \
                         "ORDER BY datetime DESC", (
                         self.catchment, datetime.datetime.strftime(start, '%d-%m-%Y %H:%M'),
                         datetime.datetime.strftime(bottomtimewindow, '%d-%m-%Y %H:%M'),))

        result = self.cur.fetchall()

        bounds = [0,12,24,48,60,72,144,288,432,786,2016,4032] #intervals at which to collate readings
        #1,2,3,4,5,6,6-12,12-24,24-36,36-48,48-1week,1week-2week

        #check data for missing periods within the first 5 hours
        missing = 0
        for r in result[0:72]:  # check how many missing 5 min periods we have in the first 5 hours
            if not r[0]:
                missing += 1
            if missing >= 12:
                values = None
                return values
        # check data for missing periods the rest of the two week period allowing for 16 hours of missing data
        missing = 0
        for r in result[72:4032]:
            if not r[0]:
                missing += 1
            if missing >= 200:
                values = None
                return values

        #collate and load data into list
        for i in range(len(bounds)-1):

            total = 0
            for tuple in result[bounds[i]:bounds[i + 1]]: #for each input node
                if tuple[0]:
                    for value in tuple[0]:
                        total += value/32
            values.append(total)

        return values

    def ingest_srew(self, start):
        values = []
        bottomtimewindow = start - datetime.timedelta(hours=336)

        self.cur.execute("SELECT value FROM hourlysrewreading " \
            "WHERE stationid = %s" \
            "AND datetime < %s" \
            "AND datetime >= %s" \
            "ORDER BY datetime DESC", (self.srewstation,datetime.datetime.strftime(start,'%d-%m-%Y %H:%M'),datetime.datetime.strftime(bottomtimewindow,'%d-%m-%Y %H:%M'),))

        result = self.cur.fetchall()

        bounds = [0, 1, 2, 3, 4, 5, 12, 24, 36, 48, 168, 336]  # intervals at which to collate readings

        for i in range(len(bounds)-1):
            total = 0
            for tuple in result[bounds[i]:bounds[i + 1]]:
                total += tuple[0]
            values.append(total)

        return values

    def ingest_past_measure_readings(self, start):
        bottomtimewindow = start - datetime.timedelta(hours=6)
        values = []

        #Get the hourly readings going back 6 hours from timenow (not including timenow readings)
        self.cur.execute("SELECT value, datetime FROM hourlygaugereading " \
                         "WHERE measureid = %s" \
                            "AND extract('minutes' from datetime) = 0" \
                            "AND datetime < %s" \
                            "AND datetime >= %s" \
                         "ORDER BY datetime DESC", (
                         self.gaugemeasure, datetime.datetime.strftime(start, '%d-%m-%Y %H:%M'),
                         datetime.datetime.strftime(bottomtimewindow, '%d-%m-%Y %H:%M'),))

        result = self.cur.fetchall()
        for r in result:
            if not r[0]: #if we have a null result for a gauge reading
                #get readings 15 mins behind and ahead
                self.cur.execute("SELECT value FROM hourlygaugereading " \
                                 "WHERE measureid = %s" \
                                 "AND datetime >= %s" \
                                 "AND datetime <= %s" \
                                 "AND datetime != %s" \
                                 "ORDER BY datetime ASC", (
                                     self.gaugemeasure, datetime.datetime.strftime(r[1] - datetime.timedelta(minutes=15), '%d-%m-%Y %H:%M'),
                                     datetime.datetime.strftime(r[1] + datetime.timedelta(minutes=15), '%d-%m-%Y %H:%M'),datetime.datetime.strftime(r[1], '%d-%m-%Y %H:%M'),))
                offset = self.cur.fetchall()
                offsetval = None
                for p in offset:
                    if p[0]:
                        offsetval = p[0]
                if offsetval:
                    values.append(offsetval)
                else:
                    values = None
                    return values #else this time point cannot be used to train with due to missing values
            else: #else put the value in the list
                values.append(r[0])

        return values

    def ingest_future_measure_readings(self, start):
        bottomtimewindow = start + datetime.timedelta(hours=6)
        values = []

        #Get the hourly readings going back 6 hours from timenow (not including timenow readings)
        self.cur.execute("SELECT value, datetime FROM hourlygaugereading " \
                         "WHERE measureid = %s" \
                            "AND extract('minutes' from datetime) = 0" \
                            "AND datetime >= %s" \
                            "AND datetime < %s" \
                         "ORDER BY datetime ASC", (
                         self.gaugemeasure, datetime.datetime.strftime(start, '%d-%m-%Y %H:%M'),
                         datetime.datetime.strftime(bottomtimewindow, '%d-%m-%Y %H:%M'),))

        result = self.cur.fetchall()
        for r in result:
            if not r[0]: #if we have a null result for a gauge reading
                    values = None
                    return values #else this time point cannot be used to train with due to missing values
            else: #else put the value in the list
                values.append(r[0])

        return values

obj = DatasetBuilder('01-01-2017-0000','31-01-2017-2355','catchment1.csv','cat1_jan2017')





