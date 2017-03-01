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
    def __init__(self, starttime, endtime, measure, outputfile, outputhour):
        try:
            self.startTime = datetime.datetime.strptime(starttime, '%d-%m-%Y-%H%M')
            self.endTime = datetime.datetime.strptime(endtime,'%d-%m-%Y-%H%M')
        except:
            exit("Start time and/or end time was provided in an incorrect format. Correct format is dd-mm-yyyy-hhmm")

        self.gaugemeasure = measure
        self.epoch = outputhour
        self.srewstation = None
        self.catchment = None
        self.nimrodcells = None
        self.inputrow = []

        #Try to establish handle to the database
        connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
        try:
            dbconn = psycopg2.connect(connection)
            self.cur = dbconn.cursor()
        except:
            print('Connection to the database could not be established')

        self.read_config() #Read in all the required reference data about the catchment
        with open(outputfile, 'w', newline='') as csvfile: #Open a file to write output to
            writer = csv.writer(csvfile, delimiter=',')

            count = 0
            #Here we are reading in hour by hour
            while self.startTime < self.endTime:
                pastgauge = self.ingest_past_measure_readings(self.startTime)
                if pastgauge:
                    srew = self.ingest_srew(self.startTime)
                    futuregauge = self.ingest_future_measure_readings(self.startTime)
                    if srew and futuregauge: #If we got all the required data from the db for this epoch
                        self.inputrow = pastgauge + srew + futuregauge
                        count += 1
                        writer.writerow(self.inputrow)
                self.startTime += datetime.timedelta(hours=1)
                #print(count)

            print(count)
        csvfile.close()

    #read in the catchment details from the database using the catchment's measure as reference
    def read_config(self):
        self.cur.execute("SELECT id, raincells, srewstation FROM catchments WHERE measure = %s", (self.gaugemeasure,))
        result = self.cur.fetchall()
        if result:
            self.catchment = result[0][0]
            self.nimrodcells = result[0][1]
            self.srewstation = result[0][2]
        else:
            exit("No record found for the given river measure.")

    def ingest_srew(self, start):
        values = []
        bottomtimewindow = start - datetime.timedelta(hours=720)

        self.cur.execute("SELECT value FROM hourlysrewreading " \
            "WHERE stationid = %s" \
            "AND datetime < %s" \
            "AND datetime >= %s" \
            "ORDER BY datetime DESC", (self.srewstation,datetime.datetime.strftime(start,'%d-%m-%Y %H:%M'),datetime.datetime.strftime(bottomtimewindow,'%d-%m-%Y %H:%M'),))

        result = self.cur.fetchall()
        if len(result) != 720:
            print("wrong len")

        bounds = [0, 2, 4, 6,8,10,12, 24, 36, 48, 168, 336]  #intervals at which to collate readings

        for i in range(len(bounds)-1):
            total = 0
            for tuple in result[bounds[i]:bounds[i + 1]]:
                total += tuple[0]
            values.append(total)

        return values

    def ingest_past_measure_readings(self, start):
        bottomtimewindow = start - datetime.timedelta(hours=4)
        values = []

        #Get the hourly readings going back 6 hours from start (not including start readings which is considered 1 hour into the future)
        self.cur.execute("SELECT value, datetime FROM hourlygaugereading " \
                         "WHERE measureid = %s" \
                            "AND extract('minutes' from datetime) = 0" \
                            "AND datetime < %s" \
                            "AND datetime >= %s" \
                         "ORDER BY datetime DESC", (
                         self.gaugemeasure, datetime.datetime.strftime(start, '%d-%m-%Y %H:%M'),
                         datetime.datetime.strftime(bottomtimewindow, '%d-%m-%Y %H:%M'),))

        result = self.cur.fetchall()
        for r in result: #Check for missing readings and attempt to correct/substitute
            if not r[0]: #if we have a null result for a gauge reading
                    values = None
                    return values #else this time point cannot be used to train with due to missing values - return null list
            else: #else put the value in the list, they are valid
                values.append(r[0])

        return values


    #Collect the future readings from the epoch. These are the actual values that we are predicting
    def ingest_future_measure_readings(self, start):
        timetopredict = start + datetime.timedelta(hours=self.epoch -1)
        values = []

        #Retrieve the future reading from the database using the provided epoch
        self.cur.execute("SELECT value FROM hourlygaugereading " \
                         "WHERE measureid = %s" \
                         "AND extract('minutes' from datetime) = 0" \
                         "AND datetime = %s", (
                             self.gaugemeasure, datetime.datetime.strftime(timetopredict, '%d-%m-%Y %H:%M'),))
        result = self.cur.fetchall()

        if not result[0][0]: #if we have a null result for the gauge reading at given epoch
            values = None
            return values
        else:
            values.append(result[0][0])
            return values

obj = DatasetBuilder('01-01-2017-0000', '31-01-2017-1255', '2133-level-stage-i-15_min-mASD', 'cat1_jan2017_slim_12hour.csv' ,12)






