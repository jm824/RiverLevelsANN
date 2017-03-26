import datetime
import csv
import psycopg2
import os

"""
This script is for building data sets both to train and test a neural network.

This python module attempts to build a training set in csv format ready to train an artificial neural
network. It does this by readings in the specific catchment info for a individual neural network
problem and then pulls the data from the database and nimrod files.

This is done by this scrip in an attempt to better separate out the process and so the neural network
does not need to be pulling data from the database and disk while training. Instead the network can
be trained from a single csv file loaded into memory. This also vastly reduces training time. This script can better
handle the training data which contains missing values and is not always consistent. Ultimately this
gives better control over the training data.

After a dataset is built the .csv file produced is read back in and a normalized version created along
with a .txt file which contains the min and max values of each column in order to allow for de-normalization
of the data later. Alternatively a minmax.txt file can be provided and its values used to normalize the
data.

In total this script creates 3 files or 2 if a minmax.txt file is provided.
"""

class DatasetBuilder:
    def __init__(self, starttime, endtime, measure, outputhour, outputfile, minmaxfile=None):
        try:
            #The starttime is the point in past data we want to start traning from
            #The starttime is also the last known reading relative to making a prediction (origin)
            self.startTime = datetime.datetime.strptime(starttime, '%d-%m-%Y-%H%M')
            #The endtime is the last point in past data we want to use as training data
            self.endTime = datetime.datetime.strptime(endtime,'%d-%m-%Y-%H%M')
        except:
            exit("Start time and/or end time was provided in an incorrect format. Correct format is dd-mm-yyyy-hhmm")

        #Check filename and path provided are valid
        if os.path.dirname(outputfile):
            if not os.path.exists(os.path.dirname(outputfile)):
                exit('Cannot save file to location that does not exist')
        if not outputfile.endswith('.csv'):
            exit('Specified file must be a .csv')

        #Check epoch given is a integer between 1 and 12
        try:
            outputhour = int(outputhour)
            if outputhour < 1 or outputhour > 12: raise ValueError
        except ValueError:
            exit('epoch provided was not a integer between 1 and 12 inclusive')

        #check if minmaxfile is valid
        if minmaxfile:
            if not os.path.isfile(minmaxfile):
                exit('Cannot find the provided min max .txt file')
            if not minmaxfile.endswith('.txt'):
                exit('min max file must be a .txt')

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
            exit('Connection to the database could not be established')

        self.read_config() #Read in all the required reference data about the catchment
        with open(outputfile, 'w', newline='') as csvfile: #Open a file to write output to
            writer = csv.writer(csvfile, delimiter=',')

            count = 0
            #Here we are creating the traning set as a sliding window, moving forward one hour for each traning instance
            while self.startTime < self.endTime:
                pastgauge = self.ingest_past_measure_readings(self.startTime)
                if pastgauge:
                    srew = self.ingest_srew(self.startTime)
                    nimrod = self.ingest_nimrod(self.startTime)
                    futuregauge = self.ingest_future_measure_readings(self.startTime)
                    if srew and futuregauge and nimrod: #If we got all the required data from the db for this epoch
                        self.inputrow = pastgauge + srew + nimrod + futuregauge
                        count += 1
                        writer.writerow(self.inputrow)
                self.startTime += datetime.timedelta(hours=1)

            print(count, 'training instances created')
        csvfile.close()

        #At this point we have created a training data file but will now create a version where the
        #data is normalized

        import pandas as pd
        from itertools import islice

        #if a file contaning the min max value of each col is provided then use it to normalize the data
        if minmaxfile:
            df = pd.read_csv(outputfile, header=None)
            max = []
            min = []
            # read in min/max col vals from training data
            with open(minmaxfile) as fin:
                for line in islice(fin, 1):
                    max = line.split(",")
                    max[len(max) - 1] = max[len(max) - 1].strip("\n")

                for line in islice(fin, 2):
                    min = line.split(",")
                    min[len(min) - 1] = min[len(min) - 1].strip("\n")

            colcount = 0
            for column in df:
                if colcount == df.shape[1] - 2: #ignore the last two colunms are they are metadata for another purpose
                    break
                colmax = float(max[colcount])
                colmin = float(min[colcount])
                count = 0
                for num in df[column]:  # for each number in the col
                    num = (num - colmin) / (colmax - colmin)
                    df.set_value(count, column, num)
                    count += 1
                colcount += 1

                df.to_csv(os.path.splitext(outputfile)[0] + "_normalized.csv", header=False, index=False)
        #else calculate the min max value of each col and use that to normalize the data
        else:
            #Read the traning file back in
            df = pd.read_csv(outputfile, header=None)

            maxvals = []
            minvals = []
            count = 0
            for column in df: #calculate the min and max of each col
                if count == df.shape[1] - 2: #ignore the last two colunms are they are metadata for another purpose
                    break
                max = df[column].max()
                min = df[column].min()
                maxvals.append(max)
                minvals.append(min)
                count += 1

                celcount = 0
                for num in df[column]: #normalize each value in the col
                    num = (num - min) / (max - min)
                    df.set_value(celcount, column, num)
                    celcount += 1

            #save the min max values of each colunm in a text file
            file = open(os.path.splitext(outputfile)[0] + '_min_max.txt', 'w')
            first = True
            for item in maxvals:
                if first:
                    first = False
                    file.write("%s" % item)
                else:
                    file.write(",%s" % item)
            file.write("\n")

            #save the normalized dataset using the same name with _normalized appended
            first = True
            for item in minvals:
                if first:
                    first = False
                    file.write("%s" % item)
                else:
                    file.write(",%s" % item)
            df.to_csv(os.path.splitext(outputfile)[0] + "_normalized.csv", header=False, index=False)

    #read in the catchment details from the database using the catchment's measure as reference
    def read_config(self):
        self.cur.execute("SELECT id, raincells, srewstation FROM catchments WHERE measure = %s", (self.gaugemeasure,))
        result = self.cur.fetchall()
        if result:
            self.catchment = result[0][0]
            self.nimrodcells = result[0][1]
            self.srewstation = result[0][2]
        else:
            exit("No record found for the given river measure. Please ensure the measure is valid.")

    #read in nimrod data from database for the time periods given
    #Get data from time given going 12 hours back
    def ingest_nimrod(self, start):
        values = []
        bottomtimewindow = start - datetime.timedelta(hours=12)

        self.cur.execute("SELECT data FROM nimrodreading " \
                         "WHERE id = %s" \
                         "AND datetime < %s" \
                         "AND datetime >= %s" \
                         "ORDER BY datetime DESC", (
                         self.catchment, datetime.datetime.strftime(start, '%d-%m-%Y %H:%M'),
                         datetime.datetime.strftime(bottomtimewindow, '%d-%m-%Y %H:%M'),))

        result = self.cur.fetchall()

        if not result: #check if the result is null or not
            values = None
            return values

        bounds = [0,12,24,36,48,60,72,84,96,108,120] #these are the windows to which readings are collated

        values = []
        # for each bound -1
        for i in range(len(bounds) - 1):
            missing = 0
            hourlyprecip = 0
            for scan in result[bounds[i]:bounds[i + 1]]:  #for each scan of each tuple pair making up an hour
                if not scan[0]: #if 5min scan missing
                    missing += 1
                    if missing >= 2:  #if there are too many missing readings then abort, allow 2*5min missing for each hour
                        values = None
                        return values
                else: #else total up each 5 min period for the hour
                    scanavg = 0
                    for cell in scan[0]: #for each 1km grid cell in the catchment
                        if cell < 0: cell = 0
                        scanavg += cell
                    hourlyprecip += (scanavg/32)/len(scan[0]) #for each 5min scan of catchment calculate average rainfall rate
            values.append(hourlyprecip)

        return values

    #read in srew data for the given time period stretching back 30 days
    def ingest_srew(self, start):
        values = []
        bottomtimewindow = start - datetime.timedelta(hours=720)

        #get srew readings from time given stretching back 720 hours
        self.cur.execute("SELECT value FROM hourlysrewreading " \
            "WHERE stationid = %s" \
            "AND datetime < %s" \
            "AND datetime >= %s" \
            "ORDER BY datetime DESC", (self.srewstation,datetime.datetime.strftime(start,'%d-%m-%Y %H:%M'),datetime.datetime.strftime(bottomtimewindow,'%d-%m-%Y %H:%M'),))

        result = self.cur.fetchall()

        bounds = [0, 1, 2, 3, 4, 5, 6,7,8,9,10,11,12, 24, 36, 48, 168, 336, 720]  #intervals at which to collate readings

        #for each collation period total the amount of rainfall and stick in the the values list to be returned.
        #we return 18 srew values collated as shown above
        for i in range(len(bounds)-1):
            total = 0
            for tuple in result[bounds[i]:bounds[i + 1]]:
                if not tuple[0]:
                    total += 0
                else:
                    total += tuple[0]
            values.append(total)

        return values

    #read in gauge data for given time period stretching back 6 hours. These are the past river levels.
    def ingest_past_measure_readings(self, start):
        bottomtimewindow = start - datetime.timedelta(hours=6)
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
        for r in result: #for each reading...
            # Check for missing readings and attempt to correct/substitute
            if not r[0]: #if we have a null result for a gauge reading
                #get readings 15 mins behind and ahead to see if we can substitute
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
                    return values #else this time point cannot be used to train with due to missing values - return null list

            else: #else if not missing then put in value list to be returned
                values.append(r[0])

        return values


    #Collect the future readings from the epoch. These are the actual values that we are predicting (used for validation)
    def ingest_future_measure_readings(self, start):
        timetopredict = start + datetime.timedelta(hours=self.epoch) #offset last taken reading by epoch to get time to predict
        values = []

        #Retrieve the future reading from the database using the provided epoch
        self.cur.execute("SELECT value, datetime FROM hourlygaugereading " \
                         "WHERE measureid = %s" \
                         "AND extract('minutes' from datetime) = 0" \
                         "AND datetime = %s", (
                             self.gaugemeasure, datetime.datetime.strftime(timetopredict, '%d-%m-%Y %H:%M'),))
        result = self.cur.fetchall()

        if not result[0][0]: #if we have a null result for the gauge reading at given epoch
            values = None
            return values #by returning null we will void this epoch (wont be used for traning)
        else:
            values.append(result[0][0])
            values.append(self.epoch)
            values.append(result[0][1])
            return values

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='argument handler')
    parser.add_argument('startdatetime', help='The start datetime to start reading in readings for. dd-mm-yyyy hh:mm format')
    parser.add_argument('enddatetime', help='The end datetime to stop reading in readings for. dd-mm-yyyy hh:mm format')
    parser.add_argument('measure', help='The river measure to create a traning set for')
    parser.add_argument('predictionepoch', help='The number of hours into the future to which to predict the river level for. Must be integer between 1 and 12 inclusive')
    parser.add_argument('output', help='The path and filename of the csv output (training data file)')
    parser.add_argument('--minmax', '-m', required=False, help='The path to the max min .txt file for normalization')
    args = parser.parse_args()

    DatasetBuilder(args.startdatetime, args.enddatetime, args.measure, args.predictionepoch, args.output, args.minmax)





