from pybrain.tools.customxml.networkreader import NetworkReader
import datetime as dt
import csv
import os
import psycopg2

"""
This script loads a trained neural net and a data set to run through the neural net.

The data must be provided in the correct format (produced by the dataset_builder.py script).
The data will be read in and a prediction made. The data is then entered into the local database
for storage.

NB: Unlike the chat plotting script the data must always be de-normalized before entry into the
database so that the database contains only actual river level values.
"""

def write_to_database(catchment,model, data_file, minmax):
    # check the model provided exists
    if not os.path.isfile(model):
        exit('Model file not found.')

    # check the data file exists
    if not os.path.isfile(data_file):
        exit('Data file not found.')

    # check the minmax file exists
    if not os.path.isfile(minmax):
        exit('minmax file not found.')

    # establish a connection to the database
    connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
    try:
        dbconn = psycopg2.connect(connection)
        cur = dbconn.cursor()
    except:
        exit('Connection to the database could not be established')

    cur.execute("SELECT measure FROM catchments " \
                "WHERE id = %s;", (catchment,))

    measure = cur.fetchall()
    if not measure:
        exit('The provided catchment is invalid.')

    # read the model into pybrain
    net = NetworkReader.readFrom(model)

    # read in the csv data to run through the model
    with open(data_file) as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',')
        datetime = []
        value = []
        count = 0
        # load the data ready to be plotted on the chart
        for row in csvreader:
            # Work out what hour this prediction is for by reading metadata
            if count == 0:
                hour = row[-2:-1]
            datetime.append(row[-1])
            value.append(net.activate(row[:-3]))
            count += 1

        datetime = [dt.datetime.strptime(d, '%Y-%m-%d %H:%M:%S') for d in datetime]

        # read in the min max values used to normalize the data - data must be de-normalised before entry
        #into database
        with open(minmax) as denorm:
            minmax = []
            for line in denorm:
                lin = line.split(",")
                minmax.append(float(lin[-1].strip("\n")))

        # go through values and de-normalise for better visualisation
        for plot in value:
            plot[0] = float(plot[0]) * (minmax[0] - minmax[1]) + minmax[1]

        # combine predictions and associated datetimes
        data = tuple(zip(datetime, value))

        # for each prediction created
        for pairs in data:
            # construct tuple
            reading = {}
            reading['dateTime'] = pairs[0]
            reading['measureid'] = measure[0][0]
            reading['hoursahead'] = hour[0]
            reading['value'] = pairs[1][0]

            # attempt to add tuple into database
            try:
                readingSQL = "INSERT INTO riverlevelpredictions" \
                             "(dateTime, measureid, hoursahead, value)" \
                             "VALUES (%(dateTime)s, %(measureid)s, %(hoursahead)s, %(value)s);"
                cur.execute(readingSQL, reading)
                dbconn.commit()
            except:
                exit('Error adding data to database')

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='argument handler')
    parser.add_argument('catchment', help='The catchment the prediction is being made for')
    parser.add_argument('model', help='A neural network (pybrain) model as a .xml file')
    parser.add_argument('data_file', help='The .csv file containing the data to activate the model with')
    parser.add_argument('minmax', help='The .txt file containing the minmax values used to de-normalize the data')
    args = parser.parse_args()

    write_to_database(args.catchment, args.model, args.data_file, args.minmax)