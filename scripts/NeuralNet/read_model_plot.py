import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pybrain.tools.customxml.networkreader import NetworkReader
import csv
import datetime as dt
import os
import matplotlib.ticker as mtick

"""
This script loads a trained neural net and a data set to run through the neural net.
This is for verification (evaluating a model).

The data must be provided in the correct format (produced by the dataset_builder.py script).
The data will be read in and a prediction made for each epoch. A optional minman file can be
provided which will be used to de-normalize the neural net output so that the 'real' predictions
are shown.

A chart is then produced showing the predicted level vs the actual reading.
"""

def run_model(model, data, minmax = None):
    #check the model provided exists
    if not os.path.isfile(model):
        exit('Model file not found.')

    #check the data file exists
    if not os.path.isfile(data):
        exit('Data file not found.')

    #check the minmax file exists
    if minmax:
        if not os.path.isfile(minmax):
            exit('minmax file not found.')

    #read the model into pybrain
    net = NetworkReader.readFrom(model)

    #read in the csv data to run through the model
    with open(data) as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',')
        x = []
        y = []
        x2 = []
        y2 = []
        count = 0
        #load the data ready to be plotted on the chart
        for row in csvreader:
            #Work out what hour this prediction is for by reading metadata
            if count == 0:
                hour = row[-2:-1]
            x.append(row[-1])
            y.append(row[-3:-2])
            y2.append(net.activate(row[:-3]))
            count += 1

        x = [dt.datetime.strptime(d, '%Y-%m-%d %H:%M:%S') for d in x]

        #if a minmax file is provided then denormalize the data before plotting
        if minmax:
            #read in the min max values used to normalize the data
            with open(minmax) as denorm:
                minmax = []
                for line in denorm:
                    lin = line.split(",")
                    minmax.append(float(lin[-1].strip("\n")))

            # go through both lists and denormalise for better visualisation
            for plot in y:
                plot[0] = float(plot[0]) * (minmax[0] - minmax[1]) + minmax[1]
            for plot in y2:
                plot[0] = float(plot[0]) * (minmax[0] - minmax[1]) + minmax[1]

        #Format chart and axis scales etc
        plt.figure(figsize=(8, 10))
        plt.plot(x, y, label='Actual readings')
        plt.plot(x, y2, label='Predicted readings')
        plt.xlabel('Date & Time')
        plt.ylabel('Water Level (meters)')
        plt.title('+' + hour[0] + 'hours: Predicted vs Actual')
        plt.tick_params(axis='x')
        plt.tick_params(axis='y')
        plt.xticks(rotation='vertical')
        plt.rc('legend')
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y %H:%M'))
        #plt.gca().yaxis.set_major_formatter(mtick.FormatStrFormatter('%.3f'))
        plt.gcf().autofmt_xdate()

        #draw chart
        plt.legend()
        plt.show()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='argument handler')
    parser.add_argument('model', help='A neural network (pybrain) model as a .xml file')
    parser.add_argument('data_file', help='The .csv file containing the data to activate the model with')
    parser.add_argument('--minmax', '-m', required = False, help='The .txt file containing the minmax values used to de-normalize the data')
    args = parser.parse_args()

    run_model(args.model, args.data_file, args.minmax)

