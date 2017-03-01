import matplotlib.pyplot as plt
from pybrain.tools.customxml.networkreader import NetworkReader
import csv

"""
This script that loads a trained neural net and data to activate (put through network).
The prediction results are then plotted on a graph next to the actual recordings.
"""

net = NetworkReader.readFrom('Trained_networks/cat1_2016_basic_9hour.csv_normalized.xml')

with open('BASIC/verification/cat1_jan2017_basic_12hour_normalized.csv') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',')
    x = []
    y = []
    x2 = []
    y2 = []
    count = 0
    #load the data ready to be plotted on the chart
    for row in csvreader:
        count += 1
        x.append(count)
        y.append(row[-1:])
        x2.append(count)
        y2.append(net.activate(row[:-1]))

    #read in the min max values used to normalize the data
    with open('BASIC/cat1_2016_basic_12hour.csv_min_max.txt') as denorm:
        minmax = []
        for line in denorm:
            lin = line.split(",")
            minmax.append(float(lin[-1].strip("\n")))

    # go through both lists and denormalise for better visualisation
    for plot in y:
        plot[0] = float(plot[0]) * (minmax[0] - minmax[1]) + minmax[1]
    for plot in y2:
        plot[0] = float(plot[0]) * (minmax[0] - minmax[1]) + minmax[1]

    plt.figure(figsize=(10, 8))
    plt.plot(x, y, label='Actual readings')
    plt.plot(x2, y2, label='Predicted readings')
    plt.xlabel('Epoch', size=25)
    plt.ylabel('Water Level (meters)', size = 25)
    plt.title('+12 hours January 2017 - predicted vs actual', size=30)
    plt.tick_params(axis='x', labelsize=15)
    plt.tick_params(axis='y', labelsize=15)
    plt.rc('legend', fontsize=25)

    plt.legend()
    plt.show()

