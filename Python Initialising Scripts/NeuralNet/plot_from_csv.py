import matplotlib.pyplot as plt
import csv

"""
This script that reads a two column CSV file and plots both columns on a line graph
"""

with open('Analysis/6_hour.csv') as csvfile:
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
        y.append(row[0])
        x2.append(count)
        y2.append(row[1])

    plt.figure(figsize=(10, 8))
    plt.plot(x, y, label='Actual readings')
    plt.plot(x2, y2, label='Predicted readings')
    plt.xlabel('Epoch', size=25)
    plt.ylabel('Water Level (meters)', size = 25)
    plt.title('+6 hour January 2017 - predicted vs actual', size=30)
    plt.tick_params(axis='x', labelsize=15)
    plt.tick_params(axis='y', labelsize=15)
    plt.rc('legend', fontsize=25)

    plt.legend()
    plt.show()

