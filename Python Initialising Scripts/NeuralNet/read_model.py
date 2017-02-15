from pybrain.datasets import SupervisedDataSet
from pybrain.supervised.trainers import BackpropTrainer
from pybrain.tools.customxml.networkreader import NetworkReader
import csv

net = NetworkReader.readFrom('network4-6hour-single2.xml')

with open('2017-verification-dataset.csv') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',')
    for row in csvreader:
        print(net.activate(row))


