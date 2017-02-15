from pybrain.structure import FeedForwardNetwork
from pybrain.structure import LinearLayer, SigmoidLayer
from pybrain.structure import FullConnection
from pybrain.datasets import SupervisedDataSet
from pybrain.supervised.trainers import BackpropTrainer
from pybrain.tools.customxml.networkwriter import NetworkWriter

ds = SupervisedDataSet(17,6)
tf = open('output1-no-nimrod-normalized.csv','r')
for line in tf.readlines():
    data = [float(x) for x in line.strip().split(',') if x != '']
    indata = tuple(data[:17])
    outdata = tuple(data[17:])
    ds.addSample(indata,outdata)

#build feedforward network ready for training
net = FeedForwardNetwork()
inputLayer = LinearLayer(17)
hiddenLayer1 = SigmoidLayer(17)
hiddenLayer2 = SigmoidLayer(8)
outputLayer = LinearLayer(6)

net.addInputModule(inputLayer)
net.addModule(hiddenLayer1)
net.addModule(hiddenLayer2)
net.addOutputModule(outputLayer)

in_to_hidden1 = FullConnection(inputLayer, hiddenLayer1)
hidden1_to_hidden2 = FullConnection(hiddenLayer1, hiddenLayer2)
hidden2_to_out = FullConnection(hiddenLayer2, outputLayer)

net.addConnection(in_to_hidden1)
net.addConnection(hidden1_to_hidden2)
net.addConnection(hidden2_to_out)
net.sortModules()

t = BackpropTrainer(net,learningrate=0.01,momentum=0.5,verbose=True)
t.trainOnDataset(ds,5000)
NetworkWriter.writeToFile(net, 'network3.xml')
t.testOnData(verbose=True)

