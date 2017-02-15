from pybrain.datasets import SupervisedDataSet
from pybrain.tools.shortcuts import buildNetwork
from pybrain.supervised.trainers import BackpropTrainer

ds = SupervisedDataSet(17,6)

tf = open('output1-no-nimrod.csv','r')

for line in tf.readlines():
    data = [float(x) for x in line.strip().split(',') if x != '']
    indata =  tuple(data[:17])
    outdata = tuple(data[17:])
    ds.addSample(indata,outdata)

n = buildNetwork(ds.indim,22,8,ds.outdim,recurrent=True)
print(n['in'])
print(n['hidden0'])
print(n['hidden1'])
print(n['hidden2'])
print(n['out'])
print(n.activate(indata))
t = BackpropTrainer(n,learningrate=0.01,momentum=0.5,verbose=True)
t.trainOnDataset(ds,5200)
t.testOnData(verbose=True)




