from pybrain.structure import FeedForwardNetwork
from pybrain.structure import LinearLayer, SigmoidLayer
from pybrain.structure import FullConnection
from pybrain.datasets import SupervisedDataSet
from pybrain.supervised.trainers import BackpropTrainer
from pybrain.tools.customxml.networkwriter import NetworkWriter
import os

"""
This is the script that produces a neural network (model)

Script to build, train and save a neural network as an XML file. This model can then be read back into
memory and have new data run through it to predict future river levels. This current network architecture
is for single node output e.g. predicting the river level at +6 hours.

Currently the neural networks are trained from a built training data set. This is passed in as an argument.
The values for the node counts of both hidden layers can be provided but are best left as default unless
you are experimenting. The verify flag should only be used when using new training data format or new network
architecture.

This script will output a .xml file that is the trained network. Training may take 10 hours or more.
"""

class TrainNeuralNet():
    def __init__(self, dataset, output, h1nodecount=None, h2nodecount=None, verify=False):
        if not os.path.isfile(dataset):
            exit('Cannot find training data file ' + dataset)

        #Check output filename and path provided are valid
        if os.path.dirname(output):
            if not os.path.exists(os.path.dirname(output)):
                exit('Cannot save file to location that does not exist')
        if not output.endswith('.xml'):
            exit('Specified file must be a .xml')

        #Check the hidden node counts given are valid
        if h1nodecount:
            try:
                h1nodecount = int(h1nodecount)
            except ValueError:
                exit('h1 node count needs to be an integer')
        else:
            h1nodecount = 34
        if h2nodecount:
            try:
                h2nodecount = int(h2nodecount)
            except ValueError:
                exit('h2 node count needs to be an integer')
        else:
            h2nodecount = 16

        #read in training data set
        tf = open(dataset,'r')
        x = tf.tell()
        imputnum = len(tf.readline().split(','))-3 #-3 to miss of  the two end cols which are metadata
        tf.seek(x)
        ds = SupervisedDataSet(imputnum, 1)
        for line in tf.readlines():
            data = [str(x) for x in line.strip().split(',') if x != '']
            data = data[:-2] #remove metadata from list
            [float(i) for i in data]
            indata = tuple(data[:imputnum])
            outdata = tuple(data[imputnum:])
            ds.addSample(indata,outdata)

        #build feedforward network ready for training (construct layers)
        net = FeedForwardNetwork()
        inputLayer = LinearLayer(imputnum)
        hiddenLayer1 = SigmoidLayer(h1nodecount)
        hiddenLayer2 = SigmoidLayer(h2nodecount)
        outputLayer = LinearLayer(1)

        #add layers to network
        net.addInputModule(inputLayer)
        net.addModule(hiddenLayer1)
        net.addModule(hiddenLayer2)
        net.addOutputModule(outputLayer)

        #wire up layers in correct order
        in_to_hidden1 = FullConnection(inputLayer, hiddenLayer1)
        hidden1_to_hidden2 = FullConnection(hiddenLayer1, hiddenLayer2)
        hidden2_to_out = FullConnection(hiddenLayer2, outputLayer)
        net.addConnection(in_to_hidden1)
        net.addConnection(hidden1_to_hidden2)
        net.addConnection(hidden2_to_out)
        net.sortModules()

        #create a trainer object - use back propagation
        t = BackpropTrainer(net,learningrate=0.01,momentum=0.1,verbose=True)
        #Start training on provided dataset
        t.trainOnDataset(ds,ds.getLength())
        #Write trained network to xml file
        NetworkWriter.writeToFile(net, output)

        if verify: #test on provided test dataset or cross validate on traning data
            t.testOnData(verbose=True)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='argument handler')
    parser.add_argument('trainingdata', help='The path to the training data csv file')
    parser.add_argument('output', help='The path and name of the output file (.xml model)')
    parser.add_argument('--hidden1', '-h1', required=False, help='The number of nodes in the 1st hidden layer')
    parser.add_argument('--hidden2', '-h2', required=False, help='The number of nodes in the 2nd hidden layer')
    parser.add_argument('-verify', '-v', required=False, action='store_true', help='Flag to enable validation')
    args = parser.parse_args()

    TrainNeuralNet(args.trainingdata, args.output, args.hidden1, args.hidden2, args.verify)

