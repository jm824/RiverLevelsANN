import os
import csv
from xml.etree import ElementTree
from bngToLatLong import OSGB36toWGS84


folder = ElementTree.Element("Folder")
name2 = ElementTree.SubElement(folder, "name")
name2.text = "Srew Stations"

tree = ElementTree.ElementTree(folder) #the actual xml document as a whole

#Read in csv data
with open('file.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        Placemark = ElementTree.SubElement(folder, "Placemark")
        name = ElementTree.SubElement(Placemark, "name")
        style = ElementTree.SubElement(Placemark, "styleUrl")
        style.text = "#icon-1899-0288D1-nodesc"
        point = ElementTree.SubElement(Placemark, "Point")
        coordinates = ElementTree.SubElement(point, "coordinates")
        returnedVal = OSGB36toWGS84(float(row[0]), float(row[1]))
        coordinates.text = str(returnedVal[1]) + ',' + str(returnedVal[0])
        name.text = 'test'

tree.write('nimrod.kml')




