import csv
from xml.etree import ElementTree

"""
One time script to take the locations of each SREW stations and plot them in KML.
This script only needs to be run once.

This reads data from a static file and creates a static kml file in return. This file can be read
straight  into Google Earth
"""

folder = ElementTree.Element("Folder")
name2 = ElementTree.SubElement(folder, "name")
name2.text = "Srew Stations"

tree = ElementTree.ElementTree(folder) #the actual xml document as a whole

#Read in csv data
with open('data/srewStationsWithLatLong&Id.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        Placemark = ElementTree.SubElement(folder, "Placemark")
        name = ElementTree.SubElement(Placemark, "name")
        style = ElementTree.SubElement(Placemark, "styleUrl")
        style.text = "#icon-1899-0288D1-nodesc"
        point = ElementTree.SubElement(Placemark, "Point")
        coordinates = ElementTree.SubElement(point, "coordinates")
        coordinates.text = row[2] + ',' + row[1]
        name.text = row[0]

tree.write('data\SREWStations.kml')




