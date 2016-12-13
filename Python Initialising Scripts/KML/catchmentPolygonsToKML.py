import os
from xml.etree import ElementTree
from bngToLatLong import OSGB36toWGS84
import simplekml

"""
One time script to take the water body catchment polygons plot them in KML.
This script only needs to be run once.

This reads data from a static file and creates a static kml file in return. This file can be read
straight  into Google Earth
"""

#Read in the polygon catchment data
file_name = 'WFD_River_Waterbody_Catchments_Cycle2.gml'
full_file = os.path.abspath(os.path.join('data', file_name))
tree = ElementTree.parse(full_file)
polygon = tree.findall('featureMember/WFD_River_Waterbody_Catchments/polygonProperty/Polygon/outerBoundaryIs/LinearRing/coordinates')
kml = simplekml.Kml()
for p in polygon:  #for each polygon
    polygon = []
    coordSet = p.text.split()
    for c in coordSet:  #for each set of polygon points, convert them
        coords = c.split(',')
        coords = [float(i) for i in coords]  # change to type float
        returnedVal = OSGB36toWGS84(coords[0], coords[1])
        polygon.append(returnedVal)
    #add translated polygon to new kml
    pol = kml.newpolygon(name='Polygon')
    pol.outerboundaryis = polygon

kml.save('data\catchmentPolygons.kml')