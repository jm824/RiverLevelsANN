import os
from xml.etree import ElementTree
from bngToLatLong import OSGB36toWGS84
import simplekml

"""
One time script to take the water body catchment polygons plot them in KML.
This script only needs to be run once to generate the KML file.

This reads data from a static file and creates a static kml file in return. This file can then be read
straight into Google Earth
"""

#Read in the polygon catchment data
def polygons_to_kml(filename, output):

    if not os.path.exists(os.path.dirname(output)):
        exit('Cannot save file to location that does not exist')
    if not os.path.isfile(filename):
        exit('File not found.')

    #Read and convery GML
    tree = ElementTree.parse(filename)
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

    kml.save(output)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='argument handler')
    parser.add_argument('filename', help='Path of file to read in. Can be relative or absolute path')
    parser.add_argument('output', help='The output file name and location')
    args = parser.parse_args()

    polygons_to_kml(args.filename, args.output)