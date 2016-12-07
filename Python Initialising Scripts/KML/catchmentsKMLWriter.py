import os
from xml.etree import ElementTree
from bngToLatLong import OSGB36toWGS84

#Read in the polygon catchment data
file_name = 'WFD_River_Waterbody_Catchments_Cycle2.gml'
full_file = os.path.abspath(os.path.join('data', file_name))
tree = ElementTree.parse(full_file)
polygon = tree.findall('featureMember/WFD_River_Waterbody_Catchments/polygonProperty/Polygon/outerBoundaryIs/LinearRing/coordinates')

for p in polygon:  # for each polygon
    polyString = ""
    coordSet = p.text.split()
    for c in coordSet:  # for each set of polygon points, convert them
        coords = c.split(',')
        coords = [float(i) for i in coords]  # change to type float
        returnedVal = OSGB36toWGS84(coords[0], coords[1])
        polyString += str(returnedVal[1]) + "," + str(returnedVal[0]) + " "
    #edit the xml to include the new polygon in latlong
    p.text = polyString

tree.write('WFD_River_Waterbody_Catchments_Cycle2_in_latlong.kml')