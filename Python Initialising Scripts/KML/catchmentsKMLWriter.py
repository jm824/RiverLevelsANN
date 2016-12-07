import os
from xml.etree import ElementTree
from bngToLatLong import OSGB36toWGS84

file_name = 'WFD_River_Waterbody_Catchments_Cycle2.gml'
full_file = os.path.abspath(os.path.join('data', file_name))

dom = ElementTree.parse(full_file)
#coords = dom.findall('boundedBy/Box/coordinates')
polygon = dom.findall('featureMember/WFD_River_Waterbody_Catchments/multiPolygonProperty/MultiPolygon/polygonMember/Polygon/outerBoundaryIs/LinearRing/coordinates')

count = 0
insideCount = 0
for p in polygon: #for each polygon
    polyString = ""
    coordSet = p.text.split()
    count+=1
    for c in coordSet: #for each set of polygon points
        coords = c.split(',')
        coords = [float(i) for i in coords] #change to type float
        returnedVal = OSGB36toWGS84(coords[0],coords[1])
        insideCount+=1
        polyString += str(returnedVal[0]) + "," + str(returnedVal[1]) + " "
    p.text = polyString

dom.write('glm2.xml')

#print(count)
#print(insideCount)


