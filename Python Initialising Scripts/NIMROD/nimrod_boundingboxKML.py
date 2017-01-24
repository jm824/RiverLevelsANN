#!/usr/bin/python
# Read a NIMROD file
# Look up all the meanings of the header values in the NIMORD format document available at the BADC
# Charles Kilburn Aug 2008
#Edits made by James Morrison 2017

import os, stat, re, sys, time
import struct, array
import simplekml
import bnglatlon

pathed_file = '201601010030_nimrod_ng_radar_rainrate_composite_1km_UK'


file_id = open(pathed_file,"rb")
record_length, = struct.unpack(">l", file_id.read(4))
if record_length != 512: raise "Unexpected record length", record_length

gen_ints = array.array("h")
gen_reals = array.array("f")
spec_reals = array.array("f")
characters = array.array("c")
spec_ints = array.array("h") #the actual data?

gen_ints.read(file_id, 31)
gen_ints.byteswap()

gen_reals.read(file_id, 28)
gen_reals.byteswap()

spec_reals.read(file_id, 45)
spec_reals.byteswap()
characters.read(file_id, 56)
spec_ints.read(file_id, 51)
spec_ints.byteswap()

record_length, = struct.unpack(">l", file_id.read(4))

if record_length != 512: raise "Unexpected record length", record_length
chars = characters.tostring()

#Read the Data
array_size = gen_ints[15] * gen_ints[16]

record_length, = struct.unpack(">l", file_id.read(4))
if record_length != array_size * 2: raise "Unexpected record length", record_length

data = array.array("h")
try:
    data.read(file_id, array_size)
    record_length, = struct.unpack(">l", file_id.read(4))
    if record_length != array_size * 2: raise "Unexpected record length", record_length
    data.byteswap()

    #James' edits to get coords for each array entry
    minEasting = -404500.0
    maxNorthing = 1549500.0
    se = -404500.0/1000  # easting resolution
    sn = 1549500.0/1000  # northing resolutio
    #se = 1000/1725 #easting resolution
    #sn = 1000/2175 #northing resolutio

    kml = simplekml.Kml()
    c = 0
    i = 0
    j = 0
    count = 0
    for d in data:
        count += 1

	if i == 0 or j == 0 or i == 1724 or j == 2174: 
		easting = minEasting + i * 1000 + 1000/2
		northing = maxNorthing - j * 1000 - 1000/2
		print(easting, northing)
		lat, lon =  bnglatlon.OSGB36toWGS84(easting, northing)
		kml.newpoint(name="ChubbyRain", coords=[(lon,lat)])

        i += 1
        if i == 1725:
            	i = 0
        	j += 1
	    	print(j)


    print(count)
    kml.save("chubby_rains.kml")

except:
    print "Read failed"

file_id.close()


