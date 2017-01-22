#!/usr/bin/python
# Read a NIMROD file
# Look up all the meanings of the header values in the NIMORD format document available at the BADC
# Charles Kilburn Aug 2008

import os, stat, re, sys, time
import struct, array

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
    result = []
    minEasting = -4045500.0
    maxNorthing = 1549500.0
    s = 1000 # easting resolution

    i = 0
    j = 0
    count = 0
    for d in data:
        count += 1
        if i == 1725:
            i = 0
            j += 1
        easting = minEasting + i * s
        northing = maxNorthing - j * s
        i += 1

        print(str(easting) + ',' + str(northing))
        result.append(str(easting) + ',' + str(northing))
    print(count)

    import csv
    with open('file.csv' , 'wb') as file:
        writer = csv.writer(file)
        for r in result:
            coord = r.split(',', 1)
            writer.writerow([coord[0], coord[1]])
except:
    print "Read failed"

file.close()



