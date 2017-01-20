#!/usr/bin/python
# Read a NIMROD file
# Look up all the meanings of the header values in the NIMORD format document available at the BADC
# Charles Kilburn Aug 2008

import os, stat, re, sys, time
import struct, array

pathed_file = '201601010030_nimrod_ng_radar_rainrate_composite_1km_UK'

print "Input file is", pathed_file

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
print "\nDate %4.4d%2.2d%2.2d Time %2.2d:%2.2d Grid %d x %d" %(gen_ints[0], gen_ints[1], gen_ints[2], gen_ints[3], gen_ints[4], gen_ints[15], gen_ints[16])

gen_reals.read(file_id, 28)
gen_reals.byteswap()
print "start northing %.1f, row interval %.1f, start easting %.1f, column interval %.1f\n"  %(gen_reals[2], gen_reals[3], gen_reals[4], gen_reals[5])

spec_reals.read(file_id, 45)
spec_reals.byteswap()
characters.read(file_id, 56)
spec_ints.read(file_id, 51)
spec_ints.byteswap()

record_length, = struct.unpack(">l", file_id.read(4))
if record_length != 512: raise "Unexpected record length", record_length

for i in range(len(gen_ints)): print i+1, gen_ints[i]
for i in range(len(gen_reals)): print i+32, gen_reals[i]
chars = characters.tostring()
print "Units are", chars[0:8]
print "Data source is", chars[8:32]
print "Parameter is", chars[32:55]
for i in range(gen_ints[22]): print i+108, spec_ints[i]

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
    print "First 100 values are", data[0:99]
except:
    print "Read failed"


file_id.close()


