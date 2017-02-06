#!/usr/bin/python
# Read a NIMROD file
# Look up all the meanings of the header values in the NIMORD format document available at the BADC
# Charles Kilburn Aug 2008
# Edits made by James Morrison 2017

"""
Most of this code was not written by me but rather Charles Kilburn.
This is a script provided by the Met Office to read NIMROD files which has been edited to better
read the data.
"""

import struct, array
import simplekml
import bngToLatLong
import csv

class nimrod:
    def __init__(self):
        self.data = None

        pathed_file = 'data/201601010030_nimrod_ng_radar_rainrate_composite_1km_UK'
        file_id = open(pathed_file, "rb")
        record_length, = struct.unpack(">l", file_id.read(4))
        if record_length != 512: raise ("Unexpected record length", record_length)

        gen_ints = array.array("h")
        gen_reals = array.array("f")
        spec_reals = array.array("f")
        characters = array.array("b")
        spec_ints = array.array("h")  # the actual data?

        gen_ints.fromfile(file_id, 31)
        gen_ints.byteswap()

        gen_reals.fromfile(file_id, 28)
        gen_reals.byteswap()

        spec_reals.fromfile(file_id, 45)
        spec_reals.byteswap()
        characters.fromfile(file_id, 56)
        spec_ints.fromfile(file_id, 51)
        spec_ints.byteswap()

        record_length, = struct.unpack(">l", file_id.read(4))

        # Read the Data
        array_size = gen_ints[15] * gen_ints[16]

        record_length, = struct.unpack(">l", file_id.read(4))

        data = array.array("h")
        try:
            data.fromfile(file_id, array_size)
            record_length, = struct.unpack(">l", file_id.read(4))
            if record_length != array_size * 2: raise ("Unexpected record length", record_length)
            data.byteswap()
            self.data = data
            file_id.close()

        except:
            print("Read failed")

        #self.plot_bounding_box()


    def plot_bounding_box(self):
        # James' edits to get coords for each array entry
        minEasting = -404500.0
        maxNorthing = 1549500.0

        kml = simplekml.Kml()
        i = 0
        j = 0
        for d in self.data:
            if i == 0 or j == 0 or i == 1724 or j == 2174:
                easting = minEasting + i * 1000 + 1000 / 2
                northing = maxNorthing - j * 1000 - 1000 / 2
                lat, lon = bngToLatLong.OSGB36toWGS84(easting, northing)
                kml.newpoint(name=str(i) + ',' + str(j), coords=[(lon, lat)])
            i += 1
            if i == 1725:
                i = 0
                j += 1
        kml.save("data/rainRadarBoundingBox.kml")

    def create_coord_matrix(self):
        with open('data/nimrodXY.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')

            minEasting = -404500.0
            maxNorthing = 1549500.0
            i = 0
            j = 0
            row = []
            for d in self.data:
                easting = minEasting + i * 1000 + 1000 / 2
                northing = maxNorthing - j * 1000 - 1000 / 2
                row.append(str(i) + ',' + str(j))
                i += 1
                #if end of row
                if i == 1725:
                    #write to csv row
                    writer.writerow(row)
                    row = []
                    i = 0
                    j += 1

    def create_latlong_matrix(self):
        with open('data/nimrodLatLon.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')

            minEasting = -404500.0
            maxNorthing = 1549500.0
            i = 0
            j = 0
            row = []
            for d in self.data:
                easting = minEasting + i * 1000 + 1000 / 2
                northing = maxNorthing - j * 1000 - 1000 / 2
                lat, lon = bngToLatLong.OSGB36toWGS84(easting, northing)
                row.append(str(lat) + ',' + str(lon))
                i += 1
                #if end of row
                if i == 1725:
                    #write to csv row
                    writer.writerow(row)
                    row = []
                    i = 0
                    j += 1

    def plot_sub_boundingbox(self,minx, maxx, miny , maxy):
        minEasting = -404500.0
        maxNorthing = 1549500.0

        kml = simplekml.Kml()
        i = 0
        j = 0
        for d in self.data:
            if i >= minx and i <= maxx and j >= miny and j <= maxy:
                easting = minEasting + i * 1000 + 1000 / 2
                northing = maxNorthing - j * 1000 - 1000 / 2
                lat, lon = bngToLatLong.OSGB36toWGS84(easting, northing)
                kml.newpoint(name=str(i) + ',' + str(j), coords=[(lon, lat)])
            i += 1
            if i == 1725:
                i = 0
                j += 1
        kml.save("data/rainRadarCustomBoundingBox.kml")




