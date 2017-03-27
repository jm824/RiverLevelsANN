#!/usr/bin/python
# Read a NIMROD file
# Look up all the meanings of the header values in the NIMORD format document available at the BADC
# Charles Kilburn Aug 2008
# Edits made by James Morrison 2017

"""
Most of this code was not written by me but rather Charles Kilburn.
This is a script provided by the Met Office to read NIMROD files which has been edited to better
read the data.

This script now reads in a nimrod file where upon there are several functions to do different
things with the nimrod data.

The location to the file must be provided as an argument.
"""

import struct, array
import simplekml
import bng_to_latlong
import csv
import os

class Nimrod:
    def __init__(self, file):
        if not os.path.isfile(file):
            raise FileNotFoundError

        self.data = None
        self.read_in_file(file)

    #Code to read in the data from a nimrod binary file (not my code but adapted)
    def read_in_file(self, location):
        file_id = open(location, "rb")
        record_length, = struct.unpack(">l", file_id.read(4))
        if record_length != 512: raise ("Unexpected record length", record_length)

        gen_ints = array.array("h")
        gen_reals = array.array("f")
        spec_reals = array.array("f")
        characters = array.array("b")
        spec_ints = array.array("h")  # the actual data matrix

        gen_ints.fromfile(file_id, 31)
        gen_ints.byteswap()

        gen_reals.fromfile(file_id, 28)
        gen_reals.byteswap()

        spec_reals.fromfile(file_id, 45)
        spec_reals.byteswap()
        characters.fromfile(file_id, 56)
        spec_ints.fromfile(file_id, 51)
        spec_ints.byteswap()

        struct.unpack(">l", file_id.read(4))

        # Read the Data
        array_size = gen_ints[15] * gen_ints[16]

        struct.unpack(">l", file_id.read(4))

        data = array.array("h")
        try:
            data.fromfile(file_id, array_size)
            record_length, = struct.unpack(">l", file_id.read(4))
            if record_length != array_size * 2: raise ("Unexpected record length", record_length)
            data.byteswap()
            self.data = data

        except:
            print("Read failed")

        file_id.close()

    #Create an KML file which is the bounding box for the NIMROD radar. This can be read into Google Earch
    #Note that clipping takes place from the bng_to_latlong script
    def plot_bounding_box(self, output):
        if not output.endswith('.kml'):
            exit('A .kml file must be specified.')
        if not os.path.exists(os.path.dirname(output)):
            exit('Cannot save file to location that does not exist')

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
                lat, lon = bng_to_latlong .OSGB36toWGS84(easting, northing)
                kml.newpoint(name=str(i) + ',' + str(j), coords=[(lon, lat)])
            i += 1
            if i == 1725:
                i = 0
                j += 1
        kml.save(output)

    #Create a .csv file which is a matrix of the x,y positions of each 1km rain cell
    def create_coord_matrix(self, output):
        if not output.endswith('.csv'):
            exit('A csv file must be specified.')
        if not os.path.exists(os.path.dirname(output)):
            exit('Cannot save file to location that does not exist')

        try:
            with open(output, 'w', newline='') as csvfile:
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
        except PermissionError:
            exit('File name not specified.')

    #Create a .csv file which is a matrix of the lat,long positions of each 1km rain cell.
    #Also converts the lat lon to be in WGS84 lat long from BNG (OSGB36)
    def create_latlong_matrix(self, output):
        if not output.endswith('.csv'):
            exit('A csv file must be specified.')
        if not os.path.exists(os.path.dirname(output)):
            exit('Cannot save file to location that does not exist')

        with open(output, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')

            minEasting = -404500.0
            maxNorthing = 1549500.0
            i = 0
            j = 0
            row = []
            for d in self.data:
                easting = minEasting + i * 1000 + 1000 / 2
                northing = maxNorthing - j * 1000 - 1000 / 2
                lat, lon = bng_to_latlong.OSGB36toWGS84(easting, northing)
                row.append(str(lat) + ',' + str(lon))
                i += 1
                #if end of row
                if i == 1725:
                    #write to csv row
                    writer.writerow(row)
                    row = []
                    i = 0
                    j += 1

    #Same as plot_bound_box() but allows you to specfify your own boundaries as x,y coords
    def plot_sub_boundingbox(self,minx, maxx, miny , maxy, output):
        if not output.endswith('.kml'):
            exit('A .kml file must be specified.')
        if not os.path.exists(os.path.dirname(output)):
            exit('Cannot save file to location that does not exist')

        minEasting = -404500.0
        maxNorthing = 1549500.0

        kml = simplekml.Kml()
        i = 0
        j = 0
        for d in self.data:
            if i >= minx and i <= maxx and j >= miny and j <= maxy:
                easting = minEasting + i * 1000 + 1000 / 2
                northing = maxNorthing - j * 1000 - 1000 / 2
                lat, lon = bng_to_latlong.OSGB36toWGS84(easting, northing)
                kml.newpoint(name=str(i) + ',' + str(j), coords=[(lon, lat)])
            i += 1
            if i == 1725:
                i = 0
                j += 1
        kml.save(output)

    #For the given nimrod file return the data value for the given x,y pos
    def get_cell_data(self, x, y):
        if x > 1724 or y > 2174: #if outside the matrix
            return -2
        return self.data[(y * 1725) + x]