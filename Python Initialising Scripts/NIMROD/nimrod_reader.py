#!/usr/bin/python
# Read a NIMROD file
# Look up all the meanings of the header values in the NIMORD format document available at the BADC
# Charles Kilburn Aug 2008
# Edits made by James Morrison 2017

import struct, array
import os


class NimrodData:
    def __init__(self, date):
        self.date = date
        self.data = None
        self.read_in_file(date)

    def read_in_file(self, date):
        path = 'H:/NIMROD/2017/Jan/'
        file_id = open(path + date, "rb")
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

        #Read the Data
        array_size = gen_ints[15] * gen_ints[16]

        record_length, = struct.unpack(">l", file_id.read(4))

        data = array.array("h")
        try:
            data.fromfile(file_id, array_size)
            record_length, = struct.unpack(">l", file_id.read(4))
            if record_length != array_size * 2: raise ("Unexpected record length", record_length)
            data.byteswap()
            #print('read in file')
            self.data = data

        except:
            print("Read failed")

        file_id.close()

    #1725 2175
    #3751875
    def get_cell_data(self, x, y):
        if x > 1724 or y > 2174:
            return -2
        return self.data[(y * 1725) + x]

