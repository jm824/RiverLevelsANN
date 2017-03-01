import pandas as pd
from itertools import islice

df = pd.read_csv("BASIC/verification/cat1_jan2017_basic_12hour.csv", header=None)

max = []
min = []

#read in min/max col vals from training data
with open('BASIC/cat1_2016_basic_12hour.csv_min_max.txt') as fin:
    for line in islice(fin, 1):
        max = line.split(",")
        max[len(max)-1] = max[len(max)-1].strip("\n")

    for line in islice(fin, 2):
        min = line.split(",")
        min[len(min) - 1] = min[len(min) - 1].strip("\n")

colcount = 0
for column in df:
    colmax = float(max[colcount])
    colmin = float(min[colcount])
    count = 0
    for num in df[column]: #for each number in the col
        num = (num-colmin)/(colmax-colmin)
        df.set_value(count, column, num)
        count += 1
    colcount += 1

df.to_csv("cat1_jan2017_basic_12hour_normalized.csv",header=False, index=False)



