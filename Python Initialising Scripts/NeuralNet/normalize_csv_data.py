import pandas as pd

df = pd.read_csv("cat1_2016_basic_12hour.csv", header=None)

maxvals = []
minvals = []
for column in df:
    max = df[column].max()
    min = df[column].min()
    maxvals.append(max)
    minvals.append(min)
    count = 0
    for num in df[column]:
        num = (num-min)/(max-min)
        df.set_value(count, column, num)
        count += 1
file = open('cat1_2016_basic_12hour.csv_min_max.txt', 'w')
first = True
for item in maxvals:
    if first:
        first = False
        file.write("%s" % item)
    else:
        file.write(",%s" % item)
file.write("\n")

first = True
for item in minvals:
    if first:
        first = False
        file.write("%s" % item)
    else:
        file.write(",%s" % item)
df.to_csv("cat1_2016_basic_12hour.csv_normalized.csv",header=False, index=False)



