import datetime
import psycopg2
import csv

connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
try:
    dbconn = psycopg2.connect(connection)
    cur = dbconn.cursor()
except:
    print('Connection to the database could not be established')

#starting date to read from
startDate = datetime.date(2016, 11, 1)

#for each day/file
while(startDate.strftime('%Y-%m-%d') != '2017-01-19'):
    currentDate = startDate.strftime('%Y-%m-%d')

    countEntered = 0
    countIgnored = 0
    with open('data/riverGauges/archivedRiverLevelRecordings/riverLevelArchive_' + currentDate +'.csv', 'r') as csvFile:
        data = csv.reader(csvFile, delimiter=',')
        reading = {}
        for row in data:
            if row[0] == 'dateTime': continue #skip heading row
            time = row[0].split(':',1)
            try:
                if time[1] != '00:00Z': continue #skip readings not on the hour
            except IndexError:
                print(row)
                exit()
            if row[1].split('/')[-1] != 'E47061-level-stage-i-15_min-mASD': continue
            reading['dateTime'] = row[0]
            reading['measureId'] = row[1].split('/')[-1]
            reading['value'] = row[2]

            try:
                readingSQL = "INSERT INTO HourlyGaugeReading" \
                             "(dateTime, measureId, value)" \
                             "VALUES (%(dateTime)s, %(measureId)s, %(value)s);"
                cur.execute(readingSQL, reading)
                countEntered +=1
                dbconn.commit()
            except psycopg2.DataError:
                #Reach here if the format of the reading is not correct
                #print('DATA ERROR: ' + reading['measureId'])
                countIgnored +=1
                dbconn.rollback()
            except psycopg2.IntegrityError:
                #Reach here if either the reading does not have a station or if it is already entered
                #print(reading['measureId'])
                countIgnored += 1
                dbconn.rollback()

            #print(countEntered)
            #print(countIgnored)
            #print('==========')



    print('end of file')
    startDate = startDate + datetime.timedelta(1)
