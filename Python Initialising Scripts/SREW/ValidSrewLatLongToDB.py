import csv
import psycopg2

"""
This is a one time run script
Enter all SREW stations into database for which we have a lat long for
In other words enter all valid SREW stations into the local database
Stations are read from a static file.
"""

#Open csv file that contains the ids for all the valid SREW stations
validSrewStations = open('data/valid srew ids (hourly sites).csv')
validIds = csv.reader(validSrewStations, delimiter=',')
ids = []
for row in validIds:
    ids.append(row[0])
validSrewStations.close()

connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
try:
    dbconn = psycopg2.connect(connection)
    cur = dbconn.cursor()
except:
    print('Connection to the database could not be established')

#Read in csv data
with open('data/MIDAS dump/export.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        if row[4] in ids: #If the station is in the list of valid stations
            data = {}
            data['id'] = row[4]
            data['id_type'] = None
            data['met_domain_name'] = row[7]
            data['src_id'] = None
            data['lat'] = row[1]
            data['long'] = row[2]
            data['src_name'] = row[0]

            #Try and enter it into database
            try:
                srewSQL = "INSERT INTO SrewStations" \
                             "(id, id_type, met_domain_name, src_id, lat, long, src_name)" \
                             "VALUES (%(id)s, %(id_type)s, %(met_domain_name)s, %(src_id)s, %(lat)s, %(long)s, %(src_name)s);"
                cur.execute(srewSQL, data)
                dbconn.commit()
            except psycopg2.IntegrityError:
                dbconn.rollback()
                continue

