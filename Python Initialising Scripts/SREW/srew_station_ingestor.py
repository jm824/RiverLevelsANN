import csv
import psycopg2
import os

"""
This is a ONE TIME run script to enter in the SREW station details into the database.

Enter all SREW stations into database for which we have a lat long for
In other words enter all valid SREW stations into the local database

Valid station IDs are contained within a static file.
SREW stations details are contained within a second file (this is a dump from the MO Midas database)
"""

#Open csv file that contains the ids for all the valid SREW stations
#Read in the ids for all the valid stations
def read_in_stations(validstationsfile, stationdetailsfile):
    if not os.path.isfile(validstationsfile):
        exit('Valid stations file not found.')
    if not os.path.isfile(stationdetailsfile):
        exit('Stations details file not found')
    if not validstationsfile.endswith('.csv'):
        exit('Valid stations file must be a .csv')
    if not stationdetailsfile.endswith('.csv'):
        exit('Stations details file must be a .csv')

    validSrewStations = open(validstationsfile)
    validIds = csv.reader(validSrewStations, delimiter=',')
    ids = []
    #get the id for each station and put into list
    for row in validIds:
        ids.append(row[0])
    validSrewStations.close()

    #try to connect to the database
    connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
    try:
        dbconn = psycopg2.connect(connection)
        cur = dbconn.cursor()
    except:
        exit('Connection to the database could not be established')

    #Read in the SREW midas dump (this is the file that contains the details on each SREW station)
    with open(stationdetailsfile) as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            if row[4] in ids: #If the station is in the list of valid stations then collect its details
                data = {}
                data['id'] = row[4]
                data['id_type'] = None
                data['met_domain_name'] = row[7]
                data['src_id'] = None
                data['lat'] = row[1]
                data['long'] = row[2]
                data['src_name'] = row[0]

                #enter the details of each valid station into the database
                try:
                    srewSQL = "INSERT INTO SrewStations" \
                                 "(id, id_type, met_domain_name, src_id, lat, long, src_name)" \
                                 "VALUES (%(id)s, %(id_type)s, %(met_domain_name)s, %(src_id)s, %(lat)s, %(long)s, %(src_name)s);"
                    cur.execute(srewSQL, data)
                    dbconn.commit()
                except psycopg2.IntegrityError:
                    #reached here if the station has already been entered
                    dbconn.rollback()
                    continue


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='argument handler')
    parser.add_argument('validstationsfile', help='Path to the csv file containing valid srew station ids')
    parser.add_argument('srewstationdetails', help='Path to the csv file which contains the srew station details')
    args = parser.parse_args()

    read_in_stations(args.validstationsfile, args.srewstationdetails)

