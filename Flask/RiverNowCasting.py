from flask import Flask, render_template
import psycopg2

app = Flask(__name__)


@app.route('/')
def home():
    return 'Home page. Test text'

@app.route('/levels/<stationReference>')
def getReadings(stationReference):
    """Renders the station page."""
    connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
    try:
        dbconn = psycopg2.connect(connection)
        cur = dbconn.cursor()
    except:
        print('Connection to the database could not be established')

    cur.execute("SELECT id FROM gaugestation WHERE id = %s;", (stationReference,))
    localstations = cur.fetchall()

    if not localstations:
        return render_template(
            'station.html',
            stationName=stationReference,
            message='No such station'
        ), 404
    else:
        cur.execute(
            "WITH r_measure AS (SELECT id FROM gaugemeasure WHERE station = %s AND qualifier = 'Stage')SELECT datetime, value FROM gaugereading WHERE measureid IN (SELECT id FROM r_measure) ORDER BY datetime DESC LIMIT  10;" , (stationReference,))
        result = cur.fetchall()
        return render_template(
            'station.html',
            stationName=stationReference,
            message='Viewing station %s' %stationReference,
            data=result
        )


if __name__ == '__main__':
    app.run(debug=True)
