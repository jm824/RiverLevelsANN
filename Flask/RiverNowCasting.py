from flask import Flask, render_template
import psycopg2

app = Flask(__name__)

connection = 'dbname=trout user=postgres password=67a256 host=localhost port=5432'
try:
    dbconn = psycopg2.connect(connection)
    cur = dbconn.cursor()
except:
    print('Connection to the database could not be established')

@app.route('/')
def get_catchments():
    cur.execute(
        "SELECT catchments.id, catchments.measure FROM catchments;", )
    result = cur.fetchall()

    return render_template(
        'catchments.html',
        data=result
    )

@app.route('/predictions/catchments/<measure>')
def get_predictions(measure):
    cur.execute(
        "SELECT DISTINCT hoursahead FROM riverlevelpredictions WHERE measureid = %s ORDER BY hoursahead;", (measure,))
    result = cur.fetchall()

    return render_template(
        'prediction_periods.html',
        data=result,
        measure=measure
    )

@app.route('/predictions/catchments/<measure>/<hour>')
def google(measure, hour):
    import time
    cur.execute(
        "SELECT riverlevelpredictions.datetime, riverlevelpredictions.value, hourlygaugereading.value FROM riverlevelpredictions " \
        "LEFT JOIN hourlygaugereading " \
        "ON riverlevelpredictions.datetime = hourlygaugereading.datetime " \
        "WHERE riverlevelpredictions.measureid = %s " \
        "AND hourlygaugereading.measureid = %s " \
        "AND riverlevelpredictions.hoursahead = %s; ", (measure, measure, hour))
    result = cur.fetchall()

    if not result:
        from flask import abort
        abort(404)

    result2 = []
    for r in result:
        for_js = int(time.mktime(r[0].timetuple())) * 1000
        r = (for_js,r[1],r[2])
        result2.append(r)
    return render_template(
        'google.html',
        data=result2,
        hour=hour
    )

@app.route('/levels/<stationReference>')
def get_readings(stationReference):
    """Renders the station page."""
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
            "WITH r_measure AS (SELECT id FROM gaugemeasure WHERE station = %s AND qualifier = 'Stage')SELECT datetime, value FROM hourlygaugereading WHERE measureid IN (SELECT id FROM r_measure) ORDER BY datetime DESC LIMIT  10;" , (stationReference,))
        result = cur.fetchall()
        return render_template(
            'station.html',
            stationName=stationReference,
            message='Viewing station %s' %stationReference,
            data=result
        )


if __name__ == '__main__':
    app.run(debug=True)
