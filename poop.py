import xml.etree.ElementTree

import requests
import pyspatialite.dbapi2 as sqlite3

from flask import Flask, render_template, g, jsonify, Response, request
app = Flask(__name__)

from config import Config

class ServiceError(Exception): pass

@app.before_request
def before_request():
   g.conn = sqlite3.connect(Config.db_dsn, detect_types=sqlite3.PARSE_DECLTYPES)
   g.c = g.conn.cursor()

@app.teardown_request
def after_request(response_class):
   g.conn.commit()
   g.c.close()
   g.conn.close()

@app.route('/')
def index_page():
   return render_template('index.html')

@app.route('/map', methods=('GET',))
def map_page():
   # get geocoded lat, long
   try:
      lat, lon = geocode(request.args.get('addr'), request.args.get('zip', ''))
   except ServiceError as e:
      return e.args[0]
   # abort if not in omaha
   if not ((41.091064527497061 <= lat) and (41.376539822656774 >= lat)) \
      or \
      not ((-96.30227087577579 <= lon) and (-95.852022352671796 >= lon)):
      return 'Your address is not within Douglas County'
   # find nearest pipe
   # 200: distance (in feet, i believe)
   # we're not escaping because these got validated by float() earlier
   # and we can't have quotes within well known text
   g.c.execute("""
SELECT id,
   ST_Distance(sewer, Transform(ST_PointFromText('POINT({0} {1})', 4326), 102704)) as dist
FROM sewers
WHERE sewers.rowid IN
   (SELECT pkid
   FROM idx_sewers_sewer
   WHERE pkid MATCH RTreeDistWithin(
      ST_X(Transform(ST_PointFromText('POINT({0} {1})', 4326), 102704)),
      ST_Y(Transform(ST_PointFromText('POINT({0} {1})', 4326), 102704)),
      500)
   )
ORDER BY dist asc""".format(lon, lat))
   candidates = g.c.fetchall()
   if len(candidates) == 0:
      return 'Couldn\'t find any sewers near you.'
   # descend the trail iteratively
   sewers = set([candidates[0][0]])
   heads = set([candidates[0][0]])
   while True:
      downstreams = set()
      print 'trying head', heads
      for head in heads:
         g.c.execute("""SELECT id FROM sewers WHERE upstream_manhole =
            (SELECT downstream_manhole FROM sewers WHERE id = ?)""", (head,))
         downstream = g.c.fetchall()
         [sewers.add(x[0]) for x in downstream]
         [downstreams.add(x[0]) for x in downstream]
      if not downstreams:
         for head in heads:
            # get number of points in line segment
            g.c.execute("SELECT ST_NumPoints(sewer) FROM sewers WHERE id = ?", (head,))
            points = g.c.fetchall()[0][0]
            # select the last one.  indexed at 1
            g.c.execute("SELECT AsWKT(PointN(sewer, ?)) FROM sewers WHERE id = ?", (points, head))
            point = g.c.fetchall()[0][0]
            # find out if anything is near it
            g.c.execute("""
SELECT id,
   ST_Distance(sewer, ST_PointFromText('{0}')) as dist
FROM sewers
WHERE sewers.rowid IN
   (SELECT pkid
   FROM idx_sewers_sewer
   WHERE pkid <> '{1}'
      AND
      pkid MATCH RTreeDistWithin(
         ST_X(ST_PointFromText('{0}')),
         ST_y(ST_PointFromText('{0}')),
         10)
   )
ORDER BY dist asc""".format(point, head, point, point))
            for nearby in g.c.fetchall():
               if nearby[0] not in sewers and nearby[0] not in downstreams and nearby[0] not in heads:
                  sewers.add(nearby[0])
                  downstreams.add(nearby[0])
         if not downstreams:
            # if, after all of that shit, there are still no new downstreams, give up
            break
      if heads == downstreams:
         break
      heads = downstreams

   # return the list of sewers
   g.c.execute('SELECT AsGeoJSON(Transform(sewer, 4326)) FROM sewers where id in ({0})'\
      .format(','.join([str(x) for x in sewers])))
   sewer_json = '[' + ','.join((x[0] for x in g.c if x[0] != None)) + ']'
   return render_template('map.html', sewer_json=sewer_json)

@app.route('/test')
def test():
   return render_template('map.html')

@app.route('/sewers')
def sewers_page():
   g.c.execute('SELECT AsGeoJSON(Transform(sewer, 4326)) FROM sewers where id = 40558')
   return Response(response='[' + ','.join((x[0] for x in g.c if x[0] != None)) + ']',
      mimetype='application/json')

def geocode(address, zip):
   r = requests.get('http://geoservices.tamu.edu/Services/Geocode/WebService/GeocoderWebServiceHttpNonParsed_V04_01.aspx', params={'apiKey': Config.apikey, 'version': '4.01', 'city': 'Omaha',
      'state': 'NE', 'format': 'xml', 'streetAddress': address, 'zip': zip})
   try:
      t = xml.etree.ElementTree.fromstring(r.content)
   except:
      raise ServiceError('Couldnt parse geocoder xml')
   result = t.findall('./QueryMetadata/FeatureMatchingResultType')
   if not result:
      raise ServiceError('Bad response from geocoder')
   try:
      lat = float(t.findall('./OutputGeocodes/OutputGeocode/Latitude')[0].text)
      lon = float(t.findall('./OutputGeocodes/OutputGeocode/Longitude')[0].text)
   except Exception as e:
      raise ServiceError('Could not figure out the location of that address.')
   return lat, lon

if __name__ == '__main__':
   app.run(host='0.0.0.0', debug=True)
