import pyspatialite.dbapi2 as sqlite3

import requests

PER_FETCH = 1000

def main():
   conn = sqlite3.connect('db.sqlite3')
   c = conn.cursor()

   c.execute('SELECT InitSpatialMetaData()')

   # schema
   c.execute("""
CREATE TABLE sewer_types (id INTEGER PRIMARY KEY, sewer_type TEXT)
""")
   c.execute("""
CREATE TABLE sewers (id INTEGER PRIMARY KEY, sewer_type INTEGER, upstream_manhole TEXT, downstream_manhole TEXT)
""")
   c.execute("""
CREATE INDEX sewers_upstream_manhole ON sewers (upstream_manhole)
""")
   c.execute("""
CREATE INDEX sewers_downstream_manhole ON sewers (downstream_manhole)
""")

   # note -  102704 (omaha) is hard-coded, you will need to change it
   # if you are using a different SRID
   # you will also need to change the insert into the database
   c.execute("""
SELECT AddGeometryColumn('sewers', 'sewer', 102704, 'MULTILINESTRING', 'XY')
""")

   c.execute("""
SELECT CreateSpatialIndex('sewers', 'sewer')
""")


   sewer_type_cache = {}

   # good luck
   r = requests.get('http://gis.dogis.org/arcgis/rest/services/Cityworks/Street_Reference/MapServer/5/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=true&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&f=json').json()

   print 'total objects: {0}'.format(len(r['objectIds']))
   for obj_ids in (r['objectIds'][pos:pos + PER_FETCH] for pos in xrange(0, len(r['objectIds']), PER_FETCH)):
      obj_ids_len = len(obj_ids)
      # %2C = urlencoded comma
      obj_ids = '%2C'.join((str(x) for x in obj_ids))

      obj_r = requests.get('http://gis.dogis.org/arcgis/rest/services/Cityworks/Street_Reference/MapServer/5/query?where=&text=&objectIds={0}&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&f=json'.format(obj_ids)).json()

      if len(obj_r['features']) != obj_ids_len:
         raise Exception('didnt fetch all objects')

      for sewer in obj_r['features']:
         line_type = sewer['attributes']['LINE_TYPE']
         if line_type not in sewer_type_cache:
            c.execute('INSERT INTO sewer_types (id, sewer_type) VALUES (NULL, ?)', (line_type,))
            sewer_type_cache[line_type] = c.lastrowid
         line_type_id = sewer_type_cache[line_type]

         multilinestring = ''
         for line in sewer['geometry']['paths']:
            multilinestring += '(' + ','.join([str(x[0]) + ' ' + str(x[1]) for x in line]) + ')'
         multilinestring = 'MULTILINESTRING({0})'.format(multilinestring)

         c.execute('INSERT INTO sewers (id, sewer_type, upstream_manhole, downstream_manhole, sewer) VALUES (NULL, ?, ?, ?, ST_MultiLineStringFromText(?, 102704))',
            (line_type_id, sewer['attributes']['UP_MANHOLE'], sewer['attributes']['DN_MANHOLE'], multilinestring))

   conn.commit()
   conn.close()

if __name__ == '__main__':
   main()
