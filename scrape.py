import pyspatialite.dbapi2 as sqlite3

import requests

PER_FETCH = 1000

def main():
   conn = sqlite3.connect('db.sqlite3')
   c = conn.cursor()

   c.execute('SELECT InitSpatialMetaData()')

   # we use this one, you probably won't
   c.execute("""
INSERT into spatial_ref_sys (srid, auth_name, auth_srid, proj4text, srs_wkt) values ( 102704, 'ESRI', 102704, '+proj=lcc +lat_1=40 +lat_2=43 +lat_0=39.83333333333334 +lon_0=-100 +x_0=500000.0000000002 +y_0=0 +datum=NAD83 +units=us-ft +no_defs ', 'PROJCS["NAD_1983_StatePlane_Nebraska_FIPS_2600_Feet",GEOGCS["GCS_North_American_1983",DATUM["North_American_Datum_1983",SPHEROID["GRS_1980",6378137,298.257222101]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["False_Easting",1640416.666666667],PARAMETER["False_Northing",0],PARAMETER["Central_Meridian",-100],PARAMETER["Standard_Parallel_1",40],PARAMETER["Standard_Parallel_2",43],PARAMETER["Latitude_Of_Origin",39.83333333333334],UNIT["Foot_US",0.30480060960121924],AUTHORITY["EPSG","102704"]]')""")

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
         # skip pure storm sewers
         if sewer['attributes']['SWR_TYPE'] == 1:
            continue
         if sewer['attributes']['LINE_TYPE'] == 'Abandoned':
            continue

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
