import os, sys
import pandas as pd
import geopandas as gpd
import shapely as shp
from shapely.geometry import Point
from fiona.crs import from_epsg
import pyproj
import csv
#import findspark
#findspark.init('/Users/shay/spark-2.1.0-bin-hadoop2.7')
import pyspark
sc = pyspark.SparkContext()
sqlContext = pyspark.sql.SQLContext(sc)

# read in desired taxi file
yellow = sc.textFile('/user/sl5335/subway_taxi_project/data/yellow_tripdata_2016-06.csv')
green = sc.textFile('/user/sl5335/subway_taxi_project/data/green_tripdata_2016-06.csv')
header = green.first()
green.filter(lambda x: x != header)
yellow = yellow.union(green.filter(lambda x: x != header)) 

# read in subway stations
subwayst = gpd.read_file('/home/sl5335/subway_taxi_project/data/subwaystations/subwaystations.shp')
subwayst.set_geometry('geometry', inplace = True, crs = 4326)
subwayst.crs = from_epsg(4326)
subwayst = subwayst.to_crs(epsg = 2263)

# create subway station buffer (100 feet radius)
# notee - wanted to use entrances but couldn't find file mapping
# entrances to stations

subwayst['buffer'] = subwayst.geometry.apply(lambda x: x.buffer(300))

# remove Staten Island (no SIRee)
subwayst = subwayst[subwayst.line != 'SIR']
subwayst.set_geometry('buffer', inplace = True, crs = 2263)
all_subways = subwayst['buffer'].unary_union

def extractGeom(partId, records):
    ''' extract pickup date
        extract pickup hour
        extract pickup long & lat
        map pickup lat & long to subway stations
        return station, date and time
        '''
    from shapely.geometry import Point
    import pyproj
    import rtree
    import geopandas as gpd
    import csv
    # create rtree for subway stations
    index = rtree.Rtree()
    for idx, geometry in enumerate(subwayst['buffer']):
        index.insert(idx, geometry.bounds)
    if partId==0:
        records.next()
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    reader = csv.reader(records)
    for row in reader:
        # extract longit & latit and convert to feet
        if len(row) > 8: 
		longit = float(row[5])
        	latit = float(row[6])
        	geom = Point(proj(longit, latit))
        	match = index.intersection((geom.x-300, geom.y-300, geom.x+300,
                	geom.y+300))
        	nearest = (1e6, None)
        	for idx in match:
           		nearest = min(nearest, (geom.distance(subwayst.geometry[idx]), idx))
        #(datetime, geom) = (row[1], (float(row[5]), float(row[6])))
        #geom = Point(float(row[5]), float(row[6]))
        	if nearest != (1e6, None):
			datetime = row[1]
			date = datetime[8:10]
        		hour = datetime[11:13]
        		if (int(hour) > 6) and (int(hour) < 21): 
				yield ((int(date), int(hour)), subwayst.objectid[nearest[1]])

# map partitions to date/hour key
# filter to between 7 am & 8 pm
# map/reduce to key list pair
# map to key & station count dictionary
# collect and save to be merged with subway delay data based on time and station
from collections import Counter
small_yellow = yellow.mapPartitionsWithIndex(extractGeom) \
		.map(lambda x: (x[0], [str(x[1])])) \
                .reduceByKey(lambda x, y: x + y, numPartitions = 128) \
                .mapValues(lambda x: Counter(x))
results = small_yellow.collect()

df = pd.DataFrame.from_records(results)
small_yellow.saveAsTextFile('/user/sl5335/subway_taxi_project/results2.txt')
df.to_csv('/home/sl5335/subway_taxi_project/results.csv')