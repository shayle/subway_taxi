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
yellow = sc.textFile('/Users/shay/CUSP/BDM/subway_taxi_project/data/yellow_tripdata_2016-06.csv')


# read in subway stations
subwayst = gpd.read_file('/Users/shay/CUSP/BDM/subway_taxi_project/data/subwaystations/subwaystations.shp')
subwayst.set_geometry('geometry', inplace = True, crs = 4326)
subwayst.crs = from_epsg(4326)
subwayst = subwayst.to_crs(epsg = 2263)

# create subway station buffer (100 feet radius)
# notee - wanted to use entrances but couldn't find file mapping
# entrances to stations

subwayst['buffer'] = subwayst.geometry.apply(lambda x: x.buffer(100))

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
        longit = float(row[5])
        latit = float(row[6])
        geom = Point(proj(longit, latit))
        match = index.intersection((geom.x-200, geom.y-200, geom.x+200,
                geom.y+200))
        nearest = (1e6, None)
        for idx in match:
            nearest = min(nearest, (geom.distance(subwayst.geometry[idx]), idx))
        #(datetime, geom) = (row[1], (float(row[5]), float(row[6])))
        #geom = Point(float(row[5]), float(row[6]))
        datetime = row[1]
        date = datetime[8:10]
        hour = datetime[11:13]
        yield ((int(date), int(hour)), subwayst.objectid[nearest[1]])

uniqueids = [str(i) for i in subwayst.objectid.unique()]

def createCounts(value_list):
    import numpy as np
    value_list = np.array(value_list)
    station_dict = {}
    for station in uniqueids:
        count = len(value_list[value_list == station])
        station_dict[station] = count
    return station_dict

# map partitions to date/hour key
# filter to between 7 am & 8 pm
# map/reduce to key list pair
# map to key & station count dictionary
# collect and save to be merged with subway delay data based on time and station
from collections import Counter
small_yellow = yellow.mapPartitionsWithIndex(extractGeom) \
                .filter(lambda x: (x[0][1] > 6) and (x[0][1] < 22) \
                .map(lambda x: (x[0], [str(x[1])])) \
                .reduceByKey(lambda x, y: x + y) \
                .mapValues(lambda x: (Counter(x))).collect()
