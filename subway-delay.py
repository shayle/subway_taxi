from google.transit import gtfs_realtime_pb2
import pyspark.sql.functions as sf
from protobuf_to_dict import protobuf_to_dict
import json
import csv
from datetime import datetime
from datetime import timedelta
import pyspark
import pandas as pd

a = datetime.now()
sc = pyspark.SparkContext()
folder = "C:/Users/Nurvirta/OneDrive/CUSP/Spring/Big Data/subway-june/"

def next_whole_minute(t):
    return t+59 - (t+59)%60

def to_minutes(t):
    return ((t // 60 + 19 * 60) % (24 * 60))

def getStops(_, part):
    feed = gtfs_realtime_pb2.FeedMessage()
    for fn, contents in part:
        hr = int(fn[-5:-3])
        dt = fn[-8:-6]
        # RUSH HOUR ONLY
        # the gtfs reports are 5 hours behind
        # from the file contents
        # therefore, add 5 hours
        if hr in [12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 0, 1]: #RUSH HOUR ONLY
            feed.ParseFromString(contents)
            for entity in feed.entity:
                if entity.HasField('vehicle'):
                    vehicle = protobuf_to_dict(entity.vehicle)
                    if vehicle.get('current_status') != 1:
                        continue
                    try:
                        line = vehicle['trip']['route_id'].rstrip('X') # fold express into normal
                        if line not in ['1', '2', '3', '4', '5', '6', 'GS', 'L', 'SI']:
                            print 'weird line', line
                            continue
                        if 'stop_id' in vehicle:
                            stop = vehicle['stop_id']
                        else:
                            # L and SI stop at every station, need to use
                            stop = '%d%s' % (vehicle['current_stop_sequence'], vehicle['trip']['trip_id'][-1])
                        #key = (line, stop)
                        timestamp = vehicle['timestamp'] # datetime.datetime.utcfromtimestamp(vehicle['timestamp'])
                        # merge 2 and 3 line
                        if line in ['2','3']:
                            line = '23'
                        if line in ['4','5']:
                            line = '45'
                        yield ((dt,line,stop),[timestamp])
                        #yield ((dt,line,stop),timestamp)
                        #yield (line, timestamp)
                    except:
                        print 'weird vehicle', vehicle
                        continue

# the for-loop method to get the timedelta
def calculate_delta(key, values):
    dt, line, stop = key
    values = sorted(values)
    last_value = None
    the_result = []
    for i in xrange(1, len(values)):
        last_value, value = values[i-1], values[i]
        delta = 1. / 60 * (value - last_value)
        ts_start = to_minutes(next_whole_minute(last_value))
        start_hr = ts_start/60
        the_result.append([delta, dt, stop, line, start_hr])
    return the_result

# numpy processing to get timedelta
def calculate_delta2(values):
    import numpy as np
    values = sorted(values)
    deltas = np.array(values[1:]) - np.array(values[:-1])
    deltas = map(lambda x: 1./60*x, deltas)
    def get_ts(stop):
        mins = to_minutes(next_whole_minute(stop))
        return mins/60
    stop_hr = map(get_ts, values[1:])
    return zip(deltas, stop_hr)

def get_line_delta(k,v):
    return k[1],v[0]

def get_percentile(v):
    import numpy as np
    v = sorted(v)
    return np.percentile(v,95)

def rearrange(k,v):
    return k[1], [v[0],k[2],k[0],v[1]]

def assign(v):
    thres = v[0]
    vals = v[1]
    assigned_val = None
    if thres > vals[0]:
        assigned_val = 0
    else:
        assigned_val = 1
    return vals[1],vals[2],vals[3],assigned_val

def create_new_key(k,v):
    new_key = v[0]+'_'+v[1]+'_'+v[2]
    new_val = v[3]
    return new_key, new_val

# the rdd
rdd = sc.binaryFiles(folder+'iter').mapPartitionsWithIndex(getStops)\
        .reduceByKey(lambda x,y : x+y, numPartitions=32)\
        .mapValues(lambda x: calculate_delta2(x))\
        .flatMapValues(lambda x:x)

# the threshold
threshold = rdd.map(lambda x: get_line_delta(x[0],x[1]))\
        .groupByKey()\
        .mapValues(lambda x: get_percentile(x))

# join rdd and threshold to assign binary value
result = thres.rightOuterJoin(rdd2)\
    .mapValues(lambda x: assign(x))\
    .map(lambda x: create_new_key(x[0],x[1]))\
    .reduceByKey(add)

df = pd.DataFrame.from_records(result.collect())
df.to_csv('subway_delay_result.csv')
result.saveAsTextFile('subway_delay_result.txt')

#print elapsed time
b = datetime.now()
print b-a
