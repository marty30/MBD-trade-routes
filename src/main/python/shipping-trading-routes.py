#!/usr/bin/env python
import sys
import os
import json
import pyspark
import time
from pyspark.sql.functions import udf

# add actual job
def doJob(full_data, sql):
  epochToDate = udf(lambda epoch: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch)))
  #print (rdd.take(1))
  #full_data = rdd.map(lambda x: (x, )).toDF()
  full_data.show()
  full_data = full_data.withColumn("date", epochToDate(full_data['ts']))
  full_data.registerTempTable("full_data")
  
  # Get static data:
  # shipname, eta_hour, dimstarboard, draught, dimstern, mmsi, destination, dimport, ts, imo, eta_day, eta_minute, shiptype, callsign, eta_month, dimbow, type
  static_data = sql.sql("SELECT shipname, eta_hour, dimstarboard, draught, dimstern, mmsi, destination, dimport, ts, date, imo, eta_day, eta_minute, shiptype, callsign, eta_month, dimbow, type FROM full_data WHERE shiptype >= 70 AND shiptype <= 89 ORDER BY mmsi DESC")
  static_data.registerTempTable("static_data")
  
  # Get dynamic data:
  #sog, ts, timestamp, mmsi, lat, lat2, lon, lon2, rot_direction, rot_angle, nav_status, cog, type, heading
  dynamic_data = sql.sql("SELECT sog, ts, date, timestamp, mmsi, lat, lat2, lon, lon2, rot_direction, rot_angle, nav_status, cog, type, heading FROM full_data WHERE lat IS NOT NULL")
  dynamic_data.registerTempTable("dynamic_data")

  # Make static data unique
  unique_static_data = sql.sql("SELECT count(*) as count, shipname, max(dimstarboard) as dimstarboard, max(dimport) as dimport, max(dimstern) as dimstern, max(dimbow) as dimbow, draught, mmsi, destination, imo, avg(eta_minute) as eta_minute_avg, avg(eta_hour) as eta_hour_avg, avg(eta_day) as eta_day_avg, avg(eta_month) as eta_month_avg, shiptype, callsign, type FROM static_data GROUP BY shipname, draught, mmsi, destination, imo, shiptype, callsign, type ORDER BY mmsi DESC")
  unique_static_data.registerTempTable("unique_static_data")
  #sql.sql("SELECT * FROM unique_static_data ORDER BY count DESC").show()

  # Check if it is truely unique
  sql.sql("SELECT COUNT(*) as cnt, first(shipname) as shipname, first(mmsi) as mmsi FROM unique_static_data GROUP BY mmsi HAVING COUNT(*)>1").show()

  # Fails:
  # - Names that are not exactly the same
  # - The ETA is different #Fixed i think by taking the average eta for each ship
  # - Destinations are not exactly the same but look like eachother
  # - Bullshit destination (233 vs LIANYUNGANG)
  # - Bullshit callsign (566 vs 309B / 5(A3328 vs 9HA3328)
  # - Different draughts
  # 

  # Join static and dynamic data
  joined_data = sql.sql("SELECT d.mmsi, d.ts, date, lat, lat2, lon, lon2, cog, d.type as dynamic_type, s.type as static_type, heading, shipname, destination, dimstarboard, dimport, dimstern, dimbow, draught, shiptype, callsign, imo, eta_minute_avg, eta_hour_avg, eta_day_avg, eta_month_avg FROM dynamic_data as d INNER JOIN unique_static_data as s ON (d.mmsi=s.mmsi) ORDER BY d.mmsi,d.ts")
  joined_data.registerTempTable("joined")
  #joined_data.show()
  return joined_data # Should return an agregated result with the following attributes: ts, lat, lon, load, dest

def main():
  # parse arguments 
  in_dir, out_dir = sys.argv[1:]
  
  conf = pyspark.SparkConf().setAppName("%s %s %s" % (os.path.basename(__file__), in_dir, out_dir))
  sc = pyspark.SparkContext(conf=conf)
  sql = pyspark.SQLContext(sc)
  
  # invoke job and put into output directory
  doJob(sql.read.json(in_dir), sql).write.json(out_dir)
  #doJob(sc.textFile(in_dir)).saveAsTextFile(out_dir)

if __name__ == '__main__':
  main()
