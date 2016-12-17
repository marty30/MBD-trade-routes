#!/usr/bin/env python
import sys
import os
import json
import pyspark
import re

def main():
  # parse arguments 
  in_dir, out_dir = sys.argv[1:]
  
  conf = pyspark.SparkConf().setAppName("PythonWordCount %s %s" % (in_dir, out_dir))
  sc = pyspark.SparkContext(conf=conf)
  
  # add actual program using sc
  sc.textFile(in_dir)\
    .flatMap(lambda line: re.split(r"\s+", line))\
    .map(lambda word: (word,1))\
    .reduceByKey(lambda a,b: a+b)\
    .saveAsTextFile(out_dir)

if __name__ == '__main__':
  main()
