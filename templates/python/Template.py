#!/usr/bin/env python
import sys
import os
import json
import pyspark

def main():
  # parse arguments 
  in_dir, out_dir = sys.argv[1:]
  
  conf = pyspark.SparkConnf().setAppName("$toolName %s %s" % (in_dir, out_dir))
  sc = pyspark.SparkContext(conf=conf)
  
  # add actual program using sc


if __name__ == '__main__':
  main()