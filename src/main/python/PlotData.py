import time
import json
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

INPUT="result.json"
OUTPUT='plots/trade-routes-'+INPUT+"-"+str(int(time.time()))+'.png'

# See: https://peak5390.wordpress.com/2012/12/08/matplotlib-basemap-tutorial-plotting-points-on-a-simple-map/

# set these variables from the output of the job
# This is the map size
TOP_RIGHT_LAT = 70  # 57.75 # N (If it is negative, it is S)
TOP_RIGHT_LON = 179  # E (If it is negative, it is W)
BOTTOM_LEFT_LAT = -70  # N (If it is negative, it is S)
BOTTOM_LEFT_LON = -179  # E (If it is negative, it is W)

# This is used to calculate the marker size
MAX_SHIP_LOAD = 81027530375218000 # if this is -1, it will be calculated
MIN_SHIP_LOAD = 100 # every load smaller than this is ignored
MAX_SHIP_SIZE = 20 # The maximal size of a marker for the bigger loads
MIN_SHIP_SIZE = 1 # The minimal size for the loads that are as small as the MAX_SHIP_LOAD

NUMBER_OF_DEGREES_IN_IMAGE = (TOP_RIGHT_LAT - BOTTOM_LEFT_LAT) * (TOP_RIGHT_LON - BOTTOM_LEFT_LON)
print "NUMBER_OF_DEGREES_IN_IMAGE: ", NUMBER_OF_DEGREES_IN_IMAGE

print "build Basemap...",
map = Basemap(projection='merc',
              resolution='h',
              area_thresh=10,
              llcrnrlon=BOTTOM_LEFT_LON,
              llcrnrlat=BOTTOM_LEFT_LAT,
              urcrnrlon=TOP_RIGHT_LON,
              urcrnrlat=TOP_RIGHT_LAT)
print " done"
#print "drawcoastlines...",
#map.drawcoastlines()
#print " done"
print "drawcountries...",
map.drawcountries()
print " done"
print "fillcontinents...",
map.fillcontinents(color='coral')
print " done"
#print "drawmapboundary...",
#map.drawmapboundary()
#print " done"

with open(INPUT) as data_file:
    data = json.load(data_file)

if MAX_SHIP_SIZE == -1:
    for dataentry in data:
        if dataentry["load"] > MAX_SHIP_LOAD:
            MAX_SHIP_LOAD = dataentry["load"]

print "MAX LOAD: ", MAX_SHIP_LOAD
def getMarkerSizeFromLoad(load):
    if (load > MIN_SHIP_LOAD):
        return int(float(load)/MAX_SHIP_LOAD*MAX_SHIP_SIZE) + MIN_SHIP_SIZE
    else:
        return 0


for dataentry in data:
    msize = getMarkerSizeFromLoad(dataentry["load"])
    if (msize>0):
        x, y = map(dataentry["longitude"], dataentry["latitude"])
        print "plot (",dataentry["latitude"], dataentry["longitude"],") ->", msize
        map.plot(x, y, 'bo', markersize=msize)

print "Save the map to", OUTPUT,"...",
plt.savefig(OUTPUT, bbox_inches='tight', dpi=300)
print "done"
