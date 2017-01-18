import time
import json
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

INPUT="data.json"
OUTPUT='plots/trade-routes-'+str(int(time.time()))+'.png'

# See: https://peak5390.wordpress.com/2012/12/08/matplotlib-basemap-tutorial-plotting-points-on-a-simple-map/

# set these variables from the output of the job
TOP_RIGHT_LAT = 70  # 57.75 # N (If it is negative, it is S)
TOP_RIGHT_LON = 179  # E (If it is negative, it is W)
BOTTOM_LEFT_LAT = -70  # N (If it is negative, it is S)
BOTTOM_LEFT_LON = -179  # E (If it is negative, it is W)
MAX_SHIP_LOAD = -1 # if this is -1, it will be calculated

NUMBER_OF_DEGREES_IN_IMAGE = (TOP_RIGHT_LAT - BOTTOM_LEFT_LAT) * (TOP_RIGHT_LON - BOTTOM_LEFT_LON)
MAX_SHIP_SIZE = 20

print "NUMBER_OF_DEGREES_IN_IMAGE: ", NUMBER_OF_DEGREES_IN_IMAGE

# print "Find middle point of the map"
print "build Basemap...",
map = Basemap(projection='merc',
              lat_0=TOP_RIGHT_LAT,
              lon_0=TOP_RIGHT_LON,
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


def getMarkerSizeFromLoad(load):
    return int(float(load)/MAX_SHIP_LOAD*MAX_SHIP_SIZE)


for dataentry in data:
    msize = getMarkerSizeFromLoad(dataentry["load"])
    x, y = map(dataentry["lat"], dataentry["lon"])
    map.plot(x, y, 'bo', markersize=msize)

print "Save the map to", OUTPUT,"...",
plt.savefig(OUTPUT, bbox_inches='tight', dpi=500)
print "done"

