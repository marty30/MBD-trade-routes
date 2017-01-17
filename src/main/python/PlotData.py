import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

# See: https://peak5390.wordpress.com/2012/12/08/matplotlib-basemap-tutorial-plotting-points-on-a-simple-map/

TOP_RIGHT_LAT = 70  # 57.75 # N (If it is negative, it is S)
TOP_RIGHT_LON = 179  # E (If it is negative, it is W)
BOTTOM_LEFT_LAT = -70  # N (If it is negative, it is S)
BOTTOM_LEFT_LON = -179  # E (If it is negative, it is W)

NUMBER_OF_DEGREES_IN_IMAGE = (TOP_RIGHT_LAT - BOTTOM_LEFT_LAT) * (TOP_RIGHT_LON - BOTTOM_LEFT_LON)
MAX_SHIP_SIZE = 20

print "NUMBER_OF_DEGREES_IN_IMAGE: ", NUMBER_OF_DEGREES_IN_IMAGE


# print "Find middle point of the map"
print "build Basemap...",
map = Basemap(projection='merc',
              lat_0=TOP_RIGHT_LAT, #+BOTTOM_LEFT_LAT/2.0,
              lon_0=TOP_RIGHT_LON, #+BOTTOM_LEFT_LON/2.0,
              resolution='h',
              area_thresh=0.1,
              llcrnrlon=BOTTOM_LEFT_LON,
              llcrnrlat=BOTTOM_LEFT_LAT,
              urcrnrlon=TOP_RIGHT_LON,
              urcrnrlat=TOP_RIGHT_LAT)
print " done"
print "drawcoastlines...",
map.drawcoastlines()
print " done"
print "drawcountries...",
map.drawcountries()
print " done"
print "fillcontinents...",
map.fillcontinents(color='coral')
print " done"
print "drawmapboundary...",
map.drawmapboundary()
print " done"

# (lat, long, load, dest)
data = [(0, 0, 86452, "Test 1"),
        (30, 0, 44845, "Test 2"),
        (0, 30, 54866, "Test 3"),
        (-30, -30, 8352, "Test 4")]

MAX_SHIP_LOAD = 0
for (lat, lon, load, dest) in data:
    if load > MAX_SHIP_LOAD:
        MAX_SHIP_LOAD = load


def getMarkerSizeFromLoad(load):
    return int(float(load)/MAX_SHIP_LOAD*MAX_SHIP_SIZE)


for (lat, lon, load, dest) in data:
    msize = getMarkerSizeFromLoad(load)
    # print "(",lat,", ", lon, ", ", load, ", ", dest, ") -> ", msize
    x, y = map(lat, lon)
    map.plot(x, y, 'bo', markersize=msize)

# lons = [-135.3318, -134.8331, -134.6572]
# lats = [57.0799, 57.0894, 56.2399]
# x,y = map(lons, lats)
# map.plot(x, y, 'bo', markersize=18)

# labels = ['Sitka', 'Baranof\n  Warm Springs', 'Port Alexander']
# x_offsets = [10000, -30000, -25000]
# y_offsets = [5000, -25000, -25000]

# for label, xpt, ypt, x_offset, y_offset in zip(labels, x, y, x_offsets, y_offsets):
#     plt.text(xpt+x_offset, ypt+y_offset, label)
print "Show the map"
plt.show()
