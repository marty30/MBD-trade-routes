import os
from os import listdir

from os.path import isfile, join

IN="data/runDests416"
OUT=IN+".json"
filenames = [join(IN, f) for f in listdir(IN) if isfile(join(IN, f))]
with open(OUT, 'w+') as outfile:
    outfile.write("[")
    for fname in filenames:
        with open(fname) as infile:
            for line in infile:
                outfile.write(line+",")
    outfile.seek(-1, os.SEEK_END)
    outfile.truncate()
    outfile.write("]")