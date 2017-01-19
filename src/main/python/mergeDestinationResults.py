import json
import os

data = {}
for file in os.listdir('data'):
    with open('data/{}'.format(file)) as data_file:
        js = json.load(data_file)
        for js_line in js:
            destination = js_line["destination"]
            load=js_line["loadSum"]
            if destination not in data.keys():
                data[destination]=load
            else:
                data[destination]=data[destination]+load

with open('result.json','w') as out:
    json.dump(data, out)