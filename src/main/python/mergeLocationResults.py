import json
import os


if __name__ == "__main__":
    data = {}
    elemc = 0
    for file in os.listdir('final-run'):
        with open('final-run/{}'.format(file)) as data_file:
            for line in data_file:
                js = json.loads(line)
                latitude = int(js['lat']) + 300
                longitude= int(js['lon']) + 300
                if latitude not in data.keys():
                    data[latitude]={}
                if longitude not in data[latitude].keys():
                    data[latitude][longitude] = [0,0]
                elem = data[latitude][longitude]
                elem[0] += js['count']
                elem[1] += js['load']
    maxelem = 0
    jsonlist = []
    for latitude in data.keys():
        for longitude in data[latitude].keys():
            elem = data[latitude][longitude]
            if elem[1] > maxelem:
                maxelem = elem[1]
            jsonlist.append({'longitude':longitude-300,'latitude':latitude-300,'count': elem[0],'load': elem[1]})

    print("from {} to {}. And the max load is: {}".format(elemc, len(jsonlist),maxelem))
    with open('result.json','w') as out:
        json.dump(jsonlist, out)

