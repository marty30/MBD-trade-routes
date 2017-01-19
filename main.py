import json
import os


if __name__ == "__main__":
    data = {}
    for file in os.listdir('final-run'):

        with open('final-run/{}'.format(file)) as data_file:
            # with open('final-run-edited/{}'.format(file), 'wt') as replace:
            #     replace.write('[')
            #     for line in data_file:
            #         print(line)
            #         if next(data_file) is not None:
            #             replace.write(line.replace('\n', ',\n'))
            #     replace.write(']')
            for line in data_file:
                js = json.loads(line)
                latitude = int(js['lat']) + 300
                longitude= int(js['lon']) + 300
                count    = js['count']
                load     = js['load']
                if latitude not in data.keys():
                    data[latitude]={}
                if longitude not in data[latitude].keys():
                    data[latitude][longitude] = [0,0]
                elem = data[latitude][longitude]
                elem[0] += count
                elem[1] += load

    jsonlist = []
    for latitude in data.keys():
        for longitude in data[latitude].keys():
            elem = data[latitude][longitude]
            jsonlist.append({'longitude':latitude-300,'latitude':longitude-300,'count': elem[0],'load': elem[1]})
    with open('result.json','w') as out:
        json.dump(jsonlist, out)

