import json
# time
from datetime import datetime
from json import JSONEncoder

import pandas as pd
import requests
from shapely.geometry import Point
# nearest point
from shapely.geometry import shape

# import route and time csv

# import route LUT
# index,lng,lat,time
lut_up = pd.read_csv('lut-up.csv')
lut_down = pd.read_csv('lut-dow.csv')

# import timetable
# 0 time(sec) 1 peak
timetable_up_workday = pd.read_csv('rsb-jhl-workday.csv')
timetable_up_holiday = pd.read_csv('rsb-jhl-weekend.csv')
timetable_down_workday = pd.read_csv('jhl-rsb-workday.csv')
timetable_down_holiday = pd.read_csv('jhl-rsb-weekend.csv')

# import control mask
f = open('bus-mask.geojson', 'r')
control_mask = json.load(f)
f.close()
for f in control_mask['features']:
    bus_route_control_mask = shape(f['geometry'])

# import loop mask
f = open('loop-mask.geojson', 'r')
control_mask = json.load(f)
f.close()
for f in control_mask['features']:
    loop_control_mask = shape(f['geometry'])

# import station mask
f = open('RSB.geojson', 'r')
control_mask = json.load(f)
f.close()
for f in control_mask['features']:
    RSB_depart_mask = shape(f['geometry'])

# import station mask
f = open('JHL.geojson', 'r')
control_mask = json.load(f)
f.close()
for f in control_mask['features']:
    JHL_depart_mask = shape(f['geometry'])

# get the latest json
# response1 = requests.get("https://bus.sustcra.com/api/v1/bus/monitor_osm/")
# latest_json = json.loads(response1.text)
with open('api.json') as json_file:
    latest_json = json.load(json_file)

bus_dataset_json = []


# create index
class bus_data:
    def __init__(self, imei, tag, depart_time, peak_line, lat, lng, course, speed, status, direction, depart_seconds):
        self.imei = imei
        self.tag = tag
        self.depart_time = depart_time
        self.peak_line = peak_line
        self.lat = lat
        self.lng = lng
        self.course = course
        self.speed = speed
        self.status = status
        self.direction = direction
        self.depart_seconds = depart_seconds


class MyEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__


# add bus to index

bus_dataset = dict()
for bus in latest_json:
    new_row = bus_data(bus['imei'], int(0), int(-1), int(0), float(bus['lat']), float(bus['lng']), int(-1), int(-1),
                       int(-1), int(-1), int(-1))
    bus_dataset.setdefault(int(bus['imei']), []).append(new_row)


# init list
# bus_dataset_json = []
# for bus in latest_json:
#     data = {}
#     data['imei'] = bus['imei']
#     data['tag'] = 0
#     data['depart_time'] = 0
#     data['peak_line'] = 0
#     data['lat'] = bus['lat']
#     data['lng'] = bus['lng']
#     data['course'] = 0
#     data['speed'] = 0
#     data['status'] = 0
#     data['depart_seconds'] = 0
#     bus_dataset_json.append(data)
def process_data(api):
    # start update index!
    # get the latest json
    # response1 = requests.get("https://bus.sustcra.com/api/v1/bus/monitor_osm/")
    # latest_json = json.loads(response1.text)

    with open('%s' % api) as json_file:
        latest_json = json.load(json_file)

    def activate_bus(mask, angle, direction, time_table):
        # direction up-1 down-2
        for bus in latest_json:
            # check if the bus is in the start mask
            # ACTIVATION
            # RSB
            bus_imei = int(bus['imei'])
            gps_time_convert = datetime.fromtimestamp(int(bus['gps_time'])).hour * 3600 + datetime.fromtimestamp(int(bus['gps_time'])).minute * 60 + datetime.fromtimestamp(int(bus['gps_time'])).second
            if (Point(float(bus['lng']), float(bus['lat'])).within(mask)) and (bus_dataset[bus_imei][0].tag == 0) and (
                    int(bus['course']) > (angle - 90)) and (int(bus['course']) < (angle + 90)):
                # lookup in time table
                for index, row in time_table.iterrows():
                    if gps_time_convert - int(row['time_sec']) < 100:

                        bus_dataset[bus_imei][0].tag = 1
                        print('active!' + str(bus_imei))
                        bus_dataset[bus_imei][0].direction = direction
                        bus_dataset[bus_imei][0].depart_time = int(row['time_sec'])
                        if int(row['peak']) == 1:
                            bus_dataset[bus_imei][0].peak = 1
                        break
                        # check if it's peak line

    # deactive bus
    def deactivate_bus(mask, angle, direction):
        for bus in latest_json:
            bus_imei = int(bus['imei'])
            if (Point(float(bus['lng']), float(bus['lat'])).within(mask)) and (bus_dataset[bus_imei][0].tag == 1) and (
                    int(bus['course']) > (angle - 90)) and (int(bus['course']) < (angle + 90)):
                bus_dataset[bus_imei][0].tag = 0
                bus_dataset[bus_imei][0].direction = -1
                bus_dataset[bus_imei][0].depart_time = -1
                bus_dataset[bus_imei][0].peak = 0
                print('DEactive!' + str(bus_imei))

    deactivate_bus(RSB_depart_mask, 0, 2)
    deactivate_bus(JHL_depart_mask, 0, 1)

    # add other metadata
    def add_metadata():
        for bus in latest_json:
            bus_imei = int(bus['imei'])
            if bus_dataset[bus_imei][0].tag == 1:
                gps_time_convert = datetime.fromtimestamp(int(bus['gps_time'])).hour * 3600 + datetime.fromtimestamp(
                    int(bus['gps_time'])).minute * 60 + datetime.fromtimestamp(int(bus['gps_time'])).second
                bus_dataset[bus_imei][0].lat = float(bus['lat'])
                bus_dataset[bus_imei][0].lng = float(bus['lng'])
                bus_dataset[bus_imei][0].course = int(bus['course'])
                bus_dataset[bus_imei][0].speed = int(bus['speed'])
                bus_dataset[bus_imei][0].status = int(bus['device_info_new'])
                bus_dataset[bus_imei][0].depart_seconds = gps_time_convert - bus_dataset[bus_imei][0].depart_time

    activate_bus(RSB_depart_mask, 180, 1, timetable_up_workday)
    activate_bus(JHL_depart_mask, 180, 2, timetable_down_workday)
    deactivate_bus(RSB_depart_mask, 0, 2)
    deactivate_bus(JHL_depart_mask, 0, 1)
    add_metadata()

    # path_sample = dict()
    #
    #
    # new_row = bus_data(int(1), int(16513165))
    # path_sample.setdefault(12345, []).append(new_row)
    #
    # new_row = bus_data(int(666), int(9999))
    # path_sample.setdefault(12346, []).append(new_row)
    #
    # #print(path_sample[12345][0].pon)
    #
    #
    # for i in path_sample.values():
    #     for j in i:
    #         print(j.pon)
    #
    #
    #
    #

    # dump json
    bus_dataset_json = []
    for k, v in bus_dataset.items():
        for i in v:
            bus_dataset_json.append(i.__dict__)
    # print(json.dumps(bus_dataset, cls=MyEncoder))

    #
    # jsd1 = json.loads(json.dumps(path_sample, cls=MyEncoder))
    # jsd2 = jsd1['12345'][0]['pon']
    with open('bus_dataset_json.json', 'w') as outfile:
        json.dump(bus_dataset_json, outfile)
    print(bus_dataset_json)

# cntt = 1
# jud = 1
# while jud:
#     process_data('api%s.json' % str(cntt))
#     cntt += 1
#     if cntt > 5:
#         jud =0