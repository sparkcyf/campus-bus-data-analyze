import json
from json import JSONEncoder
from time import sleep

import requests
import pandas as pd
import numpy as np
#nearest point
from scipy import spatial
from shapely.geometry import shape
from shapely.geometry import Point, Polygon

# import route and time csv

#import route LUT
#index,lng,lat,time
lut_up = pd.read_csv('bus-status-update/lut-up.csv')
lut_down = pd.read_csv('bus-status-update/lut-dow.csv')

#import timetable
#0 time(sec) 1 peak
timetable_up_workday = pd.read_csv('bus-status-update/rsb-jhl-workday.csv')
timetable_up_holiday = pd.read_csv('bus-status-update/rsb-jhl-weekend.csv')
timetable_down_workday = pd.read_csv('bus-status-update/jhl-rsb-workday.csv')
timetable_down_holiday = pd.read_csv('bus-status-update/jhl-rsb-weekend.csv')

#import control mask
f = open('bus-status-update/bus-mask.geojson', 'r')
control_mask = json.load(f)
f.close()
for f in control_mask['features']:
    bus_route_control_mask = shape(f['geometry'])


#import loop mask
f = open('bus-status-update/loop-mask.geojson', 'r')
control_mask = json.load(f)
f.close()
for f in control_mask['features']:
    loop_control_mask = shape(f['geometry'])

#import station mask
f = open('bus-status-update/RSB.geojson', 'r')
control_mask = json.load(f)
f.close()
for f in control_mask['features']:
    RSB_depart_mask = shape(f['geometry'])

#import station mask
f = open('bus-status-update/JHL.geojson', 'r')
control_mask = json.load(f)
f.close()
for f in control_mask['features']:
    JHL_depart_mask = shape(f['geometry'])

# get the latest json
response1 = requests.get("https://bus.sustcra.com/api/v1/bus/monitor_osm/")
latest_json = json.loads(response1.text)


#create index
class bus_data:
    def __init__(self, tag, depart_time, peak_line, lat, lng, course, speed, status, depart_seconds):
        self.tag = tag
        self.depart_time = depart_time
        self.peak_line = peak_line
        self.lat = lat
        self.lng = lng
        self.course = course
        self.speed = speed
        self.status = status
        self.depart_seconds = depart_seconds

class MyEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__




# update index

# add bus to index
imei_list = []
for bus in latest_json:
    imei_list.append(bus['imei'])

bus_dataset = dict()

for bus in latest_json:
    new_row = bus_data(int(0), int(0), int(0), float(bus['lat']), float(bus['lng']), int(0), int(0), int(0), int(0))
    bus_dataset.setdefault(int(bus['imei']), []).append(new_row)










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
j1 = json.dumps(bus_dataset, cls=MyEncoder)
#
# jsd1 = json.loads(json.dumps(path_sample, cls=MyEncoder))
#jsd2 = jsd1['12345'][0]['pon']
print(j1)