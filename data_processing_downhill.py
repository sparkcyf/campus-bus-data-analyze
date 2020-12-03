# python script for processing the geodata acquired from sustech bus api



import sqlite3
import numpy as np

#geojson
import json
from shapely.geometry import shape
from shapely.geometry import Point, Polygon

#nearest point
from scipy import spatial

#jit
#import numba
#from numba import jit

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# define func
db_connection = sqlite3.connect('bus-data.db')
db_cursor = db_connection.cursor()

# filter bus data
db_cursor.execute('''SELECT
	ts.gps_time id, t1.*
FROM
(SELECT
	*,
	gps_time - lag_gps_time diff
FROM(
SELECT 
	imei,
	gps_time,
	lag(gps_time, 1, 0) over(order by gps_time) lag_gps_time,
	lat,
	lng,
	course,
	speed,
	device_info_new,
	acc
FROM bus_stat WHERE 
lat BETWEEN 22.609331 AND 22.609994
AND lng BETWEEN 113.997557 AND 113.99864
AND course BETWEEN 90 AND 270
AND acc = 1
)
where diff > 30) ts
join (
SELECT 
	imei,
	gps_time,
	lat,
	lng,
	course,
	speed,
	device_info_new
FROM bus_stat) t1
on ts.imei = t1.imei 
where (0 < ts.gps_time - t1.gps_time and ts.gps_time - t1.gps_time < 30 and t1.device_info_new = 0)
or (0 <= t1.gps_time - ts.gps_time and t1.gps_time - ts.gps_time < 900 and (t1.lat < 22.598937 or t1.device_info_new = 0))''')

path_list = db_cursor.fetchall()
path_list_np = np.array(path_list)
# print(path_list_np)

path_list_np_len = len(path_list_np)

class gps_data:
    def __init__(self, imei, gps_time, lat, lng, course, speed, device_info_new):
        self.imei = imei
        self.gps_time = gps_time
        self.lat = lat
        self.lng = lng
        self.course = course
        self.speed = speed
        self.device_info_new = device_info_new

path_sample = dict()

for data in path_list_np:
    new_row = gps_data(int(data[1]), int(data[2]), float(data[3]), float(data[4]), int(data[5]), int(data[6]), int(data[7]))
    path_sample.setdefault(int(data[0]), []).append(new_row)


# load control mask
f = open('bus-mask.geojson', 'r')
control_mask = json.load(f)
f.close()

for f in control_mask['features']:
    bus_route_control_mask = shape(f['geometry'])


# for (key, value) in pl.items():
#     print(key, len(value))


# delete bus service transfer
print(len(path_sample))

path_sample_polished = dict()
for id, path in path_sample.items():
    for node in path:
        if not Point(node.lng, node.lat).within(bus_route_control_mask):
            break
    else:
        path_sample_polished[id] = path.copy()


for id, path in path_sample_polished.items():
    gps_time_ref = path[0].gps_time
    for node in path:
        node.gps_time -= gps_time_ref

# processing special case between LYC and HYU
f = open('loop-mask.geojson', 'r')
control_mask = json.load(f)
f.close()

for f in control_mask['features']:
    loop_mask = shape(f['geometry'])

# for id, path in path_sample_polished.items():
#     for node in path:
#         if not Point(node.lng, node.lat).within(loop_mask):
#             break
#     else:


# import route control point
f = open('route-control-point-downhill.geojson', 'r')
route_control_point = json.load(f)
f.close()

#route_control_point['features'][0]['geometry']['coordinates']
# json to np array
time_append = np.zeros(len(route_control_point['features'][0]['geometry']['coordinates']))
route_control_point_np = np.array(route_control_point['features'][0]['geometry']['coordinates'])
route_control_point_np_append_time_and_count = np.c_[route_control_point_np,time_append,time_append]
#spatial.KDTree(B).query(pt)[1]

# 0 lng 1 lat 2 time_sum 3 count
for id, path in path_sample_polished.items():
    for node in path:
#        if Point(node.lng, node.lat).within(loop_mask):
            point = [node.lng,node.lat]
            control_point_index = spatial.KDTree(route_control_point_np).query(point)[1]
            route_control_point_np_append_time_and_count[control_point_index][3] = route_control_point_np_append_time_and_count[control_point_index][3] + 1
            route_control_point_np_append_time_and_count[control_point_index][2] = route_control_point_np_append_time_and_count[control_point_index][2] + node.gps_time




normalized_time = (route_control_point_np_append_time_and_count.T[2] / route_control_point_np_append_time_and_count.T[3]).T
route_control_point_np_append_time_and_count = np.c_[route_control_point_np, normalized_time]
np.savetxt("down-mean-time.csv", route_control_point_np_append_time_and_count, delimiter=",")