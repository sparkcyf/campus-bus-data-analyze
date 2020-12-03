from time import sleep

import requests
import json
import pandas as pd
import numpy as np
#nearest point
from scipy import spatial
from shapely.geometry import shape
from shapely.geometry import Point, Polygon

response1 = requests.get("https://bus.sustcra.com/api/v1/bus/monitor_osm/")
sleep(12)
response2 = requests.get("https://bus.sustcra.com/api/v1/bus/monitor_osm/")
json_data1 = json.loads(response1.text)
json_data2 = json.loads(response2.text)


class location:
  def __init__(self, lat, lng):
      self.lat = lat
      self.lng = lng

myloc = location(22.602902, 113.995752)


time_lut = pd.read_csv('1.csv')

route_control_point_np = np.c_[time_lut.lng, time_lut.lat]
route_control_point_time = np.array(time_lut.time)
#print(route_control_point_time)


estimate_time = []

#control mask
# load control mask
f = open('bus-mask.geojson', 'r')
control_mask = json.load(f)
f.close()

for f in control_mask['features']:
    bus_route_control_mask = shape(f['geometry'])

for bus in json_data2:
    if int(bus['acc']) == 1 and Point(float(bus['lng']),float(bus['lat'])).within(bus_route_control_mask):
        #print('666')
        #bus
        bus_point = [float(bus['lng']),float(bus['lat'])]
        bus_closest_point_index = spatial.KDTree(route_control_point_np).query(bus_point)[1]
        bus_closest_point_time = route_control_point_time[bus_closest_point_index]
        #user
        user_point = [myloc.lng, myloc.lat]
        user_closest_point_index = spatial.KDTree(route_control_point_np).query(user_point)[1]
        user_closest_point_time = route_control_point_time[user_closest_point_index]
        eta = user_closest_point_time - bus_closest_point_time

        #check data in json_data1
        bus_imei = bus['imei']
        for bus in json_data1:
            if bus['imei'] == bus_imei:
                #print('found!')
                #check bus direction
                # bus
                bus_point_old = [float(bus['lng']), float(bus['lat'])]
                bus_closest_point_index_old = spatial.KDTree(route_control_point_np).query(bus_point_old)[1]
                bus_closest_point_time_old = route_control_point_time[bus_closest_point_index_old]
                # user
                eta_old = user_closest_point_time - bus_closest_point_time_old
                #print('eta is ' + str(eta))
                #print('eta_old is ' + str(eta_old))

                if eta > 0 and eta < eta_old:
                    print('Next Bus is ' + bus['imei'] + ', its eta is ' + str(int(eta)) + ' seconds')