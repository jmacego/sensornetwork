import paho.mqtt.client as mqtt
import time
import requests
import json
import time
import threading
from influxdb import InfluxDBClient
import yaml

with open("config.yaml", 'r') as conffile:
    config = yaml.load(conffile, Loader=yaml.FullLoader)

owm_api_key = config['owm']['api_key']
owm_city = config['location']['city']

influx = InfluxDBClient(config['influx']['host'], 8086, config['influx']['username'], config['influx']['password'], 'weather')

def every(delay, task, *args, **kwargs):
    next_time = time.time() + delay
    print(time.time(), next_time)
    while True:
        time.sleep(max(0, next_time - time.time()))
        try:
            task(*args, **kwargs)
        except Exception:
            traceback.print_exc()
            # in production code you might want to have this instead of course:
            # logger.exception("Problem while executing repetitive task.")
        # skip tasks if we are behind schedule:
        next_time += (time.time() - next_time) // delay * delay + delay

class temperature:
    """Store a temperature and return"""
    def __init__(self, temp, unit="k"):
        """Store a temperature"""
        if unit == "c":
            self.k = self.__convert_c_to_k(temp)
            self.c = temp
            self.f = self.__convert_k_to_f(self.k)
        elif unit == "f":
            self.k = self.__convert_f_to_k(temp)
            self.c = self.__convert_k_to_c(self.k)
            self.f = f
        elif unit == "k":
            self.k = k
            self.c = self.__convert_k_to_c(temp)
            self.f = self.__convert_k_to_f(temp)
        else:
            raise SystemError
    
    def __convert_c_to_k(c):
        """Convert a temperature from Celsius to Kelvin"""
        return c + 273.15
    
    def __convert_f_to_k(f):
        """Convert a temperature from Fahrenheit to Kelvin"""
        return (f - 32) * 5 / 9 + 273.15
    
    def __convert_k_to_f(k):
        """Convert a temperature from Kelvin to Fahrenheit"""
        return (k - 273.15) * 9 / 5 + 32
    
    def __convert_k_to_c(k):
        """Convert a temperature from Kelvin to Celsius"""
        return k - 273.15

def get_owm():
    base_url = "http://api.openweathermap.org/data/2.5/weather?"
    complete_url = base_url + "appid=" + owm_api_key + "&q=" + owm_city
    response = requests.get(complete_url) 
    x = response.json() 
    if x["cod"] != "404": 
        return x

def temp(influx):
    raw_data = get_owm()
    tags = { "city": "North Hollywood",
             "datasource": "OpenWeatherMaps",
             "state": "CA",
             "note:": "Commercial",
             "latitude": raw_data["coord"]["lat"],
             "longitude": raw_data["coord"]["lon"],}
    
    data = []
    
    data.append({"measurement": "sky",
                "tags": tags,
                "fields": {"value": raw_data["weather"][0]["main"]}
                })

    data.append({ "measurement": "temperature",
              "tags": tags,
              "fields": { "value": raw_data["main"]["temp"]}
              })
    data.append({ "measurement": "pressure",
              "tags": tags,
              "fields": { "value": raw_data["main"]["pressure"]}
              })
    data.append({ "measurement": "humidity",
              "tags": tags,
              "fields": { "value": raw_data["main"]["humidity"]}
              })
    data.append({ "measurement": "visibility",
              "tags": tags,
              "fields": { "value": raw_data["visibility"]}
              })
    data.append({ "measurement": "wind_speed",
              "tags": tags,
              "fields": { "value": raw_data["wind"]["speed"]}
              })
    data.append({ "measurement": "wind_direction",
              "tags": tags,
              "fields": { "value": raw_data["wind"]["deg"]}
              })
    data.append({ "measurement": "clouds",
              "tags": tags,
              "fields": { "value": raw_data["clouds"]["all"]}
              })

    print(data)
    influx.switch_database("weather")
    print(influx.write_points(data, time_precision='s'))

#temp(influx)

threading.Thread(target=lambda: every(60, temp, influx)).start()

