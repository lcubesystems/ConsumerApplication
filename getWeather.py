from six.moves import configparser
import requests
import sys


def get_api_key():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config['openweathermap']['api']


def get_weather(api_key, location):
    url = "https://api.openweathermap.org/data/2.5/weather?zip={}&appid={}".format(location, api_key)
    r = requests.get(url)
    return r.json()


def main():

    location = '600040,IN'

    api_key = '8b234e5319fe5076baae32946819193f'
    weather = get_weather(api_key, location)

   # print(weather['main']['temp'])
    print(weather)


if __name__ == '__main__':
    main()