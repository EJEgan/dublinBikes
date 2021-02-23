import mysql.connector
import requests
import json

try:
    print("trying to connect")
    mydb = mysql.connector.connect(
        host="dublinbikes.chj6z1a17hdc.us-east-1.rds.amazonaws.com",
        user="admin",
        passwd="Aws72gene!",
        database='DublinBikes',
        charset='utf8mb4',
    )
    mycursor = mydb.cursor(dictionary=False)

    print("Connected")

    APIKEY = "feb8a7c42cb881254b40b7ca5bc99f38e8c606b0"
    NAME = "dublin"
    STATIONS = "https://api.jcdecaux.com/vls/v1/stations"

    r = requests.get(STATIONS, params={"apiKey": APIKEY, "contract": NAME})

    def stations_to_db(text):
        stations = json.loads(text)
        for station in stations:
            vals = (int(station.get('number')), station.get('name'),
                    int(station.get('bike_stands')), int(station.get('available_bike_stands')),
                    int(station.get('available_bikes')), station.get('status'),
                    int(station.get('last_update'))
                    )
            mycursor.execute("insert into AvailableBikes values(%s,%s,%s,%s,%s,%s,%s)", vals)
            mydb.commit()
        return

    stations_to_db(r.text)
    mydb.close()
except Exception as e:
    print(e)