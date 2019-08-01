import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode

import json
import paho.mqtt.client as mqtt

from datetime import datetime

# IP Address of the broker(Rpi)
MQTT_Broker = "192.168.0.105"
# Common port
MQTT_Port = 1883
# Connection lifespan
Keep_Alive_Interval = 60

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

    # Topics to listen from
    client.subscribe("esp8266/#")


def on_message(client, userdata, msg):
    print(msg.payload)
    
    # Pass payload from NodeMCUs
    get_data(msg)

def get_data(msg):
    
    print("Recieved.")
    try:
        # Connection to remote webserver
        connection = mysql.connector.connect(
            host='2.57.89.1',
            database='u690747639_soil',
            user='u690747639_root',
            password='0OzJ2dik0HEZ')

        # Data insertion
        dbcursor = connection.cursor()
        dbcursor.execute("select * from soil_data")
        dbcursor.fetchall()
        print("rows:{}".format(dbcursor.rowcount))
        currentrowcount = dbcursor.rowcount + 1
        print(currentrowcount)

        # Extract JSON content
        jsonDoc = json.loads(msg.payload)
        print(jsonDoc['SensorID'])

        # datetime object containing current date and time
        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y %H:%M:%S")

        # Sql query
        sql_insert_query = "insert into soil_data values(NULL, '{}', '{}', {}, {}, {}, {}, {}, {})".format(
            jsonDoc['SensorID'],
            dt_string,
            jsonDoc['Nitrogen'],
            jsonDoc['Phosphorous'],
            jsonDoc['Potassium'],
            jsonDoc['Nitrogen_FRQ'],
            jsonDoc['Phosphorous_FRQ'],
            jsonDoc['Potassium_FRQ'])

        dbcursor.execute(sql_insert_query)

        # Apply changes
        connection.commit()
        
        # Dispose connection after use
        connection.close()
        dbcursor.close()

        print("Insertion successful from {}".format(jsonDoc['SensorID']))
    except mysql.connector.Error as error:
        connection.rollback()
        print("Insertion failed:" + str(error))


# Events to listen
client = mqtt.Client()
client.on_message = on_message
client.on_connect = on_connect

# Init connection
client.connect(MQTT_Broker, MQTT_Port, Keep_Alive_Interval)

# Continous listening to the broker
client.loop_forever()
