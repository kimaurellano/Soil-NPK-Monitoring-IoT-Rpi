import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode

import json
import paho.mqtt.client as mqtt

import time
import threading
from datetime import datetime

# IP Address of the broker(Rpi)
MQTT_Broker = "192.168.0.105"
# Common port
MQTT_Port = 1883
# Connection lifespan
Keep_Alive_Interval = 60

# Reference node list
referenceNodes = ["Node-1", "Node-2", "Node-3", "Node-4", "Node-5", "Node-6"]
# The node to get from payload
payloadNode = {}
# Tries
tryCount = 0


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

    # Topics to listen from
    client.subscribe("esp8266/#")


def on_message(client, userdata, msg):
    # print(msg.payload)

    # Pass payload from NodeMCUs
    get_data(msg)


def get_data(msg):

    print("Recieved.")
    try:
        # Connection to remote webserver
        connection = mysql.connector.connect(
            host='192.168.0.105',
            database='mysql',
            user='root',
            password='')

        if connection.is_connected:
            print("Successfully connected to MySQL")
        else:
            print("Connection failed")

        # Data insertion
        dbcursor = connection.cursor()
        dbcursor.execute("select * from soil_data")
        dbcursor.fetchall()
        print("rows:{}".format(dbcursor.rowcount))
        currentrowcount = dbcursor.rowcount + 1
        print(currentrowcount)

        # Extract JSON content. Dictionary
        jsonDoc = json.loads(msg.payload)
        print(jsonDoc['SensorID'])

        if tryCount < 10:
            pass

        # datetime object containing current date and time
        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y %H:%M:%S")

        # Sql query
        sql_insert_query = "insert into soil_data values(NULL, %s, %s, %s, %s, %s, %s, %s, %s)"

        dbcursor.executemany(sql_insert_query, payloadNode)

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
