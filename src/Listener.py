import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode

import json
import paho.mqtt.client as mqtt

import time
import threading
from datetime import datetime

# IP Address of the broker(Rpi)
MQTT_Broker = "192.168.31.51"
# Common port
MQTT_Port = 1883
# Connection lifespan
Keep_Alive_Interval = 60

# Reference node list
referenceNodes = ["Node-1", "Node-2", "Node-3", "Node-4", "Node-5", "Node-6"]
# Array index
i = 0
# The node to get from payload
payloadNode = {}
# Tries
tryCount = 0
#
missedNodes = 0


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

    # Extract payload as JSON format
    jsonDoc = json.load(msg.payload)

    global i, tryCount, missedNodes
    if tryCount < 10 and jsonDoc["SensorID"] == referenceNodes[i]:
        # Reset try counts
        tryCount = 0

        # Insert data with the current payload
        data_insertion(jsonDoc, jsonDoc["SensorID"])

        while missedNodes > 0:
            # Until no missing nodes left
            i -= 1

            # Insert data for missed nodes
            data_insertion(jsonDoc, referenceNodes[i])

            # We have filled the missed nodes
            missedNodes -= 1

        # Go to next node
        i += 1

    elif tryCount > 10 and jsonDoc["SensorID"] != referenceNodes[i]:
        # Remember what nodes are missed by identifying the preceeding index
        missedNodes += 1

        # SUGGESTION: that if tries exceeded 10 times, should signal node as "STOPPED WORKING"

        # Proceed to next node
        i += 1

    # We only have 6 nodes
    if i > 6:
        i = 0


# Events to listen
client = mqtt.Client()
client.on_message = on_message
client.on_connect = on_connect

# Init connection
client.connect(MQTT_Broker, MQTT_Port, Keep_Alive_Interval)

# Continous listening to the broker
client.loop_forever()


def data_insertion(data, sensorid):
    try:
        # Connection to remote webserver
        connection = mysql.connector.connect(
            host='127.0.0.1',
            database='mysql',
            user='root',
            password='')

        # Error check
        if connection.is_connected:
            print("Successfully connected to MySQL")
        else:
            print("Connection failed")

        # Current row check
        dbcursor = connection.cursor()
        dbcursor.execute("select * from soil_data")
        dbcursor.fetchall()
        print("rows:{}".format(dbcursor.rowcount))

        now = datetime.now()
        curDate = now.strftime("%d-%m-%Y %H:%M:%S")

        # Sql query
        sql_insert_query = "insert into Soil_Data values(NULL, '{}', '{}', {}, {}, {}, {}, {}, {})".format(
            sensorid,
            curDate,
            data['Nitrogen'],
            data['Phosphorous'],
            data['Potassium'],
            data['Nitrogen_FRQ'],
            data['Phosphorous_FRQ'],
            data['Potassium_FRQ'])

        # Execute query
        dbcursor.execute(sql_insert_query)

        # Apply changes by the executed query
        connection.commit()

        # Dispose connection after use
        connection.close()
        dbcursor.close()
    except mysql.connector.Error as error:
        connection.rollback()
        print("Insertion failed:" + str(error))
