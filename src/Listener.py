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
# Array index
i = 0
# The last successful data capture at certain node
lastData = {}
# Tries
tryCount = 0
#
missedNodes = 0


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

    # Topics to listen from
    client.subscribe("esp8266/#")


def on_message(client, userdata, msg):
    jsonDoc = json.loads(msg.payload)

    # Pass payload from NodeMCUs
    get_data(jsonDoc)


def get_data(jsonDoc):
    global i, tryCount, missedNodes, lastData, client
    print("Recieved node:{}".format(jsonDoc["SensorID"]))
    print("# of tries:{} with {}".format(tryCount, referenceNodes[i]))
    print("# of missed nodes:{} \n".format(missedNodes))
    tryCount += 1

    if tryCount > 10 and referenceNodes[i] == "Node-6":
        print("WARNING: Last node is reached and still no data.\nAttempting to fill with the last captured data...\n")

        client.disconnect()

        missedNodes += 1

        currentNode = i

        # Back to Node-1
        i = 0

        while missedNodes > 0:
            print("Auto filling missed nodes:{}".format(missedNodes))

            # Insert data for missed nodes
            print("Starting data insertion of sensorid:{}".format(
                referenceNodes[currentNode]))

            try:
                # Connection to remote webserver
                connection = mysql.connector.connect(
                    host='2.57.89.1',
                    database='u690747639_soil',
                    user='u690747639_root',
                    password='upNhZDv5NBu756is')

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
                sql_insert_query = "insert into soil_data values(NULL, '{}', '{}', {}, {}, {}, {}, {}, {}, '{}')".format(
                    referenceNodes[i],
                    curDate,
                    lastData['Nitrogen'],
                    lastData['Phosphorous'],
                    lastData['Potassium'],
                    lastData['Nitrogen_FRQ'],
                    lastData['Phosphorous_FRQ'],
                    lastData['Potassium_FRQ'],
                    "PROCEDURALLY-FILLED")

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

            missedNodes -= 1

            currentNode -= 1
        else:
            tryCount = 0

            # Reconnect
            client.connect(MQTT_Broker, MQTT_Port, Keep_Alive_Interval)

            print("All missed nodes are filled.\n")

    if tryCount < 10 and jsonDoc["SensorID"] == referenceNodes[i]:
        #
        client.disconnect()

        print("NODE MATCH! Attempt of insertion...")

        # Reset try counts
        tryCount = 0

        # Insert data with the current payload
        print("Starting data insertion of sensorid:{}".format(
            referenceNodes[i]))
        try:
            # Connection to remote webserver
            connection = mysql.connector.connect(
                host='2.57.89.1',
                database='u690747639_soil',
                user='u690747639_root',
                password='upNhZDv5NBu756is')

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
            sql_insert_query = "insert into soil_data values(NULL, '{}', '{}', {}, {}, {}, {}, {}, {}, '{}')".format(
                referenceNodes[i],
                curDate,
                jsonDoc['Nitrogen'],
                jsonDoc['Phosphorous'],
                jsonDoc['Potassium'],
                jsonDoc['Nitrogen_FRQ'],
                jsonDoc['Phosphorous_FRQ'],
                jsonDoc['Potassium_FRQ'],
                "NORMALLY-FILLED")

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

        # Capture the successful data for reserve
        lastData = jsonDoc

        # Remember the current node
        currentNode = i

        # Proceed to next node
        i += 1

        while missedNodes > 0:
            print("Auto filling missed nodes:{}".format(missedNodes))

            # Until no missing nodes left
            currentNode -= 1

            # Insert data for missed nodes
            print("Starting data insertion of sensorid:{}".format(
                referenceNodes[currentNode]))
            try:
                # Connection to remote webserver
                connection = mysql.connector.connect(
                    host='2.57.89.1',
                    database='u690747639_soil',
                    user='u690747639_root',
                    password='upNhZDv5NBu756is')

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
                sql_insert_query = "insert into soil_data values(NULL, '{}', '{}', {}, {}, {}, {}, {}, {}, '{}')".format(
                    referenceNodes[currentNode],
                    curDate,
                    jsonDoc['Nitrogen'],
                    jsonDoc['Phosphorous'],
                    jsonDoc['Potassium'],
                    jsonDoc['Nitrogen_FRQ'],
                    jsonDoc['Phosphorous_FRQ'],
                    jsonDoc['Potassium_FRQ'],
                    "PROCEDURALLY-FILLED")

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

            # We have filled the missed nodes
            missedNodes -= 1
        else:
            # Reconnect
            client.connect(MQTT_Broker, MQTT_Port, Keep_Alive_Interval)

            print("All missed nodes are filled. Proceeding to {}...\n".format(
                referenceNodes[i]))
    elif tryCount > 10 and jsonDoc["SensorID"] != referenceNodes[i]:
        print("Proceeding to next node...\n")

        # Proceed to next node
        i += 1

        # Reset try counts
        tryCount = 0

        # Remember what nodes are missed by identifying the preceeding index
        missedNodes += 1

    # We only have 6 nodes
    if i > 5:
        i = 0


# Events to listen
client = mqtt.Client()
client.on_message = on_message
client.on_connect = on_connect

# Init connection
client.connect(MQTT_Broker, MQTT_Port, Keep_Alive_Interval)

# Continous listening to the broker
client.loop_forever()
