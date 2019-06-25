import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode

import json
import paho.mqtt.client as mqtt

from datetime import datetime

# IP Address of the broker(Rpi)
MQTT_Broker = "192.168.31.154"
# Common port
MQTT_Port = 1883

Keep_Alive_Interval = 45

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    
    client.subscribe("esp8266")
    
def on_message(client, userdata, msg):
    # datetime object containing current date and time
    now = datetime.now()
    
    if msg.payload != "":
        print("Recieved.")
        try:
            # Connection
            connection = mysql.connector.connect(
            host='localhost',
            database='mysql',
            user='root',
            password='admin123')
            
            # Data insertion
            dbcursor = connection.cursor()
            dbcursor.execute("select * from Soil_Data");
            dbcursor.fetchall()
            print("rows:{}".format(dbcursor.rowcount))
            currentrowcount = dbcursor.rowcount + 1;
            print(currentrowcount)
            
            # Extract JSON content
            jsonDoc = json.loads(msg.payload)
            print(jsonDoc['SensorID'])
            
            # dd/mm/YY H:M:S
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            
            # Sql query
            sql_insert_query = "insert into Soil_Data values({}, '{}', '{}', {}, {}, {})".format(currentrowcount, jsonDoc['SensorID'], dt_string, jsonDoc['Nitrogen'], jsonDoc['Phosphorous'], jsonDoc['Potassium'])
            
            dbcursor.execute(sql_insert_query)
            
            # Apply changes
            connection.commit()
            
            dbcursor.close()
            connection.close()
            
            print("Insertion successful")
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