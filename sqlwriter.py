#!/usr/bin/env python3
from time import gmtime, strftime
import paho.mqtt.client as mqtt
import sqlite3


all_topics = "JsonDataFromBatteries/#"
dbFile = "mqttbatterydata.db"

##############################   mqtt  ################################################   
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(all_topics)
    
# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
 #   theTime = strftime("%Y-%m-%d %H:%M:%S", gmtime())
 #   result = (theTime + "\t" + str(msg.payload))
 #   print(msg.topic + ":\t" + result)
   # if (str(msg.topic) == all_topics):
    print("Message received from topic: " + str(msg.topic) + " payload: " + str(msg.payload))
    tablename = str(msg.topic).split("/")
    print("creating table:" +tablename[1])
    createTableDb(tablename[1])
    writeToDb(tablename[1],str(msg.payload))
        #return
    return

#######################   database   ##################################################
#Creating table in database
def createTableDb(TABLE):
 
    print ("creating table: "+TABLE)
    connection = sqlite3.connect(dbFile)
    cursor = connection.cursor()
    sql_create ="CREATE TABLE IF NOT EXISTS "+TABLE+" ( timestamp TEXT, DATA TEXT )"
    print (sql_create)
    cursor.execute(sql_create)
    connection.commit()
    cursor.close()
    connection.close()

    
def writeToDb(TABLE,datatoDb):
    print ("writting table: "+TABLE)
    datatoDb=datatoDb[1:len(datatoDb)]#eliminate that molest b at the begining
    timestamptoDb = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    connection = sqlite3.connect(dbFile)
    cursor = connection.cursor()
    sql_insert ="INSERT INTO "+TABLE+"(timestamp,DATA) VALUES ('"+timestamptoDb+"',"+ datatoDb +")"
    print (sql_insert)
    cursor.execute(sql_insert)
    connection.commit()
    cursor.close()
    print("Success Writting table");
    connection.close()

    
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("raspberrypi", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
