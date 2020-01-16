from time import gmtime, strftime
import paho.mqtt.client as mqtt
import sqlite3

all_topics = "JsonDataFromBatteries/#"
dbFile = "mqttbatterydata.db"
tableDb = "batterydataA"

def createTableDb()
    print ("creating table E")
    connection = sqlite3.connect(dbFile)
    cursor = connection.cursor()
    sql ='''CREATE TABLE E (FIRST_NAME CHAR(20) NOT NULL )'''
    cursor.execute(sql)
    connection.commit()
    
    
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    createTableDb()
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(all_topics)
    
# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    theTime = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    result = (theTime + "\t" + str(msg.payload))
    print(msg.topic + ":\t" + result)
    if (msg.topic == all_topics):
        writeToDb(str(msg.payload))
        #return
    return

def writeToDb(datatoDb):
    conn = sqlite3.connect(dbFile)
    c = conn.cursor()
    print ("Writing to db...")
    c.execute("INSERT INTO"+tableDb+"VALUES ("+ datatoDb +")")
    conn.commit()

    
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("raspberrypi", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
