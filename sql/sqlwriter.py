from time import gmtime, strftime
import paho.mqtt.client as mqtt
import sqlite3

temperature_topic = "temperature"
humidity_topic = "humidity"
dbFile = "data.db"

dataTuple = [-1,-1]

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(temperature_topic)
    client.subscribe(humidity_topic)
    
# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    theTime = strftime("%Y-%m-%d %H:%M:%S", gmtime())

    result = (theTime + "\t" + str(msg.payload))
    print(msg.topic + ":\t" + result)
    if (msg.topic == temperature_topic):
        dataTuple[0] = str(msg.payload)
    if (msg.topic == humidity_topic):
        dataTuple[1] = str(msg.payload)
        #return
    if (dataTuple[0] != -1 and dataTuple[1] != -1):
        writeToDb(theTime, dataTuple[0], dataTuple[1])
    return

def writeToDb(theTime, temperature, humidity):
    conn = sqlite3.connect(dbFile)
    c = conn.cursor()
    print "Writing to db..."
    c.execute("INSERT INTO climate VALUES (?,?,?)", (theTime, temperature, humidity))
    conn.commit()

    global dataTuple
    dataTuple = [-1, -1]

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("raspberrypi", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
