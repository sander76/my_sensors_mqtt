import json

__author__ = 'sander'
import serial
import logging
import paho.mqtt.client as mqtt_client
import fake_serial
import threading

lgr = logging.getLogger(__name__)




class Tunneler:
    def __init__(self,translation_table, address='test.mosquitto.org', port=1883, serial_connection=fake_serial.Serial()):
        # the table with mysensors to mqtt topics.
        self.translation_table=translation_table

        # the mqtt client.
        self.mqtt = mqtt_client.Client()
        self.mqtt.on_message = self.on_message_from_mqtt
        self.mqtt.connect(address,port=port)
        self.mqtt.subscribe('mysensors/#')
        self.mqtt.loop_start()

        # the serial connection
        self.serial_connection = serial_connection

        t = threading.Thread(target=self.reader, args=(self.serial_connection,self.to_mqtt))
        t.start()

    def get_mqtt_topic(self,data):
        return data

    def get_mysensor_node(self,topic):
        try:
            topic = next(i[1] for i in self.translation_table if i[0]==topic)
        except StopIteration:
            raise UserWarning("no mysensor node found for : {}".format(topic))
        if topic:
            return topic
        else:
            raise UserWarning("no mysensor node found for : {}".format(topic))


    def reader(self, serial,on_data_received):
        while 1:
            bfr = []
            x = self.serial_connection.read(1)
            bfr.append(x)
            while x:
                x = self.serial_connection.read()
                if x =='\n':
                    self.to_mqtt(''.join(bfr))
                    bfr=[]
                else:
                    bfr.append(x)

    def to_mqtt(self,data):
        lgr.debug("incoming data from serial port.")
        pass


    def on_message_from_mqtt(self,client, userdata, msg):
        lgr.debug("incoming data from mqtt: topic: {} payload: {}".format(msg.topic, msg.payload))
        try:
            self.serial_connection.write("{}{}\n".format(self.get_mysensor_node(msg.topic),msg.payload))
        except UserWarning,e:
            lgr.debug(e.message)


if __name__=="__main__":
    with open('mysensors_mqtt_table.json') as fl:
        js = json.load(fl)
    logging.basicConfig(level=logging.DEBUG)
    #lgr.setLevel(logging.DEBUG)
    Tunneler(js)