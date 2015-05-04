import json
import socket
import sys

__author__ = 'sander'
import serial
import logging
import paho.mqtt.client as mqtt_client
import fake_serial
import threading

lgr = logging.getLogger(__name__)
lgr.setLevel(logging.DEBUG)

handler=logging.FileHandler("/home/pi/tunneler.log")
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
lgr.addHandler(handler)

class Tunneler:
    def __init__(self,
                 address='test.mosquitto.org',
                 port=1883,
                 serial_connection=fake_serial.Serial(),
                 update_host='',
                 update_port=8889):
        # the table with mysensors to mqtt topics.

        self.translation_file="mysensors_mqtt_table.json"
        self.load_translation_table()

        # the mqtt client.
        self.mqtt = mqtt_client.Client()
        self.mqtt.on_message = self.on_message_from_mqtt
        self.mqtt.connect(address, port=port)
        self.mqtt.subscribe('mysensors/#')
        self.mqtt.loop_start()

        # the serial connection
        self.serial_connection = serial_connection

        t = threading.Thread(target=self.reader)
        t.start()

        u_t = threading.Thread(target=self.update_translation_table,args=(update_host,update_port))
        u_t.start()

    def load_translation_table(self):
        with open(self.translation_file) as fl:
            self.translation_table=json.load(fl)

    def save_translation_table(self,js):
        with open(self.translation_file,"w") as fl:
            json.dump(js,fl)

    def update_translation_table(self,update_host,update_port):
        s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)

        try:
            s.bind((update_host,update_port))
        except socket.error,msg:
            lgr.error("Bind failed. {} {}".format(msg[0],msg[1]))

            #sys.exit()

        s.listen(1)
        lgr.debug("socket now listening")
        conn,add = s.accept()
        try:
            while 1:
                lgr.debug("waiting for incoming data.")
                data=conn.recv(1024)
                try:
                    lgr.debug(data)
                    js = json.loads(data)
                    self.translation_table=js
                    self.save_translation_table(js)
                except ValueError,msg:
                    lgr.debug(msg.message)

        finally:
            print ("closing connection")
            conn.close()

    def get_mqtt_topic_and_payload(self, node):
        vals = node.split(';')
        node = (';'.join(vals[0:-1]))+';'
        payload=vals[-1]
        lgr.debug("node addres: {} payload: {}".format(node,payload))
        try:
            topic = next(i[0] for i in self.translation_table if i[1] == node)
            return topic,payload
        except StopIteration:
            raise UserWarning("No mqtt topic found for : {}".format(node))
        except TypeError:
            raise UserWarning("No translation table available.")


    def get_mysensor_node(self, topic):
        try:
            node = next(i[1] for i in self.translation_table if i[0] == topic)
            return node
        except StopIteration:
            raise UserWarning("no mysensor node found for : {}".format(topic))
        except TypeError:
            raise UserWarning("No translation table available.")

    def reader(self):
        while 1:
            bfr = []
            x = self.serial_connection.read(1)
            bfr.append(x)
            while x:
                x = self.serial_connection.read()
                if x == '\n':
                    self.to_mqtt(''.join(bfr))
                    bfr = []
                else:
                    bfr.append(x)

    def to_mqtt(self, data):
        lgr.debug("incoming data from serial port: {}".format(data))
        try:
            topic,payload=self.get_mqtt_topic_and_payload(data)
            self.mqtt.publish(topic,payload,retain=True)
            pass
        except UserWarning, e:
            lgr.debug(e.message)

    def on_message_from_mqtt(self, client, userdata, msg):
        lgr.debug("incoming data from mqtt: topic: {} payload: {}".format(msg.topic, msg.payload))
        try:
            self.serial_connection.write("{}{}\n".format(self.get_mysensor_node(msg.topic), msg.payload))
        except UserWarning, e:
            lgr.debug(e.message)


if __name__ == "__main__":
    serial = serial.Serial("/dev/ttyMySensorsGateway")
    #logging.basicConfig(level=logging.DEBUG)
    Tunneler(address="localhost",serial_connection=serial)