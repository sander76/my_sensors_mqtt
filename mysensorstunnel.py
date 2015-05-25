""" mysensors mqtt tunneler.

Usage:
  mysensorstunnel.py MQTT_ADDRESS MQTT_PORT STRUCTURE_FILE LOG_FILE [SERIAL_PORT]

Options:
  -h --help     Show this screen.

"""

__author__ = 'sander'
import json
import serial
import logging
import paho.mqtt.client as mqtt_client
import fake_serial
import threading
from collections import defaultdict
from docopt import docopt

lgr = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

streamh = logging.StreamHandler()
streamh.setLevel(logging.DEBUG)
streamh.setFormatter(formatter)
lgr.addHandler(streamh)

#='test.mosquitto.org'
#=1883

class Tunneler:
    def __init__(self,
                 mqtt_address,
                 mqtt_port,
                 structure_file,
                 serial_connection=fake_serial.Serial()):

        # the table with mysensors to mqtt topics.
        self.translation_file=structure_file
        self.load_translation_table()
        self.make_node_table(self.translation_table)
        self.make_topic_table(self.translation_table)
        # the mqtt client.
        self.mqtt = mqtt_client.Client()
        self.mqtt.on_message = self.on_message_from_mqtt
        self.mqtt.connect(mqtt_address, port=mqtt_port)
        self.mqtt.subscribe('room/#')


        # the serial connection
        self.serial_connection = serial_connection
        if self.serial_connection.isOpen()==False:
            self.serial_connection.open()
        self.keep_reading=True
        self.t = threading.Thread(target=self.reader)
        #self.t.setDaemon(True)
        self.t.start()
        #self.t.join()

        self.mqtt.loop_forever()
        # u_t = threading.Thread(target=self.update_translation_table,args=(update_host,update_port))
        # u_t.start()

    def load_translation_table(self):
        with open(self.translation_file) as fl:
            self.translation_table=json.load(fl)

    def make_node_table(self,js):
        self.node_table={}
        for itm in js:
            self.node_table[itm["node"]]=[]
            for topic in itm["pub"]:
                self.node_table[itm["node"]].append(topic)

    def make_topic_table(self,js):
        self.topic_table=defaultdict(list)
        for itm in js:
            for topic in itm["sub"]:
                self.topic_table[topic].append(itm['node'])



    def get_mqtt_topic_and_payload(self, node):
        vals = node.split(';')
        node = (';'.join(vals[0:-1]))+';'
        payload=vals[-1]
        lgr.debug("node addres: {} payload: {}".format(node,payload))
        if node in self.node_table:
            return self.node_table[node],payload
        else:
            raise UserWarning("No mqtt topic found for : {}".format(node))


    def get_mysensor_nodes(self, topic):
        if topic in self.topic_table:
            nodes = self.topic_table[topic]
            return nodes
        else:
            raise UserWarning("No nodes found for topic : {}".format(topic))

    def reader(self):
        while self.keep_reading:
            bfr = []
            x = self.serial_connection.read(1)
            bfr.append(x)
            while x:
                x = self.serial_connection.read(1)
                if x == '\n':
                    self.to_mqtt(''.join(bfr))
                    bfr = []
                else:
                    bfr.append(x)
        lgr.debug("Stop this read loop.")

    def to_mqtt(self, data):
        lgr.debug("incoming data from serial port: {}".format(data))
        try:
            topics,payload=self.get_mqtt_topic_and_payload(data)
            for topic in topics:
                self.mqtt.publish(topic,payload,retain=True)
            pass
        except UserWarning, e:
            lgr.debug(e.message)

    def on_message_from_mqtt(self, client, userdata, msg):
        lgr.debug("incoming data from mqtt: topic: {} payload: {}".format(msg.topic, msg.payload))
        try:
            nodes = self.get_mysensor_nodes(msg.topic)
            for node in nodes:
                self.serial_connection.write("{}{}\n".format(node,msg.payload))
        except UserWarning, e:
            lgr.debug(e.message)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    ser=None
    tunnel=None
    try:
        arguments = docopt(__doc__)
        handler=logging.FileHandler(arguments['LOG_FILE'])
        handler.setLevel(logging.WARNING)
        handler.setFormatter(formatter)
        lgr.addHandler(handler)

        #serial = serial.Serial("/dev/ttyMySensorsGateway")
        #logging.basicConfig(level=logging.DEBUG)
        structure = arguments['STRUCTURE_FILE']
        if arguments["SERIAL_PORT"]:
            ser=serial.Serial(arguments['SERIAL_PORT'])
            tunnel=Tunneler(arguments['MQTT_ADDRESS'],arguments['MQTT_PORT'],structure,serial_connection=ser)
        else:
            ser=fake_serial.Serial()
            tunnel=Tunneler(arguments['MQTT_ADDRESS'],arguments['MQTT_PORT'],structure,serial_connection=ser)
        #Tunneler(address="localhost",serial_connection=serial)
    except KeyboardInterrupt:
        lgr.warning("exiting")
    except Exception, a:
        lgr.warning(a)

    finally:
        if ser:
            ser.close()
        if tunnel:
            tunnel.keep_reading=False
            tunnel.mqtt.loop_stop()
