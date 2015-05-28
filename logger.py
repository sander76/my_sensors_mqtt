""" MongoDB Logger.

Usage:
  logger.py MONGO_URI MONGO_USR MONGO_PASS MQTT_IP MQTT_PORT

Options:
  -h --help     Show this screen.

"""

__author__ = 'sander'

from pymongo import MongoClient
import paho.mqtt.client as mqtt_client
import logging

lgr = logging.getLogger(__name__)
from docopt import docopt
from datetime import datetime



class Logger:
    def __init__(self, mongo_uri, mqtt_ip, mqtt_port):
        self.connect(mqtt_ip, mqtt_port)

        # connect to the mongo db.
        self.client = MongoClient(mongo_uri)
        # get the database
        self.db = self.client.domodb
        # get the collection we are writing to.
        self.collection = self.db.domodata

        # start the mqtt client and block this thread.
        self.mqtt.loop_forever()

    def connect(self, mqtt_ip, mqtt_port):
        # the mqtt client.
        self.mqtt = mqtt_client.Client()
        self.mqtt.on_message = self.on_message_from_mqtt
        self.mqtt.connect(mqtt_ip, port=mqtt_port)
        self.mqtt.subscribe('room/temperatures/1')

    def on_message_from_mqtt(self, client, userdata, msg):
        lgr.debug("incoming data from mqtt: topic: {} payload: {}".format(msg.topic, msg.payload))
        self.to_mongodb(msg)

    def get_current_hour_and_minute(self):
        dt = datetime.utcnow()
        zero_dt = datetime(dt.year, dt.month, dt.day, dt.hour)
        return (zero_dt, dt.minute)

    def to_mongodb(self, mqtt_message):
        cur_hour, cur_minute = self.get_current_hour_and_minute()
        self.collection.find_one_and_update({"hourtime": cur_hour},
                                  {'$set': {"hourtime": cur_hour,
                                            "topic.desc": mqtt_message.topic,
                                            "topic.vals.{}".format(cur_minute): mqtt_message.payload}},
                                  upsert=True)


if __name__ == "__main__":
    arguments = docopt(__doc__)

    mongo_uri = "mongodb://{}:{}@{}".format(arguments["MONGO_USR"],arguments["MONGO_PASS"],arguments["MONGO_URI"])

    logger = Logger(mongo_uri,arguments['MQTT_IP'],arguments['MQTT_PORT'])
