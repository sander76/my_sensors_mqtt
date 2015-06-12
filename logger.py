""" MongoDB Logger.

Usage:
  logger.py MONGO_URI MONGO_USR MONGO_PASS MQTT_IP MQTT_PORT LOG_FILE

Options:
  -h --help     Show this screen.

"""
from logging.handlers import RotatingFileHandler
import time

__author__ = 'sander'

from pymongo import MongoClient, errors
import paho.mqtt.client as mqtt_client
import logging

lgr = logging.getLogger(__name__)
from docopt import docopt
from datetime import datetime



class Logger:
    def __init__(self, mongo_uri, mqtt_ip, mqtt_port):
        try:
            self.connect(mqtt_ip, mqtt_port)

            # connect to the mongo db.
            self.client = MongoClient(mongo_uri)
            # get the database
            self.db = self.client.domodb
            # get the collection we are writing to.
            self.collection = self.db.domodata

            # start the mqtt client and block this thread.
            self.mqtt.loop_forever()
        except Exception, e:
            pass

    def connect(self, mqtt_ip, mqtt_port):
        # the mqtt client.
        self.mqtt = mqtt_client.Client()
        self.mqtt.on_message = self.on_message_from_mqtt
        self.mqtt.on_connect = self._on_connect
        #self.mqtt.on_disconnect=self.on_disconnect
        self.mqtt.connect(mqtt_ip, port=mqtt_port)

    # def on_disconnect(self,client, userdata, rc):
    #     lgr.info("disconnected from broker. reconnecting in 10 seconds")
    #     time.sleep(10)
    #     self.connect()

    def _on_connect(self,client, userdata, flags, rc):
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
        try:
            self.collection.find_one_and_update({"hourtime": cur_hour},
                                      {'$set': {"hourtime": cur_hour,
                                                #"topic.desc": mqtt_message.topic,
                                                #"topic.vals.{}".format(cur_minute): mqtt_message.payload,
                                                "topics.{}.vals.{}".format(mqtt_message.topic, cur_minute):mqtt_message.payload
                                                },},

                                      upsert=True)
        except errors.AutoReconnect,e:
            lgr.info("connection to mongodb lost. Reconnecting...")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    arguments = docopt(__doc__)
    try:
        rot_handler = RotatingFileHandler(arguments["LOG_FILE"],
                                          maxBytes=10000,
                                          backupCount=20)
        rot_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        rot_handler.setFormatter(formatter)
        lgr.addHandler(rot_handler)

        mongo_uri = "mongodb://{}:{}@{}".format(arguments["MONGO_USR"],arguments["MONGO_PASS"],arguments["MONGO_URI"])
        lgr.info("starting logger.")
        logger = Logger(mongo_uri,arguments['MQTT_IP'],arguments['MQTT_PORT'])

    except Exception, e:
        lgr.exception(e)
    finally:
        lgr.info("logger stopped.")