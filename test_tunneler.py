from mysensorstunnel import Tunneler

__author__ = 'sander'

import unittest

class TunnelerTest(unittest.TestCase):
    def setUp(self):
        self.tunneler=Tunneler()

    def test_node_to_mqtt_1(self):
        topic,payload = self.tunneler.get_mqtt_topic_and_payload("1;100;1;0;2;100")
        self.assertEqual((topic,payload),("mysensors/lights/1","100"))

    def test_node_to_mqtt_2(self):
        self.assertRaises(UserWarning,self.tunneler.get_mqtt_topic_and_payload,"1;100")

    # def test_node_to_mqtt_2(self):
    #     self.assertRaises(UserWarning,self.tunneler.get_mqtt_topic_and_payload,"1;100")


    # def incoming_from_mysensors(self):
    #     self.tunneler.serial_connection._data="1;100;1;0;2;100"