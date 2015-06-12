__author__ = 'sander'

from phue import Bridge


b=Bridge(ip="192.168.2.9")


#b.connect()

ap = b.get_api()
#grp = b.get_group(1)
scn = b.get_scene(u'e07a35d7e-on-0')
print scn
#b.set_light("Tafel",'on',True)
