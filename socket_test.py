# __author__ = 'sander'
# import socket
# import sys
#
#
#
# s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
#
# try:
#     s.bind((HOST,PORT))
# except socket.error,msg:
#     print "Bind failed. {} {}".format(msg[0],msg[1])
#     sys.exit()
#
# s.listen(1)
# print ("socket now listening")
# conn,add = s.accept()
# try:
#     while 1:
#         print("waiting for incoming data.")
#         data=conn.recv(1024)
#         print (data)
# finally:
#     print ("closing connection")
#     conn.close()