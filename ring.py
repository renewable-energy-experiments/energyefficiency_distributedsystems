# Ring model
"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Altanai Bisht
:Version: f1-01
"""
import time
import socket
import sys
import pickle
import csv

filename = 'network_traffc_counter.csv'
fieldnames = ['algorithm', 'network_message']


class Client(object):
    s = socket.socket()
    ss = socket.socket()
    host = socket.gethostname()

    currentNode = 0
    msgCounter = 0
    BUF_SIZE = 1024  # bytes
    time.sleep(5)  # to wait to other node to run the program

    def start_ring_node(self, host, port, targetnode):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((host, port))
                s.listen(1)
                conn, addr = s.accept()
                print("Ring node connected from", addr)
                with conn:
                    print('targetnode', targetnode)
                    data = pickle.loads(conn.recv(self.BUF_SIZE))
                    self.msgCounter += 1
                    if data != targetnode:
                        #  pass through the ring
                        nexthost = ''
                        nextport = port + 1
                        self.sendNextRingMessage(nexthost, nextport, targetnode)
                    else:
                        print("Received token back after looping through the ring")
                        # Write results to csv
                        print("ring msgCounter ", self.msgCounter)
                        with open(filename, 'a') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow({'ring', self.msgCounter})
                            csvfile.close()
                        exit(1)
        except:
            print("Encountered error:", sys.exc_info()[0])

    def sendNextRingMessage(self, host, port, targetnode):
        #  start the msg
        print("sendto ", (host, port))
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ss:
                ss.connect((host, port))
                ss.sendall(pickle.dumps(targetnode))
                self.msgCounter += 1
        except:
            print("Encountered error:", sys.exc_info()[0])
