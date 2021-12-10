# Distributed model
"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Altanai Bisht
:Version: f1-01
"""
import csv
import socket
import sys
import pickle

filename = 'network_traffc_counter.csv'
fieldnames = ['algorithm', 'network_message']
"""
Simple Client for connecting to Group Coordinator Daemon (GCD) and communicating with members 
"""


class Node(object):
    host = "127.0.0.1"
    port = 22
    MSG_JOIN = "JOIN"
    MSG_HELLO = "HELLO"
    SOCKET_TIMEOUT = 1500  # millseconds
    BUF_SIZE = 1024  # bytes

    def sendGroupMessage(self, lst):
        """
        GroupMessage collects response from each of the group members after sending a message
        :param gcdresp: list of group members
        """
        msgCounter = 0
        try:
            for node in lst:
                print("[sendGroupMessage", node)
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as membersocket:
                    print("%s to %s" % (self.MSG_HELLO, node))
                    membersocket.settimeout(self.SOCKET_TIMEOUT / 1000)
                    membersocket.connect((node["host"], int(node["port"])))
                    membersocket.sendall(pickle.dumps(self.MSG_HELLO))
                    msgCounter += 1
                    grpmemresp = pickle.loads(membersocket.recv(self.BUF_SIZE))
                    msgCounter += 1
                    print(grpmemresp)
                membersocket.close()

        except OSError as err:
            print("failed to connect: {0}".format(err))
        except:
            print("Encountered unexpected error:", sys.exc_info()[0])
            raise
        finally:
            # Write results to csv
            print("distributed msgCounter ", msgCounter)
            with open(filename, 'a') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow({'distributed', msgCounter})
                # writer.writerow({fieldnames[0]: 'distributed', fieldnames[1]: msgCounter})
                csvfile.close()
            exit(1)
