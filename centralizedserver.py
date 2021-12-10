# centralizer_server program
"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Altanai Bisht
:Version: f1-01
"""
import csv
import pickle
import socket
import sys
import time

timeout = time.time() + 60 * .5  # half min from now

filename = 'network_traffc_counter.csv'
fieldnames = ['algorithm', 'network_message']

class Server(object):
    def centralizer_server(self, port=8090):

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', port))
            s.listen(1)
            conn, addr = s.accept()
            with conn:
                print('Connected by', addr)
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break
                    conn.sendall(data)

                    if time.time() > timeout:
                        conn.close()
                        break


class Client(object):
    """
      SimpleClient encapsulates member variables as well as startClient and sendGroupMessage functions
    """
    host = "127.0.0.1"
    port = 22
    MSG_JOIN = "JOIN"
    MSG_HELLO = "HELLO"
    SOCKET_TIMEOUT = 1500  # millseconds
    BUF_SIZE = 1024  # bytes
    msgCounter = 0

    def startClient(self, host='127.0.0.1', port=8090):
        """
        startClient connects to Group Coordinator Daemon (GCD) server
        :param host: hostname of server
        :param port: port for connecting to server
        """
        msgCounter = 0
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as gcdsocket:
            try:
                gcdsocket.connect((host, int(port)))
                print("%s ('%s' %i)\n" % ("JOIN", host, port))
                gcdsocket.sendall(pickle.dumps("JOIN"))
                msgCounter += 1
                resp = pickle.loads(gcdsocket.recv(1024))
                print("%s \n", resp)
                msgCounter += 1
            except socket.error as err:
                print("socket creation failed with error {0}".format(err))
            except OSError as err:
                print("Failed to connect: {0}".format(err))
            except:
                print("Encountered error:", sys.exc_info()[0])
            finally:
                gcdsocket.close()
                # Write results to csv
                print("centralized msgCounter ", msgCounter)
                with open(filename, 'a') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow({'centralized',  msgCounter})
                    # writer.writerow({fieldnames[0]: 'centralized', fieldnames[1]: msgCounter})
                    csvfile.close()
                exit(1)