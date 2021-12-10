# Simple listner
import pickle
import socket
import sys
class listner(object):
    """
      SimpleClient encapsulates member variables as well as startClient and sendGroupMessage functions
    """

    def simple_listner(self, host='127.0.0.1', port=8090):
        """
        start siple listernr
        :param host: hostname of server
        :param port: port for connecting to server
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((host, int(port)))
                resp = pickle.loads(s.recv(1024))
                print("%s \n", resp)
            except socket.error as err:
                print("socket creation failed with error {0}".format(err))
            except OSError as err:
                print("Failed to connect: {0}".format(err))
            except:
                print("Encountered error:", sys.exc_info()[0])
            finally:
                s.close()