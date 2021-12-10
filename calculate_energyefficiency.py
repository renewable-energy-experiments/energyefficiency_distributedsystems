import csv
import sys
import threading

import matplotlib.pyplot as plt

import centralizedserver, distributednodes, ring, lamport, three_phase_commit, mesh, server

filename = 'network_traffc_counter.csv'
fieldnames = ['algorithm', 'network_message']


def calculate_centralized(n=2):
    print(" -------------------------------------------------------------------------------------- ")
    # centralizedserver.Server.centralizer_server()
    # centralizedserver.Client.startClient('127.0.0.1', 8050 + i,)
    # os.system("xterm -e \"centralizedserver.Server.centralizer_server()\"&")
    # os.system("xterm -e \"centralizedserver.Client.run_cleint()\"&")
    print(" calculate_centralized ")
    try:
        th = threading.Thread(target=server.listner().simple_listner, args=('', 8090))
        th.start()
        th2 = threading.Thread(target=centralizedserver.Client().startClient, args=('', 8090))
        th2.start()
        th.join()
        th2.join()
    except Exception as err:
        print("exception {}".format(err))


def calculate_distributed(n):
    print(" -------------------------------------------------------------------------------------- ")
    print(" calculate_distributed ")
    try:
        lst = []
        for i in range(0, n):
            lst.append({
                "host": "localhost",
                "port": 8090 + i
            })
        #  create n nodes to listen
        for i in range(0, n):
            th0 = threading.Thread(target=server.listner().simple_listner, args=('', 8090))
            th0.start()
        th = threading.Thread(target=distributednodes.Node().sendGroupMessage, args=(lst,))
        th.start()
        th.join()

    except Exception as err:
        print("exception {}".format(err))


def calculate_ring(n):
    """
    Calculates msg count in ring topology for packet to reach target
    :param n: number of nodes
    """
    print(" -------------------------------------------------------------------------------------- ")
    print(" calculate_ring ")
    try:
        targetnode = 8090 + n - 1
        #  create n nodes listening
        for i in range(0, n):
            th0 = threading.Thread(target=ring.Client().start_ring_node, args=('', 8090 + i, targetnode))
            th0.start()
        th = threading.Thread(target=ring.Client().sendNextRingMessage, args=('', 8090, targetnode))
        th.start()
        th.join()

    except Exception as err:
        print("exception {}".format(err))


def calculate_mesh(n):
    """ mesh is like distributed , except everyone is trying to connect to everyone else in the network."""
    print(" -------------------------------------------------------------------------------------- ")
    print(" calculate_mesh ", n)
    try:
        lst = []
        for i in range(0, n):
            lst.append({
                "host": "localhost",
                "port": 8040 + i
            })
        for i in range(0, n):
            th0 = threading.Thread(target=server.listner().simple_listner, args=('', 8040 + i))
            th0.start()
        for i in lst:
            th = threading.Thread(target=mesh.Node().sendMeshMsgs, args=(lst,))
            th.start()
            th.join()

    except Exception as err:
        print("exception {}".format(err))


def calculate_lamport(n):
    print(" -------------------------------------------------------------------------------------- ")
    print(" calculate_lampport ")
    try:
        th = threading.Thread(target=lamport.lamposrtSync(n))
        th.start()
        th.join()

    except Exception as err:
        print("exception {}".format(err))


#  Removed since it has lot of fixes
# def calculate_bully(n):
#     """
#     Calculate message count of bully leader election algorithm
#     :param n: num of nodes
#     """
#     print(" -------------------------------------------------------------------------------------- ")
#     try:
#         th = threading.Thread(target=bully_algorithm.Bully(('', 8090), n))
#         th.start()
#         th.join()
#
#     except Exception as err:
#         print("exception {}".format(err))


# def calculate_two_pc(n):
#     """
#     Calculate message count of 2 phase commit protocol
#     :param n: number of nodes
#     """
#     print(" -------------------------------------------------------------------------------------- ")
#     th = threading.Thread(target=three_phase_commit.main(n))
#     th.start()
#     th.join()


def calculate_three_pc(n):
    """
    Calculate message count of 3 phase commit protocol
    :param n: num of nodes
    """
    print(" -------------------------------------------------------------------------------------- ")
    try:
        #  python 3pc/2pc < 3pc/functional.input 2> 3pc/functional.err > 3pc/functional.output
        th = threading.Thread(target=three_phase_commit.main(n))
        th.start()
        th.join()
    except Exception as err:
        print("exception {}".format(err))


def create_graph():
    """
    Plot the message counts from various algorithms to analyze their energy efficiency
    :return:
    """
    print("finished usecase, read results from file")
    x = []
    y = []
    with open(filename, 'r') as csvfile:
        plots = csv.reader(csvfile, delimiter=',')
        for row in plots:
            print(row)
            if not row[0].isdigit():
                x.append(row[0])
                y.append(row[1])
            else:
                x.append(row[1])
                y.append(row[0])
    #  line chart plt.plot(x, y)
    # bar chart
    plt.bar(x, y)
    plt.xlabel(fieldnames[0])
    plt.ylabel(fieldnames[1])
    plt.title('Distributed system algorithms and their network message count')
    plt.legend()
    plt.show()


def create_graph_carbonemission():
    """
    plot carbon rmission from each algorithm based on data of traffic just collected
    :return:
    """
    print("finished usecase, read results from file")
    x = []
    y = []
    with open(filename, 'r') as csvfile:
        plots = csv.reader(csvfile, delimiter=',')
        for row in plots:
            print(row)
            if not row[0].isdigit():
                msgCount = row[1]
                algoname = row[0]
            else:
                msgCount = row[0]
                algoname = row[1]

            # print(" Msg count ", msgCount)
            carbonfootprint = 0.0
            if msgCount.isdigit() and msgCount != 0:
                carbonfootprint = float(msgCount) * 0.02 * 0.000000429
            x.append(algoname)
            y.append(carbonfootprint)

    plt.bar(x, y)
    # plt.plot(x, y,
    #          # label=y,
    #          linestyle='dashed', linewidth=1,
    #          marker='o', markerfacecolor='blue', markersize=5)
    plt.xlabel('nodes')
    plt.ylabel('g Co2 per microwatt hour')

    plt.title('carbon footprint of Distributed system algorithms')
    # plt.legend()
    plt.show()


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: python3 calculate_energyefficiency.py <nodecount>")
        # exit(1)
        print("assigning default 2 for this run")
        n = 4
    else:
        n = int(sys.argv[1])

    series = list(range(1, n))
    nodes = {}

    # write headers onto an empty csv file
    # with open(filename, 'w') as csvfile:
    #     fieldnames = ['algorithm', 'network_message']
    #     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    #     writer.writeheader()
    #     csvfile.close()

    # calculate_centralized(n)
    # calculate_distributed(n)
    # calculate_mesh(n)
    # # calculate_bully(n)
    # calculate_ring(n)
    # calculate_lamport(n)
    # # calculate_two_pc(n)
    # calculate_three_pc(n)
    #
    # # print(nodes)
    create_graph()
    # create_graph_carbonemission()
