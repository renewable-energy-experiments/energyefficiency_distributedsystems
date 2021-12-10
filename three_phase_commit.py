#!/usr/bin/env python
"""
The master program for CS5414 three phase commit project.
"""
import csv
import sys, os
import time
from threading import Thread

leader = -1  # coordinator
address = 'localhost'
threads = {}
live_list = {}
crash_later = []
wait_ack = False

filename = 'network_traffc_counter.csv'
fieldnames = ['algorithm', 'network_message']


# class commit_protcol(object):
def send(index, data, set_wait_ack=False):
    global leader, live_list, threads, wait_ack
    wait = wait_ack
    while wait:
        time.sleep(0.01)
        wait = wait_ack
    pid = int(index)
    if pid >= 0:
        if pid not in threads:
            print('Master or testcase error!')
            return
        if set_wait_ack:
            wait_ack = True
        threads[pid].send(data)
        return
    pid = leader
    while pid not in live_list or live_list[pid] == False:
        time.sleep(0.01)
        pid = leader
    if set_wait_ack:
        wait_ack = True
    threads[pid].send(data)


def exit(self):
    global threads, wait_ack

    wait = wait_ack
    while wait:
        time.sleep(0.01)
        wait = wait_ack

    time.sleep(2)
    for k in threads:
        threads[k].close()
    # subprocess.Popen(['./stopall'], stdout=open('/dev/null'), stderr=open('/dev/null'))
    time.sleep(0.1)
    os._exit(0)


def main(n):
    global leader, threads, crash_later, wait_ack
    print("[three phase commit] node ", n)
    leader = 0

    lst = []
    for i in range(0, n):
        lst.append(str(i) + " start " + str(9000 + i))
    lst.append("0 add_transaction t1")
    for i in range(1, n):
        lst.append(str(i) + " vote YES")

    print(lst)
    # test case input
    # lines = ["0 start 10002", "1 start 10003", "0 add_transaction t1", "1 vote YES", "exit"]
    #  "2 start 2 10003", "3 start 2 10003",
    #  "0 add_transaction t2", "-1 delete_transaction t2",  "1 vote NO",  "-1 add song2 URL3", "-1 get song1", "-1 get song2","exit"]
    handlerTh = None
    try:
        for line in lst:
            print(line)
            if line == 'exit':
                exit()

            sp2 = line.split()
            pid = int(sp2[0])  # first field is pid
            cmd = sp2[1]  # second field is command
            # print(" pid ", pid, " cmd ", cmd)

            if cmd == 'start':
                port = int(sp2[2])  # third field is the port
                print(" start at port ", port)
                # if no leader is assigned, set the first process as the leader
                if leader == 0:
                    leader = pid
                live_list[pid] = True

                # subprocess.Popen(['./process', str(pid), str(n), str(port)])
                # subprocess.Popen(['./process', str(pid), str(n), str(port)] ,stdout=open('/dev/null'), stderr=open('/dev/null'))
                Thread(energyefficiency.ThreePhaseCommit.process.main(leader, n, port)).start()

                # sleep for a while to allow the process be ready
                time.sleep(1)

                # connect to the port of the pid
                if not pid == leader:
                    print("Connect to the handler ", port)
                    handlerTh = energyefficiency.ThreePhaseCommit.process.ClientHandler(leader, address, port)
                    threads[pid] = handlerTh
                    handlerTh.start()

            elif cmd == 'add_transaction' or cmd == 'delete_transaction':
                send(pid, line, set_wait_ack=True)
            elif cmd == 'vote':
                send(pid, line)
            elif cmd == 'get':
                send(pid, leader, set_wait_ack=True)

        time.sleep(1)

    except Exception as err:
        print("exception {}".format(err))
        print("Encountered unexpected error:", sys.exc_info()[0])
        raise
    finally:
        print("3phase commit message count " + handlerTh.msgCounter)
        with open(filename, 'a') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow({'three_phase_commit', handlerTh.msgCounter})
            # writer.writerow({fieldnames[0]: 'three_phase_commit', fieldnames[1]: msgCounter})
            csvfile.close()
        exit()
