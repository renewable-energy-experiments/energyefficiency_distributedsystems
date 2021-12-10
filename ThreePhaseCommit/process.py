#Three Phase Commit
# Code adapted heavily form https://github.com/dkmiller/3PC. (unlicensed Code )

import json
import threading

from numpy import long

import sys, os
import time
from socket import SOCK_STREAM, socket, AF_INET
from threading import Thread, Lock

client = 1
root_port = 20000
address = 'localhost'
processes = dict()
threads = dict()
conns = dict()
outgoing_conns = dict()
my_pid = -2
timeout_wait = 40

class TimeoutThread(Thread):
    def __init__(self, timeout, waiting_on):
        Thread.__init__(self)
        self.timeout = timeout
        self.waiting_on = waiting_on
        self.__is__running = True
        self.__suspend__ = True

    def run(self):
        global timeout_wait
        while (self.__is__running):
            if not self.__suspend__:
                time.sleep(0.5)
                self.timeout -= 0.5

                if (self.timeout <= 0):
                    if (self.waiting_on == 'coordinator-vote-req'):
                        print('timed out waiting for vote-req!')
                        # Run re-election protocol
                        # self.suspend()
                        #  TODO : FIX this
                        # sys.exit(1)
                        self.stop()
                        # client.re_election_protocol()
                        # self.timeout = timeout_wait
                    elif self.waiting_on == 'coordinator-precommit':
                        print('timed out waiting for precommit')
                        self.suspend()
                        # Run Termination protocol
                        self.timeout = timeout_wait
                    elif self.waiting_on == 'process-vote':
                        self.suspend()
                        print('timed out waiting for votes')
                        # Send abort to all
                    elif self.waiting_on == 'process-acks':
                        self.suspend()
                        print('timed out waiting for acks')
                        # Send Commits to remaining processes
                        client.after_timed_out_on_acks()
                    elif self.waiting_on == 'coordinator-commit':
                        self.suspend()
                        print('timed out waiting for commit')
                        # Termination protocol
            if self.__is__running == False:
                sys.exit()

    def reset(self):
        global timeout_wait
        self.timeout = timeout_wait

    def restart(self):
        self.reset()
        self.__suspend__ = False

    def suspend(self):
        self.__suspend__ = True

    def stop(self):
        self.__is__running = False


class ListenThread(Thread):
    def __init__(self, conn, addr):
        Thread.__init__(self)
        self.conn = conn
        self.addr = addr

    # From Daniel
    def run(self):
        global client
        while True:
            try:
                data = self.conn.recv(1024)
                if data != "":
                    data = data.split('\n')
                    data = data[:-1]
                    for line in data:
                        # print "process - Inside loop - " + str(line)
                        client.receive(line)

            except:
                break


class WorkerThread(Thread):
    def __init__(self, address, internal_port, pid):
        Thread.__init__(self)
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.bind((address, internal_port))
        self.sock.listen(1)

    def run(self):
        global threads
        while True:
            conn, addr = self.sock.accept()
            handler = ListenThread(conn, addr)
            handler.start()


class ClientHandler(Thread):
    def __init__(self, index, address, port):
        Thread.__init__(self)
        self.index = index
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.connect((address, port))
        self.buffer = ""
        self.valid = True

    def run(self):
        global leader, threads, wait_ack
        while self.valid:
            if "\n" in self.buffer:
                # print self.buffer
                (l, rest) = self.buffer.split("\n", 1)
                self.buffer = rest
                s = l.split()
                if len(s) < 2:
                    continue
                if s[0] == 'coordinator':
                    leader = int(s[1])
                    wait_ack = False
                elif s[0] == 'resp':
                    sys.stdout.write(s[1] + '\n')
                    sys.stdout.flush()
                    wait_ack = False
                elif s[0] == 'ack':
                    wait_ack = False
                else:
                    print(s)
            else:
                try:
                    data = self.sock.recv(1024)
                    # sys.stderr.write(data)
                    self.buffer += data
                except:
                    print(sys.exc_info())
                    self.valid = False
                    del threads[self.index]
                    self.sock.close()
                    break

    def send(self, s):
        if self.valid:
            self.sock.send(str(s) + '\n')

    def close(self):
        try:
            self.valid = False
            self.sock.close()
        except:
            pass


# class ClientHandler(Thread):
#     def __init__(self, index, address, port):
#         Thread.__init__(self)
#         self.index = index
#         self.sock = socket(AF_INET, SOCK_STREAM)
#         self.sock.connect((address, port))
#         self.valid = True
#
#         def run(self):
#             while True:
#                 a = 1  # do something
#
#         def send(self, msg):
#             self.sock.send(str(msg) + '\n')


# master
class MasterHandler(Thread):
    def __init__(self, index, address, port):
        Thread.__init__(self)
        self.index = index
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.bind((address, port))
        self.sock.listen(1)
        self.conn, self.addr = self.sock.accept()
        self.valid = True

    def run(self):
        global client, conns, threads
        # conns[-1] = self.conn
        while True:
            try:
                data = self.conn.recv(1024)
                if data:
                    data = data.split('\n')
                    data = data[:-1]
                    for line in data:
                        client.receive_master(line)
            except:
                print(sys.exc_info())
                # self.valid = False
                # del threads[self.index]
                self.sock.close()
                break

    def send(self, s):
        if self.valid:
            self.conn.send(str(s) + '\n')

    def close(self):
        try:
            self.valid = False
            self.sock.close()
        except:
            pass


def send_many(p_id_list, data):
    print('send_many called by ' + str(p_id_list))
    global root_port, outgoing_conns, address, my_pid
    true_list = []
    for p_id in p_id_list:
        if p_id == -1:
            print(str(data))
            outgoing_conns[p_id].send(str(data) + '\n')
            true_list.append(p_id)
            continue
        # if p_id == my_pid:
        #  true_list.append(p_id)
        #  client.receive(data)
        #  continue

        try:
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect((address, root_port + p_id))
            sock.send(str(data) + '\n')
            sock.close()
        except:
            continue
        true_list.append(p_id)
    return true_list


def main(pid, num_processes, myport):
    global address, client, root_port, processes, outgoing_conns
    print("Process  pid ", pid, "| num of process :", num_processes, " | myport :", myport)
    # Timeouts
    # coordinator_timeout_vote_req = TimeoutThread(timeout_wait, 'coordinator-vote-req')
    # coordinator_timeout_precommit = TimeoutThread(timeout_wait, 'coordinator-precommit')
    # coordinator_timeout_commit = TimeoutThread(timeout_wait, 'coordinator-commit')
    # process_timeout_vote = TimeoutThread(timeout_wait, 'process-vote')
    # process_timeout_acks = TimeoutThread(timeout_wait, 'process-acks')
    # coordinator_timeout_vote_req.start()
    # coordinator_timeout_precommit.start()
    # coordinator_timeout_commit.start()
    # process_timeout_vote.start()
    # process_timeout_acks.start()

    # # Connection with MASTER
    # print("MasterHandler")
    # mhandler = MasterHandler(pid, address, myport)
    # outgoing_conns[-1] = mhandler

    # All incoming connections
    # print("WorkerThread")
    # handler = WorkerThread(address, root_port + pid, pid)
    # handler.start()

    # All outgoing connections
    ##  for pno in range(num_processes):
    ##    if pno == pid:
    ##      continue
    ##    handler = ClientHandler(pno, address, root_port+pno)
    ##    outgoing_conns[pno] = handler
    ##    handler.start()
    try:
        # client = Client(pid, num_processes, send_many, coordinator_timeout_vote_req, coordinator_timeout_precommit,
        #                 coordinator_timeout_commit, process_timeout_vote, process_timeout_acks)
        client = Client(pid, num_processes, send_many)
        print("Client has been initiated")
        client.load_state()
        # mhandler.start()
        # coordinator_timeout_vote_req.restart()
        # while True:
        #     a = 1

    except:
        print("Encountered unexpected error:", sys.exc_info()[0])
        raise
        TimeoutThread.stop()
        # coordinator_timeout_vote_req()
        # coordinator_timeout_precommi.clear()
        # coordinator_timeout_commit.clear()
        # process_timeout_vote.clear()
        # process_timeout_acks.clear()
        os_.exit(1)


class Client:
    global msgCounter
    msgCounter= 0
    def __init__(self, pid, num_procs, send, c_t_vote_req, c_t_prec, c_t_c, p_t_vote, p_t_acks):
        # Array of processes that we think are alive.
        self.alive = [pid]
        # True if a process restarts
        self.stupid = False
        # Unknown coordinator.
        self.coordinator = None
        # Internal hash table of URL : song_name.
        self.data = {}
        # Flags to crash at a certain moment, or to vote no.
        self.flags = {}
        # Process id.
        self.id = pid
        # Single global lock.
        self.lock = threading.Lock()
        self.logfile = '%dlog.p' % pid
        # Message to send. Possible values are:
        # abort, ack, commit, just-woke, state-resp, state-req, ur-elected,
        # vote-no, vote-req, vote-yes
        self.message = 'state-req'
        # Total number of processes (not including master).
        self.N = num_procs
        # Send functionL
        self.send = send
        # All known information about current transaction.
        self.transaction = {'number': 0,
                            'song': None,
                            'state': 'committed',
                            'action': None,
                            'URL': None}
        self.num_messages_received_for_election = 0
        self.msgCounter = num_procs
        # Timeouts
        self.c_t_vote_req = c_t_vote_req
        self.c_t_prec = c_t_prec
        self.c_t_c = c_t_c
        self.p_t_vote = p_t_vote
        self.p_t_acks = p_t_acks
        # Alive list for re-election protocol
        self.election_alive_list = [pid]
        # Wait for a vote-req
        self.c_t_vote_req.restart()

    # Returns internal state as a dictionary.
    def simple_dict(self):
        return {'alive': self.alive,
                'coordinator': self.coordinator,
                'data': self.data,
                'flags': self.flags,
                'id': self.id,
                'message': self.message,
                'transaction': self.transaction}

    # Should be called immediately after constructor.
    def load_state(self):
        print('Process %d alive for the first time' % self.id)
        # Find out who is alive and who is the coordinator.
        self.alive = self.broadcast()
        # Self is the first process to be started.
        if len(self.alive) == 1:
            self.coordinator = self.id
            # Tell master this process is now coordinator.
            self.send([-1], 'coordinator %d' % self.id)
            self.msgCounter = self.msgCounter + 1

    def re_election_protocol(self):
        with self.lock:
            print('starting re-election!')
            # TODO : Fix this. I exit here
            exit()

            self.num_messages_received_for_election = 0
            self.message = 'lets-elect-coordinator'
            self.election_alive_list = self.broadcast()

            # If alive item not present in election_alive, remove from alive
            for p in self.alive:
                if p not in self.election_alive_list:
                    self.alive.remove(p)

    def after_timed_out_on_acks(self):
        with self.lock:
            self.message = 'commit'
            self.send(self.alive, self.message_str())
            self.msgCounter = self.msgCounter + 1
            self.log()

    # Broadcasts message corresponding to state and returns all live recipients.
    # The broadcast goes to all messages, including the sender.
    # NOT thread-safe.
    def broadcast(self):
        recipients = range(self.N)  # PID of all processes.
        message = self.message_str()  # Serialize self.
        self.msgCounter = self.msgCounter + 1
        return self.send(recipients, message)

    # Writes state to log.
    # NOT thread-safe.
    def log(self):
        pass

    # Returns a serialized version of self's state. Since in this assignment,
    # an objects state will easily be captured in at most 300B, smaller than
    # the standard size of a TCP block, there is no cost incurred by including
    # possibly unnecessary information in every message.
    # NOT thread safe.
    def message_str(self):
        to_send = self.simple_dict()
        result = json.dumps(to_send)
        return result

    # Called when self receives a message s from the master.
    # IS thread-safe.
    def receive_master(self, s):
        print('received from master: ' + str(s))
        with self.lock:
            parts = s.split()
            # Begin three-phase commit.
            if parts[0] in ['add', 'delete']:
                if self.coordinator == self.id:
                    self.transaction = {'number': self.transaction['number'] + 1,
                                        'song': parts[1],
                                        'state': 'uncertain',
                                        'action': parts[0],
                                        'URL': parts[2] if parts[0] == 'add' else None}
                    self.message = 'vote-req'
                    self.acks = {}
                    self.votes = {}
                    if 'crashVoteREQ' in self.flags:
                        # If this code executes, the coordinator will crash.
                        self.send(self.flags['crashVoteREQ'], self.message_str())
                        self.msgCounter = self.msgCounter + 1
                        del self.flags['crashVoteREQ']
                        self.log()
                        os._exit(1)
                    else:
                        # Alive = set of processes that received the vote request.
                        self.alive = self.broadcast()
                        print('self alive = ' + str(self.alive))
                        self.log()
                        # Wait for votes
                        self.p_t_vote.restart()
                else:
                    self.send([-1], 'ack abort')
                    self.msgCounter = self.msgCounter + 1
            elif parts[0] == 'crash':
                os._exit(1)
            elif parts[0] in ['crashAfterAck', 'crashAfterVote']:
                self.flags[parts[0]] = True
            elif parts[0] in ['crashPartialCommit',
                              'crashPartialPreCommit',
                              'crashVoteREQ']:
                # Flag maps to list of process id's.
                self.flags[parts[0]] = map(int, parts[1:])
            # If we have the song
            elif parts[0] == 'get':
                if parts[1] in self.data:
                    # Send song URL to master.
                    url = self.data[parts[1]]
                    self.send([-1], 'resp ' + url)
                else:
                    self.send([-1], 'resp NONE')
                self.msgCounter = self.msgCounter + 1
            elif parts[0] == 'vote' and parts[1] == 'NO':
                self.flags['vote NO'] = True
        print('end receive_master')

    # Called when self receives message from another backend server.
    # IS thread-safe.
    def receive(self, s):
        print('receive ' + s)
        with self.lock:
            m = json.loads(s)
            # Only pay attention to abort if in middle of transaction.
            if m['message'] == 'abort' and self.transaction['state'] not in ['aborted', 'committed']:
                self.transaction['state'] = 'abort'
                self.log()
                # No longer need to wait for precommit.
                self.c_t_prec.suspend()
                # Wait for next vote request.
                self.c_t_vote_req.restart()
            # Only pay attention to acks if you are the coordinator.
            if m['message'] == 'ack' and self.id == self.coordinator and self.transaction['state'] == 'precommitted':
                self.acks[m['id']] = True
                # All live processes have acked.
                if len(self.acks) == len(self.alive):
                    # Stop waiting for acks
                    self.p_t_acks.suspend()
                    self.message = 'commit'
                    self.log()
                    if 'crashPartialCommit' in self.flags:
                        self.send(self.flags['crashPartialCommit'], self.message_str())
                        del self.flags['crashPartialCommit']
                        os._exit(1)
                    else:
                        self.send(self.alive, self.message_str())
                    self.send([-1], 'ack commit')
                    self.msgCounter = self.msgCounter + 2
            # Even the coordinator only updates data on receipt of commit.
            # Should only receive this emssage if internal state is precommitted.
            if m['message'] == 'commit' and self.transaction['state'] == 'precommitted':
                # No longer wait for commit.
                self.c_t_c.suspend()
                if m['transaction']['action'] == 'add':
                    self.data[m['transaction']['song']] = m['transaction']['URL']
                else:
                    del self.data[m['transaction']['song']]
                self.log()
                # Wait for next vote request.
                self.c_t_vote_req.restart()
            if m['message'] == 'precommit':
                # No longer need to wait for precommit.
                self.c_t_prec.suspend()
                self.message = 'ack'
                self.transaction['state'] = 'precommitted'
                self.send([self.coordinator], self.message_str())
                self.msgCounter = self.msgCounter + 1
                self.log()
                if 'crashAfterAck' in self.flags:
                    del self.flags['crashAfterAck']
                    os._exit(1)
                # Wait for commit.
                self.c_t_c.restart()
            if m['message'] == 'state-req':
                self.message = 'state-resp'
                stuff = self.send([m['id']], self.message_str())
                self.msgCounter = self.msgCounter + 1
            if m['message'] == 'state-resp':
                # Self tried to learn state, didn't crash during a transaction.
                if self.transaction['state'] in ['committed', 'aborted']:
                    # Only update internal state if sender knows more than self.
                    if self.transaction['number'] <= m['transaction']['number'] and isinstance(m['coordinator'],
                                                                                               (int, long)):
                        self.coordinator = m['coordinator']
                        self.data = m['data']
                        self.transaction = m['transaction']
                # Self crashed during a transaction.
                else:
                    # TODO: recovery code here.
                    pass
            if m['message'] == 'ur-elected':
                # TODO: termination protocol
                pass
            # Assume we only receive this correctly.
            if m['message'] == 'vote-req':
                print('received vote-req')
                # Restart timeout counter
                self.c_t_vote_req.suspend()
                self.transaction = m['transaction']
                # self.alive = m['alive']
                if 'vote NO' in self.flags:
                    self.message = 'vote-no'
                    self.transaction['state'] = 'aborted'
                    # Clear your flag for the next transaction.
                    del self.flags['vote NO']
                    # Wait for next transaction.
                    self.c_t_vote_req.restart()
                else:
                    print('voting yes')
                    self.message = 'vote-yes'
                    # Wait for a precommit
                    self.c_t_prec.restart()
                self.log()
                self.send([m['id']], self.message_str())
                self.msgCounter = self.msgCounter + 1
                if 'crashAfterVote' in self.flags and self.id != self.coordinator:
                    del self.flags['crashAfterVote']
                    os._exit(1)
            # Only accept no votes if you're the coordinator.
            if m['message'] == 'vote-no' and self.id == self.coordinator:
                self.p_t_vote.suspend()
                self.message = 'abort'
                self.transaction['state'] = 'aborted'
                self.votes[m['id']] = False
                self.log()
                # Tell everyone we've aborted.
                self.send(self.alive, self.message_str())
                self.msgCounter = self.msgCounter + 1
                # Tell master you've aborted.
                self.send([-1], 'ack abort')
                self.msgCounter = self.msgCounter + 1
            # Only accept yes votes if you're the coordinator
            if m['message'] == 'vote-yes' and self.id == self.coordinator:
                self.votes[m['id']] = True
                # Everybody has voted yes!
                if len(self.alive) == len(self.votes) and all(self.votes.values()):
                    self.p_t_vote.suspend()
                    self.message = 'precommit'
                    self.transaction['state'] = 'precommitted'
                    if 'crashPartialPreCommit' in self.flags:
                        self.send(self.flags['crashPartialPreCommit'], self.message_str())
                        self.msgCounter = self.msgCounter + 1
                        del self.flags['crashPartialPreCommit']
                        self.log()
                        os._exit(1)
                    else:
                        self.send(self.alive, self.message_str())
                        self.msgCounter = self.msgCounter + 1
                        self.log()
                    # Start waiting for acks.
                    self.p_t_acks.restart()
            # Election protocol messages
            if m['message'] == 'lets-elect-coordinator':
                if self.stupid:
                    self.message = 'i-am-stupid-for-election'
                else:
                    self.message = 'take-my-alive-list-for-election'
                self.send([m['id']], self.message_str())
                self.msgCounter = self.msgCounter + 1
            # Here we wait
            if m['message'] == 'take-my-alive-list-for-election' or m['message'] == 'i-am-stupid-for-election':
                self.num_messages_received_for_election += 1
                l1 = self.alive
                l2 = m['alive']
                self.alive = [val for val in l1 if val in l2]
                if len(self.election_alive_list) == self.num_messages_received_for_election:
                    self.coordinator = min(self.alive)
                    print('pid = %d, coord = %d' % (self.id, self.coordinator))
                    if self.coordinator == self.id:
                        # tell master about new coordinator
                        print('decided new coordinator: ' + str(self.id))
                        self.send([-1], 'coordinator ' + self.id)
                        self.msgCounter = self.msgCounter + 1
        print("end receive")

#
# if __name__ == '__main__':
#     main()
