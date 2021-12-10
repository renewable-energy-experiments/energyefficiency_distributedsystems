"""
Lamport Sync
Adapted for counting messages  from whong92/timestampSim.py https://gist.github.com/whong92/d0df440fbad880bc0221488eeedbb89f
"""
import csv
import sys
from random import randrange

from Node import node

filename = 'network_traffc_counter.csv'
fieldnames = ['algorithm', 'network_message']


class PI(node):
    """
    Process Instruction event
    """

    def __init__(self):
        super(PI, self).__init__()
        self.type_name = 'PI'

    def get_name(self):
        #  prints event type _ process Id _ selfId
        return self.type_name + '_' + str(self.proc_id) + '_' + str(self.id)  # for debugging


class MR(node):
    """
    Message receive event
    """

    def __init__(self, mid):
        super(MR, self).__init__()
        self.mid = mid
        self.type_name = 'MR'

    def get_name(self):
        return self.type_name + '_' + str(self.mid)


class MS(node):
    """
    Message send event
    """

    def __init__(self, mid):
        super(MS, self).__init__()
        self.mid = mid
        self.type_name = 'MS'

    def get_name(self):
        return self.type_name + '_' + str(self.mid)


class distProc:
    """
    distributed process with multiple concurrent processes communicating with each other via message passing.
    Has a 'clock' for lamport and a procs argument for process list of lists.
    Each list is a list of events which can be one of PI, MR or MS.
    """

    def __init__(self, clock='lamport', procs=None):
        self.clock = clock
        self.procs = procs
        self.stamped = {}
        self.msgCounter = 0
        id = 0
        for ip, p in enumerate(self.procs):
            for ie, e in enumerate(p):
                e.set_proc_id(ip)
                e.set_id(id)
                self.stamped[id] = False
                id += 1

                if ie == 0:
                    continue
                f = p[ie - 1]
                e.add_parent(f)
                f.add_child(e)

        self.is_solved = False

    def solve(self):
        """
        Calculate the timestamps
        """
        if self.is_solved:
            return

        for proc in self.procs:
            if (proc[0].get_ts() is None) and (len(proc[0].get_parents()) == 0):
                BFS_lamport(proc[0], self.stamped)

        self.is_solved = True

    def __str__(self):
        """
        Event with its calculated timestamp.
        """
        the_str = ''
        for proc in self.procs:
            for e in proc:
                the_str += e.__str__() + '(' + e.get_ts().__str__() + ') '
            the_str += '\n'
        return the_str


def BFS_lamport(S, stamped):
    """
    BFS to calculate a topological sort of dependent events starting with an event S with no parents,
    and calculate timestamps, using lamport timestamps
    """
    # first event S with no parents has lamport timestamp = 1
    stamped[S.get_id()] = True
    S.set_ts(1)
    q = [S]

    # Use BFS to compute topological sort of dependent events and place in a queue only when all parents have
    # timestamps, then calculate timestamps based on parents
    while len(q) > 0:

        U = q.pop(0)

        for V in U.get_childs():

            if stamped[V.get_id()]:
                raise Exception('node was stamped before being reached by all parents!')

            parent_ts = []
            is_det = True

            for W in V.get_parents():
                if W.get_ts() is None:
                    is_det = False
                    break
                parent_ts.append(W.get_ts())

            if is_det:
                stamped[V.get_id()] = True
                V.set_ts(max(parent_ts) + 1)
                q.append(V)


def make_messages(N, msgCounter=0):
    """
    create and link N send (MSs) and message receive (MRs) events
    """
    MRs = {}
    MSs = {}

    for i in range(N):
        MRs[i] = MR(i)
        MSs[i] = MS(i)
        MRs[i].add_parent(MSs[i])
        MSs[i].add_child(MRs[i])
        msgCounter = msgCounter + 1
    return MRs, MSs, msgCounter


def lamposrtSync(n):
    """
    calculating the lamport timestamps for a distributed process consisting of n concurrent processes,
    and 2n messages in total
    """
    # Make messages to calculate timestamps and network traffic
    msgCounter = 0
    MRs, MSs, msgCounter = make_messages(n + 1, msgCounter)
    lst = []
    plst = []
    for i in range(0, n):
        m = randrange(1, n)  # int(input())
        print("messages for this process", m)
        msgCounter = msgCounter + m
        plst = [PI()]
        for j in range(0, m):
            plst.append(MSs[j])
            plst.append(MRs[j])
        lst.append(plst)

    #  testcase for 4 nodes
    # lst=[
    #     [PI(), MSs[0], PI(), MRs[1], MSs[2]],
    #     [MRs[3], MRs[0], MSs[1]],
    #     [MSs[3], PI(), MRs[2]]
    # ]
    print("Sync up processes")
    dp_lamport = distProc(clock='lamport', procs=lst)
    try:
        dp_lamport.solve()
        print('Lamport timestamped: ')
        print(dp_lamport)
        print("lamposrtSync msgCounter ", msgCounter)
        with open(filename, 'a') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow({'lamport', msgCounter})
            # writer.writerow({fieldnames[0]: 'lamport', fieldnames[1]: msgCounter})
            csvfile.close()
    except Exception as err:
        print("exception {}".format(err))
        # print("Encountered unexpected error:", sys.exc_info()[0])
        # exit(1)
