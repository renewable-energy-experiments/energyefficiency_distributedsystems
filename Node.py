class node:
    """
    Node class to model the events as nodes in a graph.
    Have parent and child pointers, a node id (unique integer) and a process id
    Has a timestamp (ts) attribute which can be set by the BFS
    """

    def __init__(self):
        self.id = None
        self.type_name = None
        self.parents = []
        self.childs = []
        self.ts = None
        self.proc_id = None

    def add_parent(self, parent):
        if not parent in self.parents:
            self.parents.append(parent)

    def add_child(self, child):
        if not child in self.childs:
            self.childs.append(child)

    def set_id(self, id):
        self.id = id

    def set_ts(self, ts):
        self.ts = ts

    def set_proc_id(self, proc_id):
        self.proc_id = proc_id

    def get_parents(self):
        return self.parents

    def get_childs(self):
        return self.childs

    def get_name(self):
        return None

    def get_id(self):
        return self.id

    def get_ts(self):
        return self.ts

    def get_proc_id(self):
        return self.proc_id

    def __str__(self):
        return self.get_name()