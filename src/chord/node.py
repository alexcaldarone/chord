from typing import Any, Optional, List, NewType
import numpy as np
import asyncio

from chord.helpers import is_between, is_between_reverse
from chord.resources import ResourceStorage

class Node:
    """
    Implementation of a node belonging to a Distributed Hash Table
    """
    def __init__(self, id: int, k: int) -> None:
        # do i want to define the possibilty of defining a new node 
        # with a given resource storage 
        assert id <= 2**k - 1
        self.id: int = int(id)
        self.k: int = k
        self.FT: list[int] = [None] + [-1 for _ in range(1, self.k)] # nella ft ci vanno i nodi non gli id
        self.successor_list: list[int] = [self.id for _ in range(self.k)] # direct successor list
        self.resources: list[Any] = []
        self.predecessor: int = None

        self.__DIRECT_SUCC = self.k
    
    def __repr__(self) -> str:
        return f"Node(id={self.id}, next = {self.successor}, pred={self.predecessor})"
    
    @property
    def successor(self) -> int:
        return self.FT[0]
    
    @successor.setter
    def successor(self, value: int):
        self.FT[0] = value
    
    def is_in(self, resource_id: int) -> bool:
        for el in self.resources:
            if el == resource_id:
                return True
        return False
    
    def add_resource(self, value: Any):
        self.resources.append(value)
    
    def get_closest(self, resource_id: int) -> int:
        # find the closest inedx in the finger table to the resource index
        # cambia questo?
        for idx in range(self.k-1, -1, -1):
            if self.FT[idx] < resource_id:
                return self.FT[idx]

        return self.FT[0]
    
    def __init_empty_ft(self):
        # if node has a successor inherit its finger table,
        # otherwise it fills the finger table with its id
        id = self.successor if self.successor is not None else self.id

        self.FT = [id for _ in range(self.k)]
        self.successor = id if self.successor is None else self.successor # change successor only if we dont have one 
        # set predecessor?

        # initialize direct successor list with successor's id if we have
        # a successor, otherwise use node's id
        self.successor_list = [id for _ in range(self.__DIRECT_SUCC)]
    
    # how do i manage adding node to empty dht?
    def join(self, network, other = None):
        # posso anche non passare l'altro nodo
        # function that sets this node as the predecessor of node other
        # on the dht network

        # here other = None indicates we are adding the first node of
        # the network
        print("self inside join:", self)
        network.nodes[self.id] = self
        print("inside join", network.nodes)
        if other == None:
            self.predecessor = None
            self.__init_empty_ft()
        else:
            self.predecessor = None
            self.successor = other.__find_successor(self.id)
            # as suggested by the paper, we inherit the FT 
            # from the neighbouring node
            self.__init_empty_ft()
        
    def find_successor(self, id:int):
        pass

    def __find_successor(self, id: int):
        # ask node to find id's successor
        print("find_successor", self)
        print(self.predecessor)
        if self.predecessor is None:
            # if predecessor and successor are the same there's only
            # one node in the dht
            return self.successor
        
        n_first = self.__find_predecessor(id)
        return n_first.successor
    
    def __find_predecessor(self, id: int):
        # ask the node to find its predecessor

        # c'e' qualcosa che non va nella ricerca in
        # closest preceding finger.
        n_first = self
        print("id", id)
        print("n_first", n_first)
        # in questo caso la condizione del while non viene soddisfatta mai
        while not is_between(id,
                             n_first.id,
                             n_first.successor,
                             include_upper=True,
                             include_lower=False):
            print("inside loop", n_first)
            n_first = n_first.__closest_preceding_finger(id)
            # se dopo una iterazione il nodo è ancora self 
            # allora esco
            if n_first == self: break # is this correct?
        return n_first

    def __closest_preceding_finger(self, id: int):
        # return closest finger preceding id
        print("closest_preceding_finger")
        print("id di cui trovare il finger", id)
        print("id del nodo che chiama la funzione", self.id)
        for i in range(self.k-1, -1, -1):
            print(self.FT[i])
            if is_between(self.FT[i],
                          self.id,
                          id):
                return self.FT[i]
        # in questa funzione potrei avere due output diversi (un intero o un Node)
        # quello che potrei fare è restituire anche qui sotto un id ma distringuere
        # i due return con un flag (True, False) che gestico sopra
        return self
    
    def __find_successor_from_list(self, id:int):
        # find the id's successor using the direct successor list

        # mi fermo al più piccolo indice maggiore di id
        for succ in self.successor_list:
            if succ >= id:
                return True, succ
        return False, id

    def stabilize(self, network):
        # periodically verify the node's immediate successor
        # and tell the other successor about it
        print("stabilize", self)
        x = network.nodes[self.successor].predecessor # prendo il predecessore del successore sull'anello
        print("x", x)
        print("self.id", self.id)
        print("self.successor", self.successor)
        if (x is not None) and is_between(x,
                                          self.id,
                                          self.successor):
            self.successor = x

        network.nodes[self.successor].notify(self)

    def notify(self, other):
        print("notify")
        print("self", self, "other", other)
        print("self.predecessor", self.predecessor)
        if (self.predecessor is None) or is_between(other.id,
                                                    self.predecessor,
                                                    self.id):
                print("inside if", self, self.predecessor)
                self.predecessor = other.id
        
        try:
            if is_between_reverse(self.id,
                                  other.id,
                                  self.successor,
                                  self.k):
                other.successor = self.id
        except AssertionError:
            pass

    def fix_fingers(self):
        # periodically refresh finger table entries
        i = np.random.randint(low = 0, high = self.k)
        print("i", i)
        print("starting id:", (self.id + 2**i) % 2**self.k)
        self.FT[i] = self.__find_successor((self.id + 2**i) % 2**self.k)