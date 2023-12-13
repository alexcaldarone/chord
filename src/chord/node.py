from typing import Any, Optional, List, NewType, Dict
import numpy as np
import asyncio

from helpers import is_between, is_between_reverse
from resources import ResourceStorage

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
        self.FT: list[int] = [{"id": None, "node": self}] + [{"id": -1, "node": self} for _ in range(1, self.k)] # nella ft ci vanno i nodi non gli id
        self.successor_list: list[int] = [{"id": self.id, "node": self} for _ in range(self.k)] # direct successor list
        self.resources: list[Any] = []
        self.predecessor: int = {"id": None, "node": None}

        self.__DIRECT_SUCC = self.k
    
    def __repr__(self) -> str:
        return f"Node(id={self.id}, next = {self.successor['id']}, pred={self.predecessor['id']})"
    
    @property
    def successor(self) -> Dict:
        return self.FT[0]
    
    @successor.setter
    def successor(self, value: Dict):
        self.FT[0] = value
    
    def is_in(self, resource_id: int) -> bool:
        for el in self.resources:
            if el == resource_id:
                return True
        return False
    
    def add_resource(self, value: Any):
        self.resources.append(value)
    
    def get_closest(self, resource_id: int) -> Dict:
        # find the closest inedx in the finger table to the resource index
        # cambia questo?
        for idx in range(self.k-1, -1, -1):
            if self.FT[idx]["id"] < resource_id:
                return self.FT[idx]

        return self.FT[0]
    
    def __init_empty_ft(self):
        # if node has a successor inherit its finger table,
        # otherwise it fills the finger table with its id
        id = self.successor["id"] if self.successor["id"] is not None else self.id
        node = self.successor["node"] if self.successor["id"] is not None else self

        self.FT = [{"id":id, "node":node} for _ in range(self.k)]
        self.successor = {"id":id, "node":node} if self.successor is None else self.successor # change successor only if we dont have one 
        # set predecessor?

        # initialize direct successor list with successor's id if we have
        # a successor, otherwise use node's id
        self.successor_list = [{"id":id, "node":node} for _ in range(self.__DIRECT_SUCC)]
    
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
            self.predecessor = {"id": None, "node": None}
            self.__init_empty_ft()
        else:
            self.predecessor = {"id": None, "node": None}
            self.successor = other.__find_successor(self.id)
            # as suggested by the paper, we inherit the FT 
            # from the neighbouring node
            self.__init_empty_ft()
        
    def find_successor(self, id:int):
        pass

    def __find_successor(self, id: int) -> Dict:
        # ask node to find id's successor
        print("find_successor", self)
        print(self.predecessor)
        if self.predecessor["id"] is None and self.predecessor["node"] is None:
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
        start = self
        print("id", id)
        print("n_first", n_first)
        # in questo caso la condizione del while non viene soddisfatta mai
        while not is_between(id,
                             n_first.id,
                             n_first.successor["id"],
                             k = 2**self.k,
                             include_upper=True,
                             include_lower=False):
            print("inside loop", n_first)
            _, n_first = n_first.__closest_preceding_finger(id)
            print("INSIDE FIND PREDECESSOR", n_first)
            # per evitare loop infinito
            if n_first == start: break
        return n_first

    def __closest_preceding_finger(self, id: int):
        # return closest finger preceding id
        print("closest_preceding_finger")
        print("id di cui trovare il finger", id)
        print("id del nodo che chiama la funzione", self.id)
        for i in range(self.k-1, -1, -1):
            print("FT[i]:", self.FT[i])
            if is_between(self.FT[i]["id"],
                          self.id,
                          id,
                          k = 2**self.k):
                return self.FT[i]["id"], self.FT[i]["node"]
        # in questa funzione potrei avere due output diversi (un intero o un Node)
        # quello che potrei fare è restituire anche qui sotto un id ma distringuere
        # i due return con un flag (True, False) che gestico sopra
        return (self.id, self)
    

    def stabilize(self, network):
        # periodically verify the node's immediate successor
        # and tell the other successor about it
        print("stabilize", self)
        print(self.predecessor)
        x_id, x_node = network.nodes[self.successor["id"]].predecessor.values() # prendo il predecessore del successore sull'anello
        print("x id and node", x_id, x_node)
        print("self.id", self.id)
        print("self.successor", self.successor)
        print(f"{x_id is not None and x_node is not None}")
        if (x_id is not None and x_node is not None):
            if is_between(x_id, self.id, self.successor["id"], k = self.k):
                self.successor = {"id": x_id, "node": x_node}

        network.nodes[self.successor["id"]].notify(self)

    def notify(self, other):
        # cambia la funzione is between cosi 
        # gestisce entrambi i casi
        print("notify")
        print("self", self, "other", other)
        print("self.predecessor", self.predecessor)
        if (self.predecessor["id"] is None) or is_between(other.id,
                                                    self.predecessor["id"],
                                                    self.id,
                                                    k = 2**self.k):
                print("inside if", self, self.predecessor)
                self.predecessor = {"id": other.id, "node": other}
        print("notify -- second if\t self.id:", self.id, "other.id:", other.id,
              "self.successor.id:", self.successor['id'])
        if is_between(self.id,
                      other.id,
                      self.successor["id"],
                      k = 2**self.k):
            other.successor = {"id": self.id, "node": self}
        
        # edge case: ho due nodi, uno vede l'altro come succ e pred mentre
        # l'altro lo vede solo come pred
        if (other.successor["id"] == self.id) and (self.predecessor["id"] == other.id) and \
            (other.predecessor["id"] == self.id) and (self.successor["id"] != other.id):
            self.successor = {"id": other.id, "node": other}
    
    def fix_fingers(self):
        # periodically refresh finger table entries
        i = np.random.randint(low = 0, high = self.k)
        print("i", i)
        print("starting id:", (self.id + 2**i) % 2**self.k)
        self.FT[i] = self.__find_successor((self.id + 2**i) % 2**self.k)