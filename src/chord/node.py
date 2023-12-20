from typing import Any, Optional, List, NewType, Dict, Union, Tuple
import numpy as np

from helpers import is_between
from resources import ResourceStorage, Resource

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
        self.__DIRECT_SUCC = self.k

        self.FT: list[int] = [{"id": None, "node": self}] + [{"id": -1, "node": self} for _ in range(1, self.k)] # nella ft ci vanno i nodi non gli id
        self.successor_list: list[int] = [{"id": self.id, "node": self} for _ in range(self.__DIRECT_SUCC)] # direct successor list
        self.resources: ResourceStorage = ResourceStorage()
        self.predecessor: int = {"id": None, "node": None}

    def __repr__(self) -> str:
        return f"Node(id={self.id}, next = {self.successor['id']}, pred={self.predecessor['id']})"
    
    @property
    def successor(self) -> Dict:
        return self.FT[0]
    
    @successor.setter
    def successor(self, value: Dict):
        self.FT[0] = value
        self.successor_list[0] = value
    
    def is_in(self, resource_id: int) -> bool:
        try:
            res = self.resources.get_resource(resource_id)
        except KeyError:
            return False
        else:
            return True
    
    def add_resource(self, value: Union[Resource, Tuple[int, Any]]):
        value = value if isinstance(value, Resource) else Resource(value[0], value[1])

        if self.predecessor["id"] is None or \
            is_between(value.id, self.predecessor["id"], self.id, include_lower= True,
                       k = 2**self.k):
            self.resources.add_resource(value)
        else:
            raise Exception("Cannot resource with this id on this node")
    
    def get_closest(self, resource_id: int) -> Dict:
        # find the closest inedx in the finger table to the resource index
        # cambia questo?
        for idx in range(self.k-1, -1, -1):
            if is_between(resource_id,
                          self.FT[idx]["id"],
                          self.id,
                          k = 2**self.k,
                          include_lower=True):
                return self.FT[idx]["id"], self.FT[idx]["node"], False

        return self.FT[0]["id"], self.FT[0]["node"], True
    
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
        #print("self inside join:", self)
        if network.counter == 2**self.k:
            raise Exception("Cannot add node to a full DHT")
        
        network.nodes[self.id] = self
        network.counter += 1
        #print("inside join", network.nodes)
        if other == None:
            self.predecessor = {"id": None, "node": None}
            self.__init_empty_ft()
        else:
            if self.id == other.id:
                raise RuntimeError("Cannot have two nodes with same id in network")
            
            self.predecessor = {"id": None, "node": None}
            self.successor = other.__find_successor(self.id)
            # as suggested by the paper, we inherit the FT 
            # from the neighbouring node
            self.__init_empty_ft()
            # aggiungi funzione per controllare se si devono spostare risorse

    def __find_successor(self, id: int) -> Dict:
        # ask node to find id's successor
        #print("find_successor", self)
        #print(self.predecessor)
        
        n_first = self.__find_predecessor(id)
        return n_first.successor
    
    def __find_predecessor(self, id: int):
        # ask the node to find its predecessor

        # c'e' qualcosa che non va nella ricerca in
        # closest preceding finger.
        n_first = self
        #print("id", id)
        #print("n_first", n_first)
        # in questo caso la condizione del while non viene soddisfatta mai
        while not is_between(id,
                             n_first.id,
                             n_first.successor["id"],
                             k = 2**self.k,
                             include_upper=True,
                             include_lower=False):
            #print("inside loop", n_first)
            old_n_first = n_first
            _, n_first = n_first.__closest_preceding_finger(id)
            #print("INSIDE FIND PREDECESSOR", n_first)
            # per evitare loop infinito
            if n_first == old_n_first: break
        return n_first

    def __closest_preceding_finger(self, id: int):
        # return closest finger preceding id
        #print("closest_preceding_finger")
        #print("id di cui trovare il finger", id)
        #print("id del nodo che chiama la funzione", self.id)
        for i in range(self.k-1, -1, -1):
            #print("FT[i]:", self.FT[i])
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
        #print("stabilize", self)
        #print(self.predecessor)
        predessor_dict = network.nodes[self.successor["id"]].predecessor
        x_id, x_node = predessor_dict["id"], predessor_dict["node"] # prendo il predecessore del successore sull'anello
        #print("x id and node", x_id, x_node)
        #print("self.id", self.id)
        #print("self.successor", self.successor)
        #print(f"{x_id is not None and x_node is not None}")
        if (x_id is not None and x_node is not None):
            if is_between(x_id, self.id, self.successor["id"], k = self.k):
                self.successor = {"id": x_id, "node": x_node}

        network.nodes[self.successor["id"]].notify(self)

    def notify(self, other):
        # cambia la funzione is between cosi 
        # gestisce entrambi i casi
        #print("notify")
        #print("self", self, "other", other)
        #print("self.predecessor", self.predecessor)
        if (self.predecessor["id"] is None) or \
            (is_between(other.id,self.predecessor["id"],self.id,k = 2**self.k) and \
             other is not None):
                #print("inside if", self, self.predecessor)
                self.predecessor = {"id": other.id, "node": other}
        #print("notify -- second if\t self.id:", self.id, "other.id:", other.id,
        #      "self.successor.id:", self.successor['id'])
        if (self.successor["id"] is not None) and is_between(self.id,
                                                             other.id,
                                                             self.successor["id"],
                                                             k = 2**self.k):
            other.successor = {"id": self.id, "node": self}
        
        # edge case: ho due nodi, uno vede l'altro come succ e pred mentre
        # l'altro lo vede solo come pred
        if (other.successor["id"] == self.id) and (self.predecessor["id"] == other.id) and \
            (other.predecessor["id"] == self.id) and (self.successor["id"] != other.id):
            self.successor = {"id": other.id, "node": other}
    
    def exit(self, network):
        # se nodo esce dalla rete deve:
        # 1. controllare che sia il successore del suo predecessore e impostare
        # il successore del suo predecessore come il successore del nodo che esce
        # sono l'unico nodo presente
        if self.successor["id"] is None:
            network.nodes[self.id] = None
        
        if self.predecessor["node"].successor["node"] == self and \
            self.predecessor["id"] is not None:
            self.predecessor["node"].successor = self.successor
        if self.successor["node"].predecessor["node"] == self and \
            self.predecessor["id"] is not None:
            self.successor["node"].predecessor = self.predecessor
        
        self.move_resources(self.successor["node"], exit = True)
        network.nodes[self.id] = None

    def fix_fingers(self):
        # periodically refresh finger table entries
        i = np.random.randint(low = 0, high = self.k)
        #print("i", i)
        #print("starting id:", (self.id + 2**i) % 2**self.k)
        self.FT[i] = self.__find_successor((self.id + 2**i) % 2**self.k)
    
    def fix_successor_list(self):
        i = np.random.randint(low = 1, high = self.__DIRECT_SUCC)
        x = self.successor["node"]
        for _ in range(i):
            x = x.successor["node"]
        self.successor_list[i] = {"id": x.id, "node": x}
    
    def move_resources(self, other, exit: bool):
        if exit:
            # we are leaving the network, move our resources to our successor
            # assumes we have already fixed successor and predecessor pointers
            for res in self.resources: other.add_resource(res)
        else:
            # we are joining the network
            # ask other to give us the resources we are now in charge of
            for res in other.resources:
                try:
                    # controlla se risorsa mi spetta, se si la aggiunge
                    self.add_resource(res) # will raise exception if we are not in charge of resource
                    other.delete_resource(res) # la elimino da other
                except:
                    # se risorsa non mi spetta, la lascio in other e passo alla prossima
                    continue