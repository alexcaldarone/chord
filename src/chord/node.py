from typing import Any, Dict, Union, Tuple, TypeVar
import numpy as np

from chord.helpers import is_between
from chord.resources import ResourceStorage, Resource

DistributedHashTable = TypeVar("DistributedHashTable")

class Node:
    """
    Implementation of a node belonging to a Distributed Hash Table
    """
    def __init__(self, 
                 id: int, 
                 k: int):
        assert id <= 2**k - 1
        self.id: int = int(id)
        self.k: int = k
        self.__DIRECT_SUCC: int = self.k

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
    
    def is_in(self, 
              resource_id: int) -> bool:
        try:
            res = self.resources.get_resource(resource_id)
        except KeyError:
            return False
        else:
            return True
    
    def add_resource(self, 
                     value: Union[Resource, Tuple[int, Any]]):
        value = value if isinstance(value, Resource) else Resource(value[0], value[1])

        if self.predecessor["id"] is None or \
            is_between(value.id, self.predecessor["id"], self.id, include_lower= True,
                       k = 2**self.k):
            self.resources.add_resource(value)
        else:
            raise Exception("Cannot add resource with this id on this node")
    
    def get_closest(self, 
                    resource_id: int) -> Dict:
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

        # initialize direct successor list with successor's id if we have
        # a successor, otherwise use node's id
        self.successor_list = [{"id":id, "node":node} for _ in range(self.__DIRECT_SUCC)]
    
    def join(self, 
             network: DistributedHashTable, 
             other = None):
        if network.counter == 2**self.k:
            raise Exception("Cannot add node to a full DHT")
        
        network.nodes[self.id] = self
        network.counter += 1
        # we are first node in network
        if other == None:
            self.predecessor = {"id": None, "node": None}
            self.__init_empty_ft()
        else: # we use bootstrap node
            if self.id == other.id:
                raise RuntimeError("Cannot have two nodes with same id in network")
            
            old_other = other
            #print("before finding successor:", old_other)
            self.successor = other.__find_successor(self.id)
            #print("right after finding successor", other)
            #print("successore trovato da nodo bootsrap:", self.successor)
            # questo pezzo di codice dentro l'if è quello che sta causano problemi al nodo 
            # bootstrap
            if self.successor["node"].predecessor["id"] is not None:
                # il mio predecessore è il predecessore del mio successore
                self.predecessor = self.successor["node"].predecessor
                # divento il successore del predecessore del mio successore
                self.successor["node"].predecessor = {"id": self.id, "node": self} # pred del mio succ sono io 
                self.predecessor["node"].successor = {"id": self.id, "node": self} # succ del mio pred sono io"""
            
            #if self.successor == other:
            #    print(f"successore del nodo {self} è diventato other {other}")
            # as suggested by the paper, we inherit the FT from the neighbouring node
            self.__init_empty_ft()
            # aggiungi funzione per controllare se si devono spostare risorse
            #print("other after the node joins:", other)
            #print("what is in network:", network.nodes[other.id])

    def __find_successor(self, 
                         id: int) -> Dict:
        n_first = self.__find_predecessor(id)
        #print("dentro __find_successor, ispeziono other:", n_first)
        #print("dentro __find_successor, n_first.successor:", n_first.successor)
        return n_first.successor
    
    def __find_predecessor(self, 
                           id: int):
        # ask the node to find its predecessor
        n_first = self
        while not is_between(id,
                             n_first.id,
                             n_first.successor["id"],
                             k = 2**self.k,
                             include_upper=True,
                             include_lower=False):
            old_n_first = n_first
            _, n_first = n_first.__closest_preceding_finger(id)
            # condition to avoid infinite loop
            if n_first == old_n_first: break
        return n_first
    
    def __closest_preceding_finger(self, 
                                   id: int):
        for i in range(self.k-1, -1, -1):
            if is_between(self.FT[i]["id"],
                          self.id,
                          id,
                          k = 2**self.k):
                return self.FT[i]["id"], self.FT[i]["node"]
        return (self.id, self)

    def stabilize(self, 
                  network: DistributedHashTable):
        # periodically verify the node's immediate successor
        # and tell the other successor about it
        if self.successor["id"] is None or \
            network.nodes[self.successor["id"]] is None: # check to avoid error during simulation
            return
        else:
            # prendo il predecessore del successore sull'anello
            predessor_dict = network.nodes[self.successor["id"]].predecessor
            x_id, x_node = predessor_dict["id"], predessor_dict["node"]

            if (x_id is not None and x_node is not None):
                if is_between(x_id, self.id, self.successor["id"], k = self.k):
                    self.successor = {"id": x_id, "node": x_node}
            if network.nodes[self.successor["id"]] is not None: # predecessore del mio successore è sbagliato
                network.nodes[self.successor["id"]].notify(self)

    def notify(self, 
               other):
        if other is None:
            return
        if (self.predecessor["id"] is None) or \
            (is_between(other.id, self.predecessor["id"], self.id, k = 2**self.k) and \
             other is not None):
                self.predecessor = {"id": other.id, "node": other}
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
    
    def exit(self, 
             network: DistributedHashTable):
        # se è unico nodo nella rete
        if network.counter > 1:
            if self.predecessor["id"] is not None:
                    print("self.successor:", self.successor)
                    print("----> in exit: predecessor pointer is correct")
                    # primo indice dove ho un elemento diverso da me
                    for i in range(len(self.successor_list)):
                        if self.successor_list[i]["id"] != self.id:
                            idx = i
                            break
                    self.predecessor["node"].successor = self.successor_list[idx]
                    self.predecessor["node"].stabilize(network)
            if self.successor["id"] is not None:
                print("----> in exit: successor pointer is correct")
                self.successor["node"].predecessor = self.predecessor
                self.move_resources(self.successor["node"], exit = True)
                self.successor["node"].stabilize(network)
        network.counter -= 1
        network.nodes[self.id] = None
        # stabilizzo i nodi dopo l'exit
        #if self.predecessor["id"] is not None: self.predecessor["node"].stabilize(network)
        #if self.successor["id"] is not None: self.successor["node"].stabilize(network)

    def fix_fingers(self):
        # periodically refresh finger table entries
        i = np.random.randint(low = 1, high = self.k)
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
            for res in self.resources: 
                try:
                    other.add_resource(res)
                except:
                    continue # in this case the resource goes lost
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