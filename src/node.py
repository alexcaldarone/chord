from typing import Any
import numpy as np

class Node:
    """
    Implementation of a node belonging to a Distributed Hash Table
    """
    def __init__(self, id: int, k: int) -> None:
        assert id <= 2**k - 1
        self.id: int = int(id)
        self.k: int = k
        self.FT: list[int] = [-1 for _ in range(0, self.k)] # nella ft ci vanno i nodi non gli id
        self.resources: list[Any] = []
        self.predecessor: int = None
    
    def __repr__(self) -> str:
        return f"Node(id={self.id}, next = {self.successor})"
    
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
        distances = [abs(node_id - resource_id) for node_id in self.FT]
        min_idx = np.argmin(distances)

        return self.FT[min_idx]

    def find_successor(self, id: int):
        # ask node to find id's successor
        n_first = self.find_predecessor(id)
        return n_first.successor
    
    def find_predecessor(self, id: int):
        # ask the node to find its predecessor
        n_first = self
        while not (id > n_first.id and id < n_first.successor):
            n_first = n_first.closest_preceding_finger(id)
        return n_first

    def closest_preceding_finger(self, id: int):
        # return closest finger preceding id
        for i in range(self.k, 0, -1):
            if self.FT[i] > self.id and self.FT[i] < id:
                return self.FT[i]
        return self

    def join(self, other, network):
        # function that sets this node as the predecessor of node other
        # on the dht network
        network.nodes[self.id] = self
        self.successor = other.find_successor(self.id)

    def stabilize(self, network):
        # periodically verify the node's immediate successor
        # and tell the other successor about it
        x = network.nodes[self.successor].predecessor # prendo il predecessore del successore sull'anello
        if x not in (self.id, self.successor):
            self.successor = x
        network.nodes[self.successor].notify(self.id)

    def notify(self, other, network):
        if self.predecessor == None or other.id in (self.predecessor, self.id):
            self.predecessor = other.id

    def fix_fingers(self):
        # periodically refresh finger table entries
        pass