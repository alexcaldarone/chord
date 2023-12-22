from typing import Tuple
import math

from node import Node
from helpers import is_between

class DistributedHashTable:
    def __init__(self, 
                 k: int):
        self.counter: int = 0
        self.k: int = k
        self.nodes: list[Node] = [None for _ in range(0, 2**k)]
        self.start = math.inf
    
    @property
    def is_empty(self) -> bool:
        return self.counter == 0
    
    def __getitem__(self, 
                    idx:int) -> Node:
        if self.nodes[idx] == None:
            raise IndexError("Cannot index where there is no node")
        return self.nodes[idx]
    
    def linear_search_resource(self, 
                               node_id: int, 
                               resource_id: int, 
                               k: int = 0) -> Tuple[Node, int]:
        node = self[node_id]

        if self.counter >= 1:
            # there's more than one node in the network
            while not is_between(resource_id, node.predecessor["id"], node.id,
                                 k = 2**self.k, include_lower=True):
                # block cases where it goes into an infinite loop
                if k > 2**(self.k + 1): 
                    raise LookupError("Resource not found in DHT")
                node = node.successor["node"]
                k += 1
        
        if node.is_in(resource_id):
                return node, k
        else:
            raise LookupError("Resource not found in DHT")
    
    def search(self, 
               resource_id: int, 
               node_id: int, 
               k: int = 0) -> Tuple[Node, int]:
        # logarithmic research
        node = self[node_id]
        if node.is_in(resource_id):
            return node, k
        else:
            _, new_node, succ = node.get_closest(resource_id)
            if succ:
                if new_node.is_in(resource_id):
                    return new_node, k+1
                else:
                    raise LookupError("Resource not found in DHT")
            else:
                return self.search(resource_id, new_node.id, k+1)