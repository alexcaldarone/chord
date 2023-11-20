from typing import Any, List, Tuple, Optional, NewType
import math

from node import Node

class DistributedHashTable:
    def __init__(self, k: int):
        self.counter: int = 0
        self.k: int = k
        self.nodes: list[Node] = [None for _ in range(0, 2**k - 1)]
        self.start = math.inf
    
    @property
    def is_empty(self) -> bool:
        return self.counter == 0
    
    # looking for next node: iterate on sublist following the newly added node and stop at the first node
    # looking for previous node: iterate (backwards) on sublist that comes before newly added node and stop at the first node

    def __iter__(self):
        if self.is_empty:
            raise Exception("Cannot iterate over empty table.")
        
        idx = int(self.start)

        while self.nodes[idx].successor != self.start:
            yield self.nodes[idx]
            idx = (self.nodes[idx].successor) % (2 ** self.k - 1)
        # nel ciclo while rimase escluso l'ultimo, lo faccio qui
        yield self.nodes[idx]

    def add_node(self, node: Node):
        assert node.get_id <= 2 ** self.k - 1 # trasformare in exception

        if self.nodes[node.get_id] != None:
            raise Exception(f"Cannot add node with id={node.get_id} becasue there already is a node with this id.")
        
        self.nodes[node.get_id] = node
        self.start = min(self.start, node.get_id)
        self.counter += 1
        if self.counter > 1:
            self.__update_prev_node_next(node) # move this method to the node class?
            node.successor = self.__find_next(node) # move this method to the node class?

    def linear_search_resource(self, resource_id: int):
        for node in self:
            if node.get_id >= resource_id:
                if node.is_in(resource_id):
                    return node.get_id
                return -1
        return -1
    
    def __update_prev_node_next(self, node: Node):
        # caso in cui ho due nodi
        idx = (node.get_id - 1) % (2 ** self.k - 1)

        while idx != node.get_id and self.nodes[idx] == None:
            idx = (idx - 1) % (2 ** self.k - 1)
        
        self.nodes[idx].successor = node.id


    def __find_next(self, node: Node):
        idx = (node.get_id + 1) % (2 ** self.k - 1)

        while idx != node.get_id and self.nodes[idx] == None:
            idx = (idx + 1) % (2 ** self.k - 1)
        
        return idx


if __name__ == "__main__":
    d = DistributedHashTable(3)
    d.add_node(Node(id = 0, k = 3))
    d.add_node(Node(id = 2, k = 3))
    d.add_node(Node(id = 4, k = 3))
    node1 = Node(id = 6, k = 3)
    node1.add_resource(5)
    d.add_node(node1)
    print(d.nodes)
    print(d.k)

    for el in d:
        print(el)

    print(f"Resource {5} is in node: {d.linear_search_resource(5)}")