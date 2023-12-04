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
    
    def __getitem__(self, idx):
        if self.nodes[idx] == None:
            raise IndexError("Cannot index where there is no node")
        return self.nodes[idx]

    # delete all this code? How do i do new insert? On node class or dht table?
    def add_node(self, node: Node):
        assert node.id <= 2 ** self.k - 1 # trasformare in exception

        if self.nodes[node.id] != None:
            raise Exception(f"Cannot add node with id={node.id} becasue there already is a node with this id.")
        
        self.nodes[node.id] = node
        self.start = min(self.start, node.id)
        self.counter += 1
        if self.counter >= 1:
            self.__update_prev_node_next(node) # move this method to the node class?
            # make sure that also the first node has a predecessor set to the
            # last node of the ring
            node.successor = self.succ(node.id) # move this method to the node class?

    def linear_search_resource(self, resource_id: int):
        for node in self:
            if node.id >= resource_id:
                if node.is_in(resource_id):
                    return node.id
                return -1
        return -1
    
    def search(self, resource_id: int, node_id: int = None):
        node_id = self.start if node_id is None else node_id
        # logarithmic research
        node = self[node_id]
        
        if node.is_in(resource_id):
            return node_id
        else:
            new_node_idx = node.get_closest(resource_id)
            new_node = self.nodes[new_node_idx]
            if node.successor == new_node.id:
                if new_node.is_in(resource_id):
                    return new_node
                else:
                    raise LookupError("Resource not found in DHT")
            else:
                return self.search(resource_id, new_node.id)
    
    def __update_prev_node_next(self, node: Node):
        # caso in cui ho due nodi
        idx = (node.id - 1) % (2 ** self.k - 1)

        while idx != node.id and self.nodes[idx] == None:
            idx = (idx - 1) % (2 ** self.k - 1)
        
        self.nodes[idx].successor = node.id
        node.predecessor = self.nodes[idx].id


    def succ(self, start_id: int):
        # with empty table node is his own successor
        if self.counter == 1:
            return start_id
        
        idx = (start_id + 1) % (2 ** self.k - 1)

        while idx != start_id and self.nodes[idx] == None:
            idx = (idx + 1) % (2 ** self.k - 1)
        
        return idx


if __name__ == "__main__":
    """d = DistributedHashTable(3)
    d.add_node(Node(id = 0, k = 3))
    d.add_node(Node(id = 2, k = 3))
    d.add_node(Node(id = 4, k = 3))
    node1 = Node(id = 6, k = 3)
    node1.add_resource(5)
    d.add_node(node1)
    print(d.nodes)
    print(d.k)

    for el in d:
        for idx in range(1, len(el.FT)):
            el.FT[idx] = d.succ(2 ** (idx - 1))
    
    for el in d:
        print(el, el.predecessor)

    
    print(f"Resource {5} is in node: {d.linear_search_resource(5)}")
    print(f"Resource {5} is in node: {d.search(5)}")"""

    # network 2
    d2 = DistributedHashTable(3)
    node_1 = Node(id = 3, k = 3)
    print(node_1.FT)
    print(node_1.successor)
    node_1.join(d2)
    print(d2.nodes)
    print(node_1.successor)
    print(node_1.FT)
    test_node = Node(id = 0, k = 3)
    print(test_node.FT)
    test_node.join(d2, d2[3])
    print(test_node.FT)
    print(test_node.successor)
    print(d2.nodes)
    print(node_1.predecessor)
    test_node.stabilize(network = d2)
    print(d2.nodes)
    print(d2.nodes)
    node_1.fix_fingers()
    print(node_1.FT)
    #test_node.fix_fingers()
    #test_node_2 = Node(id = 2, k = 3)
    #test_node_2.join(node_1, d2)
    #print(d2.nodes)
    #node_1.stabilize(d2)
    #print(d2.nodes)

    # in questa dht vorrei che 0 avesse come pred e succ 3
    # mentre 3 deve avere come succ e pred 0
    # però non avviene, perche?

    # il nodo 2 non è agganciato a nessun altro

    # ipotesi: se un nodo è il primo ad esskere aggiunto 
    # al network la sua DHT contiene solo il suo ids