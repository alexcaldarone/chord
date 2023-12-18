from typing import Any, List, Tuple, Optional, NewType, Union
import math

from chord.node import Node
from chord.resources import Resource
from chord.helpers import is_between

class DistributedHashTable:
    def __init__(self, k: int):
        self.counter: int = 0
        self.k: int = k
        self.nodes: list[Node] = [None for _ in range(0, 2**k)]
        self.start = math.inf
    
    @property
    def is_empty(self) -> bool:
        return self.counter == 0
    
    # looking for next node: iterate on sublist following the newly added node and stop at the first node
    # looking for previous node: iterate (backwards) on sublist that comes before newly added node and stop at the first node

    # se io non aggiungo i nodi linearmente è inutile
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
    
    def add_resource(self, resource: Union[Resource, Tuple[int, Any]]):
        pass

    def linear_search_resource(self, node_id: int, resource_id: int, k = 0):
        node = self[node_id]

        if node.predecessor["id"] is not None:
            # there's more than one node in the network
            while not is_between(resource_id, node.predecessor["id"], node.id,
                                include_lower=True):
                node = node.successor["node"]
        
        if node.is_in(resource_id):
                return node, k
        else:
            raise LookupError("Resource not found in DHT")
    
    def search(self, resource_id: int, node_id: int, k = 0):
        #node_id = self.start if node_id is None else node_id
        # logarithmic research
        node = self[node_id]
        #print(node.FT)
        #print("res:", resource_id, "node:", node)
        #print("node FT", node.FT)
        if node.is_in(resource_id):
            return node, k
        else:
            new_node_idx, new_node, succ = node.get_closest(resource_id)
            #print("new_node_idx", new_node_idx)
            #print(new_node)
            if succ:
                if new_node.is_in(resource_id):
                    return new_node, k+1
                else:
                    raise LookupError("Resource not found in DHT")
            else:
                return self.search(resource_id, new_node.id, k+1)
    
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

        while self.nodes[idx] == None:
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
    print("------ node_1 joins network -----")
    node_1.join(d2)
    print(d2.nodes)
    print(node_1.successor)
    print(node_1.FT)
    print("---- creation of test_node -----")
    test_node = Node(id = 0, k = 3)
    print(test_node.FT)
    print("---- test_node joins network ----")
    test_node.join(d2, d2[3])
    print(test_node.FT)
    print("test_node successor:", test_node.successor)
    print(d2.nodes)
    print("--- adding resource to test_node ---")
    node_1.add_resource((0, None))
    print("risorse di test_node:", test_node.resources)
    print("predecessor of node_1:", node_1.predecessor)
    print("-- test_node calls stabilization ---")
    test_node.stabilize(network = d2)
    print(d2.nodes)
    test_node.notify(node_1)
    node_1.notify(test_node)
    #node_1.stabilize(d2)
    print(d2.nodes)
    print("--- node_1 chiama fix_fingers ---")
    for _ in range(10): node_1.fix_fingers()
    print(node_1.FT)
    for _ in range(10): test_node.fix_fingers()
    print(test_node.FT)
    print("--- test_node_2 joins the network ---")
    test_node_2 = Node(id = 2, k = 3)
    test_node_2.join(d2, node_1)
    print(d2.nodes)
    test_node_2.stabilize(network=d2)
    print(d2.nodes)
    test_node_2.notify(test_node)
    print(d2.nodes)
    test_node.notify(node_1)
    test_node.stabilize(d2)
    print(d2.nodes)
    print("node_1 FT:", node_1.FT)
    print("before fix_fingers:", test_node.FT)
    print(test_node.FT)
    node_1.stabilize(d2)
    print(d2.nodes)
    test_node.notify(node_1)
    print(node_1.FT)
    print(d2.nodes)
    print("---- test_node_2 fix_fingers----")
    print(test_node_2.FT)
    for _ in range(10): test_node_2.fix_fingers()
    print(test_node_2.FT)
    print(d2.search(resource_id=0, node_id = 2))
    #print(f"La risorsa 0 si trova su {res}, che è stato raggiunto in {jumps} salti")
    # in questa dht vorrei che 0 avesse come pred e succ 3
    # mentre 3 deve avere come succ e pred 0
    # però non avviene, perche?

    # il nodo 2 non è agganciato a nessun altro

    # ipotesi: se un nodo è il primo ad esskere aggiunto 
    # al network la sua DHT contiene solo il suo ids


    # current problem: i need to make sure that node 3 finds node
    # 0 as his successor. 
    # possible solution: make the is_between algebra modular?