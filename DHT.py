from typing import Any, List, Tuple, Optional
import math

class Node:
    """
    Implementation of a node belonging to a Distributed Hash Table
    """
    def __init__(self, id: int, k: int):
        assert id <= 2**k - 1
        self.id: int = int(id)
        self.k: int = k
        self.FT: list[int] = [-1 for _ in range(0, self.k)]
        self.resources: list[Any] = []
        self.next: int = None
    
    @property
    def get_id(self) -> int:
        return self.id
    
    @property
    def get_next(self) -> int:
        return self.next
    
    def __repr__(self):
        return f"Node(id={self.id}, next = {self.next})"

class DistributedHashTable:
    def __init__(self, k: int):
        self.counter: int = 0
        self.k: int = k
        self.nodes: list[Node] = [None for _ in range(0, 2**k - 1)]
        self.start = math.inf

        # posso inizializzare un campo start a infinito e poi 
        # ogni volta che aggiungo un nodo lo aggiorno, facendo il minimo
        # tra lo start che ho e l'indice in cui aggiungo il nuovo nodo

        # se non passassi *args qui e scrivessi un metodo per 
        # aggiungere nodi?
        # in questo metodo devo poi fare una ricerca sull'indice che è appena
        # maggiore di quello che sto aggiungendo e aggiorna il campo next del nodo

        # in self.nodes posso semplicemente passare il primo 
        # degli args e poi a catena aggiungo i successori.
        """#i need to eventually add the nodes in the position 
        for arg in args:
            if self.nodes[arg.id]:# checking that the cell is empty
                self.nodes[arg.id] = arg"""
    
    @property
    def is_empty(self) -> bool:
        return self.counter == 0
    
    # looking for next node: iterate on sublist following the newly added node and stop at the first node
    # looking for previous node: iterate (backwards) on sublist that comes before newly added node and stop at the first node

    def __iter__(self):
        # if counter is set to zero raise an error
        idx = self.start

        while self.nodes[idx].get_next != self.start:
            yield self.nodes[idx]
            idx = (self.nodes[idx].get_next) % (2 ** self.k - 1)
        # nel ciclo while rimase escluso l'ultimo, lo faccio qui
        yield self.nodes[idx]

    def add_node(self, node: Node):
        assert node.get_id <= 2 ** self.k - 1 # trasformare in exception

        if self.nodes[node.get_id] != None:
            return -1 # cannot add node here because position is occupied
        
        # devo trovare un modo per aggiornare il next del nodo precedente a quello che aggiungo
        self.nodes[node.get_id] = node
        self.start = min(self.start, node.get_id)
        self.counter += 1
        if self.counter > 1:
            self.__update_prev_node_next(node)
            node.next = self.__find_next(node)

    def search(self, input_id: int):
        for node in self:
            if node.get_id == input_id:
                return node.get_id
        
        return -1
    
    def __update_prev_node_next(self, node: Node):
        # caso in cui ho due nodi
        idx = (node.get_id - 1) % (2 ** self.k - 1)

        while idx != node.get_id and self.nodes[idx] == None:
            idx = (idx - 1) % (2 ** self.k - 1)
        
        self.nodes[idx].next = node.id


    def __find_next(self, node: Node):
        idx = (node.get_id + 1) % (2 ** self.k - 1)

        while idx != node.get_id and self.nodes[idx] == None:
            idx = (idx + 1) % (2 ** self.k - 1)
        
        return idx


if __name__ == "__main__":
    d = DistributedHashTable(3)
    d.add_node(Node(id = 0, k = 3))
    d.add_node(Node(id = 2, k = 3))
    d.add_node(Node(id = 5, k = 3))
    d.add_node(Node(id = 6, k = 3))
    print(d.nodes)
    print(d.k)

    for el in d:
        print(el)

    d.search(5)