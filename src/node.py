from typing import Any

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
    
    def __repr__(self) -> str:
        return f"Node(id={self.id}, next = {self.successor})"

    @property
    def get_id(self) -> int:
        return self.id
    
    @property
    def successor(self) -> int:
        return self.FT[0]
    
    @successor.setter
    def successor(self, value: int):
        self.FT[0] = value
    
    def is_in(self, resource: Any) -> bool:
        for el in self.resources:
            if el == resource:
                return True
        return False
    
    def add_resource(self, value: Any):
        self.resources.append(value)
    
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
