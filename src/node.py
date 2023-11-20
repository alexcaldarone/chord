from typing import Any 

class Node:
    """
    Implementation of a node belonging to a Distributed Hash Table
    """
    def __init__(self, id: int, k: int) -> None:
        assert id <= 2**k - 1
        self.id: int = int(id)
        self.k: int = k
        self.FT: list[int] = [-1 for _ in range(0, self.k)]
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
    def successor(self, value):
        self.FT[0] = value
    
    def is_in(self, resource) -> bool:
        for el in self.resources:
            if el == resource:
                return True
        return False
    
    def add_resource(self, value: Any):
        self.resources.append(value)