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
        self.next: int = None
    
    def __repr__(self) -> str:
        return f"Node(id={self.id}, next = {self.next})"

    @property
    def get_id(self) -> int:
        return self.id
    
    @property
    def get_next(self) -> int:
        return self.next
    
    def is_in(self, resource) -> bool:
        for el in self.resources:
            if el == resource:
                return True
        return False