from typing import Any, Set, Optional

class Resource:    
    def __init__(self, 
                 id: int,
                 content: Any):
        self.id = id
        self.content = content
    
    def __repr__(self):
        return f"Resource(id={self.id})"
    
    def __hash__(self):
        return self.id

class ResourceStorage:
    def __init__(self,
                 resource_list: Optional[Set[Resource]] = None):
        self.storage: Set[Resource] = set(resource_list) if resource_list is not None else set()
    
    def __iter__(self):
        for el in self.storage:
            yield el
    
    def get_resource(self, 
                     idx: int) -> Resource:
        for resource in self:
            if resource.id == idx:
                return resource
        raise KeyError(f"Resource with id={idx} not present")
    
    def add_resource(self, 
                     resource: Resource):
        self.storage.add(resource)
    
    def delete_resource(self,
                        resource: Resource):
        to_delete = self.get_resource(resource.id)
        self.storage.remove(to_delete)