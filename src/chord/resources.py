from typing import Any, Set, Optional

class Resource:    
    def __init__(self, 
                 id: int,
                 content: Any):
        # think if you can have a way of determining
        # the resource's id based on the content 
        # problem: whould I have to pass k?
        # (remember how resources are organized on chord)
        self.id = id
        self.content = content
    
    def __repr__(self):
        return f"Resource(id={self.id})"
    
    def __hash__(self):
        return self.id
    # think of other useful methods


class ResourceStorage:
    def __init__(self,
                 resource_list: Optional[Set[Resource]] = None):
        self.storage: Set[Resource] = set(resource_list) if resource_list is not None else set()
        # if i use set, __hash__ on resource returns id

        # should i store a list/set of all ids so that before i lookup
        # a resource i can make sure its actually there?
        # this would save me from having to define hash methods
        # although it has more space complecity
    
    def __iter__(self):
        for el in self.storage:
            yield el
    
    def get_resource(self, 
                     idx: int):
        for resource in self:
            if resource.id == idx:
                return resource
        raise KeyError(f"Resource with id={idx} not present")
    
    def add_resource(self, 
                     resource: Resource):
        # actually here having a set of ids would be useful because
        # i could quickly check if there is already a resource with the same id
        self.storage.add(resource)

    
    def delete_resource(self,
                        idx: int):
        to_delete = self.get_resource(idx)
        self.storage.remove(to_delete)