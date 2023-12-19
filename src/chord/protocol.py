import asyncio
import numpy as np

from DHT import DistributedHashTable

class ProtocolSimulator:
    
    def __init__(self, k: int):
        self.k = k
        self.network = DistributedHashTable(k)
        self._used_node_ids = set()

    async def node_join(self, node1, node2):
        # generate a random node
        if not node2:
            node1.join(self.network)
        else:
            node1.join(self.network, node2)
        
        self.__used_node_ids.add(node1.id)
        # do i run stabilize and notify here?
        

    async def stabilize_network(self):
        random_node_to_stabilize_id = np.random.choice(self._used_node_ids,
                                                    size = 1)
        self.network.nodes[random_node_to_stabilize_id].stabilize()

    async def notification(self):
        random_node_notification_id = np.random.choice(self._used_node_ids,
                                                    size = 1)
        # potrei indicizzare direttamente su nodes
        node = self.network.nodes[random_node_notification_id]
        pred = self.network.nodes[random_node_notification_id].predecessor["node"]
        succ = self.network.nodes[random_node_notification_id].successor["node"]
        # 4 notifications useless?
        node.notify(pred)
        pred.notify(node)
        node.notify(succ)
        succ.notify(pred)

    async def manage_node_fail(self, failed_node_id):
        self.network[failed_node_id].exit()

    async def search(self, node_id, resource_id):
        start_node_id = node_id
        for _ in range(3):
            try:
                res_node, jumps = self.network.search(resource_id, start_node_id)
                return res_node, jumps
            except LookupError:
                # if search fails we start it from another randomly chosen node
                start_node_id = np.random.choice(self._used_node_ids, size = 1)
                continue
        # if not found try linear search
        res_node_lin, jumps_lin = self.network.linear_search_resource(node_id, resource_id)
        return res_node_lin, jumps_lin

    async def fix_fingers(self):
        random_node_fix_id = np.random.choice(self._used_node_ids,
                                              size = 1)
        self.network[random_node_fix_id].fix_fingers()

    async def fix_successor_list(self):
        random_node_fix_id = np.random.choice(self._used_node_ids,
                                              size = 1)
        self.network[random_node_fix_id].fix_successor_list()

    def simulate(self,
                 n_epochs,
                 node_join_probability,
                 node_failure_probability):
        # misurare epoche in numero di operazioni fatte? secondi?
        # se misuro in secondi sarò più lento
        pass

# resource movement qui o su classe nodo?