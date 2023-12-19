import asyncio
import numpy as np

from DHT import DistributedHashTable
from node import Node

class ProtocolSimulator:
    
    def __init__(self, k: int):
        self.k = k
        self.network = DistributedHashTable(k)
        self._used_node_ids = []

    async def node_join(self, node1, node2 = None):
        # generate a random node
        if not node2:
            await node1.join(self.network)
        else:
            await node1.join(self.network, node2)
            await node1.notify(node2)
            await node2.notify(node1)
        
        self._used_node_ids.append(node1.id)
        # do i run stabilize and notify here?
    
    """
    def async_node_join(self, node1, node2 = None, existing_loop = True):
        if existing_loop:
            loop = asyncio.get_event_loop()
        else:
            loop = asyncio.new_event_loop()
        return loop.create_task(self.node_join(node1, node2))"""

    async def stabilize_network(self):
        random_node_to_stabilize_id = np.random.choice(self._used_node_ids,
                                                    size = 1)[0]
        node = self.network[random_node_to_stabilize_id]
        await node.stabilize(self.network)

    async def notification(self):
        random_node_notification_id = np.random.choice(self._used_node_ids,
                                                    size = 1)[0]
        node = self.network[random_node_notification_id]
        pred = self.network[random_node_notification_id].predecessor["node"]
        succ = self.network[random_node_notification_id].successor["node"]
        # 4 notifications useless?
        await node.notify(pred)
        await pred.notify(node)
        await node.notify(succ)
        await succ.notify(pred)

    async def manage_node_fail(self, failed_node_id):
        await self.network[failed_node_id].exit()

    async def search(self, node_id, resource_id):
        start_node_id = node_id
        for _ in range(3):
            try:
                res_node, jumps = self.network.search(resource_id, start_node_id)
                return res_node, jumps
            except LookupError:
                # if search fails we start it from another randomly chosen node
                start_node_id = np.random.choice(self._used_node_ids, size = 1)[0]
                continue
        # if not found try linear search
        res_node_lin, jumps_lin = self.network.linear_search_resource(node_id, resource_id)
        return res_node_lin, jumps_lin

    async def fix_fingers(self):
        random_node_fix_id = np.random.choice(self._used_node_ids,
                                              size = 1)[0]
        await self.network[random_node_fix_id].fix_fingers()

    async def fix_successor_list(self):
        random_node_fix_id = np.random.choice(self._used_node_ids,
                                              size = 1)[0]
        await self.network[random_node_fix_id].fix_successor_list()

    async def simulate(self,
                 n_epochs,
                 node_join_probability,
                 node_failure_probability):
        # misurare epoche in numero di operazioni fatte? secondi?
        # se misuro in secondi sarò più lento
        asyncio.gather(
            self.notification(),
            self.stabilize_network(),
            self.fix_fingers(),
            self.fix_successor_list()
        )
        # this works when called one
        # now:
        # 1. figure out a way to have this run for n_epochs
        # 2. add node joins and failures here
        # 3. add searches

if __name__ == "__main__":
    p = ProtocolSimulator(3)
    n1 = Node(1, k = 3)
    n2 = Node(4, k = 3)
    asyncio.run(p.node_join(n2))
    asyncio.run(p.node_join(n1, n2))
    print(p.network.nodes)
    asyncio.run(p.simulate(0, 0, 0))