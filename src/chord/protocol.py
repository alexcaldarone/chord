import asyncio
import numpy as np
import time
import concurrent.futures
import schedule
from threading import Thread
from periodic import Periodic
from aiomultiprocess import Pool
import multiprocess

from DHT import DistributedHashTable
from node import Node

class ProtocolSimulator:
    
    def __init__(self, k: int):
        self.k = k
        self.network = DistributedHashTable(k)
        self._used_node_ids = []

    def node_join(self, node1, node2 = None):
        # generate a random node
        if not node2:
            node1.join(self.network)
        else:
            node1.join(self.network, node2)
            node1.notify(node2)
            node2.notify(node1)
        self._used_node_ids.append(node1.id)

    def stabilize_network(self):
        if self.network.counter > 0:
            random_node_to_stabilize_id = np.random.choice(self._used_node_ids,
                                                        size = 1)[0]
            node = self.network[random_node_to_stabilize_id]
            node.stabilize(self.network)

    def notification(self):
        if self.network.counter > 0:
            random_node_notification_id = np.random.choice(self._used_node_ids,
                                                        size = 1)[0]
            print(f"NOTIFICATION: Selected node = {random_node_notification_id}")
            node = self.network[random_node_notification_id]
            pred = self.network[random_node_notification_id].predecessor["node"]
            succ = self.network[random_node_notification_id].successor["node"]
            # 4 notifications useless?
            node.notify(pred)
            #node.notify(succ)
            pred.notify(node)
            #succ.notify(node)

    def manage_node_fail(self, failed_node_id):
        node_to_fail = self.network[failed_node_id]
        node_to_fail.exit(self.network)

    def search(self, node_id, resource_id):
        start_node_id = node_id
        for _ in range(3):
            try:
                res_node, jumps = self.network.search(resource_id, start_node_id)
                return res_node, jumps, "log"
            except LookupError:
                # if search fails we start it from another randomly chosen node
                start_node_id = np.random.choice(self._used_node_ids, size = 1)[0]
                continue
        # if not found try linear search
        res_node_lin, jumps_lin = self.network.linear_search_resource(node_id, resource_id)
        return res_node_lin, jumps_lin, "lin"

    def fix_fingers(self):
        if self.network.counter > 0:
            random_node_fix_id = np.random.choice(self._used_node_ids,
                                                size = 1)[0]
            self.network[random_node_fix_id].fix_fingers()

    def fix_successor_list(self):
        if self.network.counter > 0:
            random_node_fix_id = np.random.choice(self._used_node_ids,
                                                size = 1)[0]
            self.network[random_node_fix_id].fix_successor_list()

    def periodic_stabilization_procedures(self):
        while True:
            #print("beginning stabilization ...")
            self.notification()
            self.stabilize_network()
            self.fix_fingers()
            #self.fix_successor_list()
            #print("ending stabilization ...")
            time.sleep(0.5)
    
    def simulate_epochs(self,
                        n_epochs,
                        node_join_prob,
                        node_failure_prob,
                        q):
        # 1. uno nodo si unisce alla dht con prob node_join_prob
        # 2. un nodo fallisce con prob node_failure_prob
        # 3. far partire delle search
        for epoch in range(n_epochs):
            print(f"|\t\t EPOCH {epoch} \t\t|")
            print(self.network.nodes)

            # probabilistic node join
            node_joins = np.random.choice([0, 1], size = 1, 
                                          p = [1 - node_join_prob, node_join_prob])[0]
            if node_joins:
                if self.network.counter == 2**self.k:
                    print("Can't add node to network as it is full")
                    continue
                available_ids = [n for n in range(0, 2**self.k) if n not in self._used_node_ids]

                rand_id_choice = np.random.choice(available_ids, size = 1)[0]
                node_to_add = Node(rand_id_choice, k = self.k)

                if self.network.counter >= 1:
                    boot_id = np.random.choice(self._used_node_ids, size = 1)[0]
                    boot_node = self.network[boot_id]
                    print(f"node to add: {rand_id_choice}")
                    print(f"Boostrap node: {boot_id}")
                else:
                    boot_node = None
                self.node_join(node_to_add, boot_node)
                print(f"Node with id {rand_id_choice} joined the network")
            else:
                print(f"No node joins the network at this epoch")

            # probabilistic node failure
            if self.network.counter > 0:
                node_fails = np.random.choice([0,1], size = 1,
                                            p = [1 - node_failure_prob, node_failure_prob])
                if node_fails:
                    rand_node_to_fail_id = np.random.choice(self._used_node_ids, size = 1)[0]
                    self.manage_node_fail(rand_node_to_fail_id)
                    print(f"Node with id {rand_node_to_fail_id} exited the network")
                else:
                    print(f"No node exits the network at this epoch")


            time.sleep(3)
        q.get()


    def simulate(self,
                 n_epochs,
                 node_join_probability,
                 node_failure_probability):
        queue = multiprocess.Queue()
        processes = []
        p1 = multiprocess.Process(target = self.simulate_epochs, args = (n_epochs,
                                                                         node_join_probability,
                                                                         node_failure_probability,
                                                                         queue,))
        processes.append(p1)
        p1.start()
        p2 = multiprocess.Process(target = self.periodic_stabilization_procedures)
        processes.append(p2)
        p2.start()
        queue.put("initiated_stab")
        #print("added put")

        p1.join()

        if queue.empty():
            # vuol dire che p1 ha finito
            p2.terminate()

        print("exited loop")
        # as of now this repeats stabilization periodically
        # to do:
        # 1. put this and code block for searches in separate threads ? 
        # oppure due processi separati, il primo per la stabilizzazione,
        # il secondo per il resto. Nel secondo poi posso usare più thread per 
        # gestire inserimenti, fail e search    
        # 2. write code block for searches/joins/failures

if __name__ == "__main__":
    p = ProtocolSimulator(3)
    """n1 = Node(1, k = 3)
    n2 = Node(4, k = 3)
    p.node_join(n1)
    p.node_join(n2, n1)
    print(p.network.counter)
    print(p.network.nodes)"""
    p.simulate(10,0.5,0)