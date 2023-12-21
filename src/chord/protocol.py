import numpy as np
import time
from threading import Thread, Event

from DHT import DistributedHashTable
from node import Node

class ProtocolSimulator:
    
    def __init__(self, k: int):
        self.k = k
        self.network = DistributedHashTable(k)
        self._used_node_ids = []
        self._resource_ids = []

    def node_join(self, node1, node2 = None):
        self._used_node_ids.append(node1.id)
        if not node2:
            node1.join(self.network)
        else:
            node1.join(self.network, node2)
            node1.notify(node2)
            node2.notify(node1)
        
        try:
            node1.add_resource(((node1.id-1) % 2**self.k, None))
            self._resource_ids.append((node1.id-1) % 2**self.k)
        except:
            print(f"Was not able to add resource wit id {(node1.id-1) % 2**self.k} on node {node1.id}")

    def stabilize_network(self):
        if self.network.counter > 0:
            #for node in self.network.nodes:
            random_node_to_stabilize_id = np.random.choice(self._used_node_ids,
                                                        size = 1)[0]
            node = self.network[random_node_to_stabilize_id]
            node.stabilize(self.network)
            #    if node is not None: node.stabilize(self.network)

    def notification(self):
        #+print(self.network.counter)
        if self.network.counter > 1:
            """for node in self.network.nodes:
                if node is not None:
                    pred = node.predecessor["node"]
                    succ = node.successor["node"]"""
            random_node_notification_id = np.random.choice(self._used_node_ids,
                                                        size = 1)[0]
            #print(f"NOTIFICATION: Selected node = {random_node_notification_id}")
            node = self.network[random_node_notification_id]
            pred = self.network[random_node_notification_id].predecessor["node"]
            succ = self.network[random_node_notification_id].successor["node"]
            # 4 notifications useless?
            node.notify(pred)
            node.notify(succ)
            pred.notify(node)
            succ.notify(node)

    def manage_node_fail(self, failed_node_id):
        node_to_fail = self.network[failed_node_id]
        self._used_node_ids.remove(failed_node_id)
        node_to_fail.exit(self.network)
        try:
            self._resource_ids.remove((failed_node_id-1) % 2**self.k)
        except ValueError:
            pass

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
        try:
            res_node_lin, jumps_lin = self.network.linear_search_resource(node_id, resource_id)
            return res_node_lin, jumps_lin, "lin"
        except LookupError:# linear seach fails
            return np.nan, np.nan, "not_found"

    def fix_fingers(self):
        #print(self.network.counter)
        if self.network.counter > 0:
            """for node in self.network.nodes:
                if node is not None: 
                    #print(f"fixing fingers of {node}")
                    node.fix_fingers()"""
            random_node_fix_id = np.random.choice(self._used_node_ids,
                                                size = 1)[0]
            self.network[random_node_fix_id].fix_fingers()

    def fix_successor_list(self):
        if self.network.counter > 0:
            random_node_fix_id = np.random.choice(self._used_node_ids,
                                                size = 1)[0]
            self.network[random_node_fix_id].fix_successor_list()

    def periodic_stabilization_procedures(self, event):
        while True:
            if event.is_set():
                print("stopped")
                break
            #print("beginning stabilization ...")
            self.notification()
            self.stabilize_network()
            self.fix_fingers()
            self.fix_successor_list()
            time.sleep(0.5)
            #print("ending stabilization ...")
    
    def simulate_epochs(self,
                        n_epochs,
                        node_join_prob,
                        node_failure_prob,
                        exit_event):
        # 1. uno nodo si unisce alla dht con prob node_join_prob
        # 2. un nodo fallisce con prob node_failure_prob
        # 3. far partire delle search
        for epoch in range(n_epochs):
            print(f"EPOCH: {epoch}")
            #print(self.network.nodes)

            # probabilistic node join
            node_joins = np.random.choice([0, 1], size = 1, 
                                          p = [1 - node_join_prob, node_join_prob])[0]
            if self.network.counter < 2**self.k:
                if node_joins:
                    available_ids = [n for n in range(0, 2**self.k) if n not in self._used_node_ids]

                    rand_id_choice = np.random.choice(available_ids, size = 1)[0]
                    node_to_add = Node(rand_id_choice, k = self.k)

                    if self.network.counter >= 1:
                        boot_id = np.random.choice(self._used_node_ids, size = 1)[0]
                        boot_node = self.network[boot_id]
                        #print(f"node to add: {rand_id_choice}")
                        #print(f"Boostrap node: {boot_id} with FT {boot_node.FT}")
                    else:
                        boot_node = None
                    self.node_join(node_to_add, boot_node)
                    print(f"--- Node with id {rand_id_choice} joined the network")
                else:
                    print(f"--- No node joins the network at this epoch")
            else:
                print("--- Can't add node to a full network")

            # probabilistic node failure
            if self.network.counter > 0:
                node_fails = np.random.choice([0,1], size = 1,
                                            p = [1 - node_failure_prob, node_failure_prob])
                if node_fails:
                    rand_node_to_fail_id = np.random.choice(self._used_node_ids, size = 1)[0]
                    self.manage_node_fail(rand_node_to_fail_id)
                    print(f"--- Node with id {rand_node_to_fail_id} exited the network")
                else:
                    print(f"--- No node exits the network at this epoch")
            
            # search
            if self.network.counter > 0:
                rand_node_begin_search = np.random.choice(self._used_node_ids, size = 1)[0]
                rand_res_to_find = np.random.choice(self._resource_ids, size = 1)[0]
                #print("node that begins search:", rand_node_begin_search)
                #print("resource to find:", rand_res_to_find)

                solver, jumps, how = self.search(rand_node_begin_search, rand_res_to_find)
                if jumps is np.nan:
                    print(f"--- Resource was not found (search failed)")
                else:
                    print(f"--- Resource was found in {jumps} ({how}) jumps")

            time.sleep(1)
        exit_event.set()


    def simulate(self,
                 n_epochs,
                 node_join_probability,
                 node_failure_probability):
        print("""
        **********************************************************
        * You are now entering the simulation...                 *
        **********************************************************
        """)
        exit_event = Event()
        """p1 = multiprocess.Process(target = self.simulate_epochs, args = (n_epochs,
                                                                         node_join_probability,
                                                                         node_failure_probability,
                                                                         queue,))"""
        p1 = Thread(target = self.simulate_epochs, args = (n_epochs,
                                                           node_join_probability,
                                                           node_failure_probability,
                                                           exit_event,))
        p1.start()
        p2 = Thread(target = self.periodic_stabilization_procedures, args = (exit_event,))
        p2.start()
        p1.join()
        p2.join()
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
    p.simulate(100,0.7,0.2)