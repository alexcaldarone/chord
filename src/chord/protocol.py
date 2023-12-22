import numpy as np
import pandas as pd
import time
from threading import Thread, Event
from typing import Tuple, Union, NewType

NaNType = NewType("NaN", np.nan)

from DHT import DistributedHashTable
from node import Node

class ProtocolSimulator:
    
    def __init__(self, 
                 k: int):
        self.k: int = k
        self.network: DistributedHashTable = DistributedHashTable(k)
        self._used_node_ids: list[int] = []
        self._resource_ids: list[int] = []

    def node_join(self, 
                  node1: Node, 
                  node2: Node = None):
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
            for node in self.network.nodes:
                if node is not None: 
                    node.stabilize(self.network)
    
    def notification(self):
        if self.network.counter > 1:
            for node in self.network.nodes:
                if node is not None:
                    pred = node.predecessor["node"]
                    succ = node.successor["node"]
                    node.notify(pred)
                    node.notify(succ)
    
    def manage_node_fail(self, 
                         failed_node_id: int):
        node_to_fail = self.network[failed_node_id]
        self._used_node_ids.remove(failed_node_id)
        node_to_fail.exit(self.network)
        try:
            self._resource_ids.remove((failed_node_id-1) % 2**self.k)
        except ValueError: # resource wasn't there
            pass
    
    def search(self, 
               node_id: int, 
               resource_id: int) -> Tuple[Union[Node, NaNType], Union[int, NaNType], str]:
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
        if self.network.counter > 0:
            for node in self.network.nodes:
                if node is not None: 
                    node.fix_fingers()
    
    def fix_successor_list(self):
        if self.network.counter > 0:
            random_node_fix_id = np.random.choice(self._used_node_ids,
                                                size = 1)[0]
            self.network[random_node_fix_id].fix_successor_list()
    
    def periodic_stabilization_procedures(self, event):
        while True:
            if event.is_set():
                break
            self.notification()
            self.stabilize_network()
            self.fix_fingers()
            self.fix_successor_list()
            time.sleep(0.5)
    
    def simulate_epochs(self,
                        n_epochs: int,
                        node_join_prob: float,
                        node_failure_prob: float,
                        exit_event: Event,
                        save_data: bool,
                        df: Union[pd.DataFrame, None]):
        for epoch in range(n_epochs):
            print(f"EPOCH: {epoch}")

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
                                            p = [1 - node_failure_prob, node_failure_prob])[0]
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
                solver, jumps, how = self.search(rand_node_begin_search, rand_res_to_find)
                if jumps is np.nan:
                    print(f"--- Resource was not found (search failed)")
                else:
                    print(f"--- Resource was found in {jumps} ({how}) jumps")
            
            # saving epoch data
            if save_data:
                if self.network.counter == 0: # there's been no search
                    search_res = (np.nan, np.nan, np.nan, np.nan, np.nan)
                    fails = (0, 0)
                else:
                    search_res = (rand_node_begin_search, rand_res_to_find, 
                                  solver.id if isinstance(solver, Node) else solver, jumps, how)
                    fails = (node_fails, rand_node_to_fail_id if node_fails else 0)
                df.loc[len(df)] = (epoch, self.network.counter, node_joins,
                                   rand_id_choice if node_joins else 0,  *fails, *search_res)
            time.sleep(1)
        exit_event.set()

    def simulate(self,
                 n_epochs: int,
                 node_join_probability: float,
                 node_failure_probability: float,
                 save_data: bool) -> Union[pd.DataFrame, None]:
        print("""
        **********************************************************
        * Welcome to the simulaiton, Neo ...                     *
        **********************************************************
        """)
        if save_data:
            df = pd.DataFrame(columns = [
                "epoch", "n_nodes", "node_join", "node_join_id", "node_fail",
                "node_fail_id", "search_init", "resource_to_find", "search_solver",
                "jumps", "how"
            ])
        else:
            df = None
        
        exit_event = Event()
        p1 = Thread(target = self.simulate_epochs, args = (n_epochs,
                                                           node_join_probability,
                                                           node_failure_probability,
                                                           exit_event,
                                                           save_data,
                                                           df))
        p1.start()
        p2 = Thread(target = self.periodic_stabilization_procedures, args = (exit_event,))
        p2.start()
        p1.join()
        p2.join()
        
        print("""
        **********************************************************
        * Sadly, you are exiting the simulation...               *
        **********************************************************
        """)
        return df