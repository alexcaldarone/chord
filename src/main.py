import matplotlib.pyplot as plt
import sys
import numpy as np
import pandas as pd

from chord.DHT import DistributedHashTable
from chord.node import Node
from chord.resources import Resource

# code has been changes since this file was last run
# there's a possibility it wont work anymore

if __name__ == "__main__":
    ks = [3, 4, 5, 6, 7, 8, 9, 10 , 11]
    LIST_TO_ADD_TO_DF = []
    for k_net in ks:
        print(f"----- K = {k_net} -----")
        n_nodes = 2**k_net // 3
        resources_per_node = 1

        assert n_nodes * resources_per_node <= 2**k_net

        MAX_NET = 10
        n_network = 0
        tries = 0
        while n_network < MAX_NET:
            tries += 1
            print(f"- Network {n_network}")
            network = DistributedHashTable(k_net)
            rng = np.random.default_rng()
            available_indexes_1 = np.arange(int(2*(2**k_net)/6), int(4*(2**k_net)/6))
            #available_indexes_2 = np.arange(int(4*(2**k_net)/6), int(5*(2**k_net)/6))
            #random_indices = rng.choice(available_indexes, size=n_nodes, replace=False)
            #random_indices = sorted(random_indices)
            random_indices = available_indexes_1
            #random_indices = [np.random.randint(0, 2**k) for _ in range(n_nodes)]
            #random_indices = [n for n in range(0, n_nodes)]
            #print("RANDOM INDICES:", random_indices)

            print("-- adding nodes")
            # for reason only works properly if the nodes are joined
            # to a node with higher index
            node_list = [Node(el, k_net) for el in random_indices]
            node_list[-1].join(network)
            for i in range(len(node_list)-2, -1, -1):
                node_list[i].join(network, node_list[i+1])
                node_list[i].notify(node_list[i].successor["node"])
                node_list[i].notify(node_list[i].predecessor["node"])
                node_list[i].notify(node_list[-1])
                node_list[-1].notify(node_list[i])
                node_list[i].stabilize(network)
            
            print("-- stabilizing network")
            for _ in range(n_nodes * 10):
                k = np.random.choice(node_list, 1)[0]
                k.stabilize(network)
                k.notify(k.successor["node"])
                k.notify(k.predecessor["node"])
                k.stabilize(network)
                for _ in range(10): k.fix_fingers()

            print("-- verification of network correctness")
            good = True
            for node in node_list:
                if node.id != node.successor["node"].predecessor["id"]:
                    good = False
                    print(f"bad connection: {node}, {node.successor['node']}")
                    break
            if not good:
                continue
            else:
                print("--- network formed correctly")
                #print(network.nodes)
                print("-- adding resources")
                # aggiunta risorse
                resources = [Resource((node.id - 1) % 2**k_net, None)
                            for node in node_list]
                for node in node_list:
                    node.add_resource(Resource((node.id - 1) % 2**k_net, None))

                #for node in node_list:
                #    print(node, node.resources.storage)
                
                print("-- running searches")
                jump_list = []
                for search_n in range(100):
                    rand_node = np.random.choice(node_list, size = 1)[0]
                    rand_res = np.random.choice(resources, size = 1)[0]
                    #print("rand_resource:", rand_res)
                    #print("beginning node:", rand_node)
                    res_node, jumps = network.search(rand_res.id, rand_node.id)
                    jump_list.append(
                        (k_net, n_network, search_n, jumps)
                    )
                LIST_TO_ADD_TO_DF.append(jump_list)
                n_network += 1
    
    df = pd.DataFrame.from_records(zip(LIST_TO_ADD_TO_DF))
    df.to_csv("C:/Users/alexc/Desktop/Universita/3_anno/sistemi_di_elaborazione_2/laboratori/dht/data/stable_networks_one_cluster.csv")