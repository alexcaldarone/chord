import matplotlib.pyplot as plt
import sys
import numpy as np

from chord.DHT import DistributedHashTable
from chord.node import Node

if __name__ == "__main__":
    k = 3
    n_nodes = 4
    resources_per_node = 1

    assert n_nodes * resources_per_node <= 2**k

    network = DistributedHashTable(k)

    random_indices = [np.random.randint(0, 2**k) for _ in range(n_nodes)]
    print("RANDOM INDICES:", random_indices)

    node_list = [Node(el, k) for el in random_indices]
    node_list[0].join(network)
    for i in range(1, len(node_list)):
        node_list[i].join(network, node_list[i-1])
        node_list[i].stabilize(network)
        node_list[i-1].notify(node_list[i])
        node_list[i].notify(node_list[i-1])
    
    for _ in range(10):
        for i in range(len(node_list)):
            node_list[i].stabilize(network)
            node_list[i].notify(node_list[i-1])
            node_list[i-1].notify(node_list[i])
    
    for node in node_list:
        for _ in range(15): node.fix_fingers()
    
    print(network.nodes)
    print("---------------------")
    # aggiunta risorse
    res_id = [(node.id - 1) % 2**node.k for node in node_list]
    print("res_id:", res_id)
    print("node_list:", node_list)
    for i in range(len(node_list)):
        node_list[i].add_resource((res_id[i], None))
    
    random_res = int(np.random.choice(res_id, 1))
    random_node = np.random.choice(node_list, 1)[0]
    network.search(random_res, random_node.id)

    """
    # aggiungo risorse (per ora una pari a id-1)
    resources = [node.id-1 for node in network]
    print("resources:", resources)
    for res in resources:
        network[res+1].add_resource(res)
        print(network[res+1])
    
    # scelgo un nodo a caso e faccio partire una ricerca
    for _ in range(100):
        idx_choice = np.random.choice(random_indices, 1)
        print("idx_choice", int(idx_choice))
        res_choice = np.random.choice(resources, 1)
        print("resource choice", int(res_choice))
        network.search(res_choice, int(idx_choice))"""