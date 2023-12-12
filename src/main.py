import matplotlib.pyplot as plt
import sys
import numpy as np

from chord.DHT import DistributedHashTable
from chord.node import Node

if __name__ == "__main__":
    k = int(sys.argv[1])
    n_nodes = int(sys.argv[2])
    resources_per_node = int(sys.argv[3])

    assert n_nodes * resources_per_node <= 2**k

    network = DistributedHashTable(k)

    rng = np.random.default_rng()
    random_indices = rng.choice(2**k, size = n_nodes, replace = False)
    print(random_indices)
    for i in random_indices:
        network.add_node(Node(id = i, k = k))
    
    print(network.nodes)

    for node_index in random_indices:
        for idx in range(0, k):
            network[node_index].FT[idx] = network.succ(2 ** (idx))
        print(f"Filled FT of node: {network[node_index]}")
    
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
        network.search(res_choice, int(idx_choice))

    """
    Some thoughts:
    1. In DHT.add_node should I really update the prev and the finger table, if I am 
    doing the linear add? Should I just assume that all the nodes get inserted and then
    finger tables updated all at once and then the network starts working? This would imply
    that the struvture of the ring stays static and there can be no changes. 
    Could this be a viable option if I then focus on the concurrent joins?
    """