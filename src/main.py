import matplotlib.pyplot as plt
import sys
import numpy as np

from chord.DHT import DistributedHashTable
from chord.node import Node

if __name__ == "__main__":
    k = sys.argv[1]
    n_nodes = sys.argv[2]

    network = DistributedHashTable(k)

    random_indices = np.random.randint(0, 2**k, size = n_nodes)

    for i in random_indices:
        network.add_node(Node(id = i, k = k))
    
    for el in network:
        for idx in range(1, len(el.FT)):
            el.FT[idx] = network.succ(2 ** (idx - 1))
    
    print(network.nodes)
