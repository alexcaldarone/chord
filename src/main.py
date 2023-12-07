import matplotlib.pyplot as plt
import sys
import numpy as np

from chord.DHT import DistributedHashTable

if __name__ == "__main__":
    k = sys.argv[1]
    n_nodes = sys.argv[2]

    network = DistributedHashTable(k)

    
