# Chord: A Scalable Peer-to-peer Lookup Service for Internet Applications

The goal of this project is to implement the basic functionalities of the Chord protocol. More specifically, the code is aimed at simulating the _evolution_ of a network that utilized the Chord protocol. By _evolution_ we inted the behaviour of a network that starts out with zero nodes and that then is subject to node joins, node fails and resource lookups.


### Usage
To clone the repository run:
```shell
git clone https://github.com/alexcaldarone/chord.git
cd chord
python setup.py install
```

To see the parameters of the simulation execute:
```shell
cd src
python simulate.py -h
```

Example of how to run a simulation for 1000 epochs, on a network of maximum size 2048 nodes, where at each epoch a node can join with probability 0.8 and a node can leave with probability 0.2:
```shell
python simulate.py 11 -n 1000 -j 0.8 -f 0.2
```

### TO DO:
- [ ] There is a error somewhere that is causing the bootstrap node to see the node joining the network as his successor when he shouldn't (also, it seems that when I reassing it nothing changes in the list of nodes?)
- [ ] Improve exit procedure of nodes (make use of successors list) as it is causing searches to fail 
- [ ] Improve FT (just use node instead of dictionary? Do i need to refactor some code in Node class?)
- [ ] Change node failure probability from fixed to variable (can assume several different values during the simulation)
- [ ] Add tqdm to time simulations and add a printed summary at the end
- [ ] ***add tests***