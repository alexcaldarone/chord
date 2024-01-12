# Chord: A Scalable Peer-to-peer Lookup Service for Internet Applications

The goal of this project is to implement the basic functionalities of the Chord protocol. More specifically, the code is aimed at simulating the _evolution_ of a network that utilized the Chord protocol. By _evolution_ we inted the behaviour of a network that starts out with zero nodes and that then is subject to node joins, node fails and resource lookups.


### Usage
To clone the repository run:
```shell
git clone https://github.com/alexcaldarone/chord.git
cd chord
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