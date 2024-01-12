import argparse
import os
import datetime

from chord.protocol import ProtocolSimulator

parser = argparse.ArgumentParser(
    prog = "Chord Simulator",
    description= """
        Simulate a Chord protocol where at each epoch a node can join 
        or leave the network with a certain probability set by the user.

        This simulator is limited to one node joining or leaving at a time,
        so there is no concurrency in the joins/leaves.
    """
)

parser.add_argument("k", type = int,
                    help = "Dimension parameter for network")
parser.add_argument("-n", "--n_epochs", type = int, required = True,
                    help = "Number of epochs to run the simulation for")
parser.add_argument("-j", "--node_join_prob", type = float, required = True,
                    help = "Probability with which there is a join during an epoch")
parser.add_argument("-f", "--node_fail_prob", type = float, required = True,
                    help = "Probability with which there is a fail during an epoch")
parser.add_argument("-s", "--save_data", type = bool, required = True,
                    help = "Whether to save simulation data or not")
parser.add_argument("--stab_sleep", type = float, required = False,
                    help = "Pause between one stabilization and another (you must set both this and epoch_sleep)")
parser.add_argument("--epoch_sleep", type = float, required = False,
                    help = "Pause between epochs (you must_set both this and stab_sleep)")

if __name__ == "__main__":
    args = parser.parse_args()
    p = ProtocolSimulator(args.k)
    try:
        if args.stab_sleep is None and args.epoch_sleep is None:
            res = p.simulate(n_epochs=args.n_epochs, 
                            node_join_probability=args.node_join_prob,
                            node_failure_probability=args.node_fail_prob,
                            save_data=args.save_data)
        else:
            res = p.simulate(n_epochs=args.n_epochs, 
                            node_join_probability=args.node_join_prob,
                            node_failure_probability=args.node_fail_prob,
                            save_data=args.save_data,
                            stab_sleep=args.stab_sleep,
                            epoch_sleep=args.epoch_sleep)
    except KeyboardInterrupt:
        exit()
    
    if True:
        path_to_here = os.path.abspath(os.getcwd())
        path_to_data = path_to_here[:-3] + "data"
        
        simulation_date = str(datetime.date.today())
        siulation_time = str(datetime.datetime.now().time()).split(".")[0].replace(":", "-")
        res.to_csv(f"{path_to_data}/{simulation_date}_{siulation_time}_K{args.k}_J{args.node_join_prob}_F{args.node_fail_prob}.csv")