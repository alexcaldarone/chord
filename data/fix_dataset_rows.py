import pandas as pd 
import ast
import sys
import os

cwd = os.getcwd()
names = ["stable_networks_full.csv", "stable_networks_half_full.csv",
         "stable_networks_one_cluster.csv", "stable_networks_two_cluster.csv"]

for name in names:
    df = pd.read_csv(f"{cwd}\data\{name}")
    new_df = pd.DataFrame(columns = ["k", "network_n", "search_n", "jumps"])

    for row in df.iterrows():
        row_list = ast.literal_eval((row[1][1]))
        for tuple in row_list:
            new_df.loc[len(new_df)] = tuple

    new_df.to_csv(f"{cwd}\data\{name}")