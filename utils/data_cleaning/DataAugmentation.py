import pandas as pd
import os
import sys

workdir_path = os.path.join("datasets")
input_filename = "reviews_clean.csv"  # "test_shuffle.csv"  # #
input_file_path = os.path.join(workdir_path, input_filename)
iterations = 5

if len(sys.argv) > 1:
    iterations = int(sys.argv[1])

output_filename = input_filename.split(".")[0]+"_"+str(iterations)+"x."+input_filename.split(".")[1]

output_file_path = os.path.join(workdir_path, output_filename)

fmt = '%Y-%m-%d %H:%M:%S'

""" Augment File """
reviews = pd.read_csv(input_file_path)
dataframes = [reviews]
i = 1
while i <= iterations:
    pd2 = pd.DataFrame.copy(reviews)
    pd2["Id"] += i*len(reviews)
    dataframes.append(pd2)
    i += 1

conc = pd.concat(dataframes, ignore_index=True)
conc.to_csv(output_file_path, index=False, sep=",", line_terminator="\n")