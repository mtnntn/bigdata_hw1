import os
import sys
import subprocess
import pyspark as ps

"""
 Il Job genera, per ciascun anno, le dieci parole che sono state piu' usate nelle recensioni (campo 'Summary'). 
 L'output e' ordinato, per ogni anno, sulla frequenza delle parole. 
 Per frequenza si intende il numero di occorrenze della parola nelle recensioni di quell'anno.
"""


""" Defining Constants """
master = "local[*]"
app_name = "Esercizio1"
input_filename = "reviews_clean.csv"  # "test_shuffle_clean.csv"  #
input_path = os.path.join(os.sep, "user", "root", input_filename)
output_folder_name = "1"
output_path = os.path.join(os.sep, "user", "root", "output_spark", output_folder_name)

""" Job > Constants """

header = {
    "Id": 0,
    "ProductId": 1,
    "UserId": 2,
    "ProfileName": 3,
    "HelpfulnessNumerator": 4,
    "HelpfulnessDenominator": 5,
    "Score": 6,
    "Time": 7,
    "Summary": 8,
    "Text": 9
}

""" Get Input """
# Expected arguments-> "path/to/input", "path/to/output"
arguments = sys.argv
arguments.pop(0)
if len(arguments) == 2:
    input_path = arguments[0]
    output_path = arguments[1]

""" Cleaning Output Folder """

rmdir_cmd = ["hdfs", "dfs", "-rm", "-r", output_path]
proc = subprocess.Popen(rmdir_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
proc.communicate()


""" Job > Functions """


def get_year_word_frequency(x):
    splitted_line = x.split(",")
    year = splitted_line[header["Time"]].split("-")[0]
    text = splitted_line[header["Summary"]].split(" ")
    return [(year+"-"+w, 1) for w in text]


def get_top_n(kv):
    values = list(kv[1])
    values = sorted(values, key=lambda x: (x[1], x[0]), reverse=True)
    res = list()
    for i, e in enumerate(values):
        if i < 10:
            y = kv[0]
            w = e[0]
            f = e[1]
            res.append(y+" "+w+" "+str(f))
        else:
            break
    return res


""" Job > Main """

conf = ps.SparkConf().setMaster(master).setAppName(app_name)

spark_context = ps.SparkContext(conf=conf)

lines = spark_context.textFile(input_path)

year_word_sum = lines.flatMap(get_year_word_frequency).reduceByKey(lambda k1_value_1, k1_value_2: k1_value_1+k1_value_2)

output = year_word_sum.map(lambda kv: (kv[0].split("-")[0], (kv[0].split("-")[1], kv[1]))).groupByKey().flatMap(get_top_n)

output.saveAsTextFile(output_path)
