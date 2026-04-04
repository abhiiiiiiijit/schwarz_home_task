import gzip
import csv

# Input the path to your .csv.gz file
#/home/adminabhi/gitrepo/schwarz_home_task/data/data_example.csv.gz
file_path = "/home/adminabhi/gitrepo/schwarz_home_task/data/data_example.csv.gz"

with gzip.open(file_path, mode="rt", newline="") as f:
    reader = csv.reader(f)
    for row in reader:
        print(row)


