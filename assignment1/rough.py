import gzip
import csv

# Input the path to your .csv.gz file
file_path = "/home/adminabhi/gitrepo/schwarz_home_task/assignment1/data_5.csv.gz"

# Output CSV file path
output_file = "input_data.csv"

# Read from compressed CSV and write to normal CSV
with gzip.open(file_path, mode="rt", newline="") as f_in:
    reader = csv.reader(f_in)
    
    with open(output_file, mode="w", newline="") as f_out:
        writer = csv.writer(f_out)
        
        for row in reader:
            writer.writerow(row)          # Write each row to the output CSV
            # Optional: print(row)        # Uncomment if you still want to see output in console

print(f"✅ Successfully written to {output_file}")