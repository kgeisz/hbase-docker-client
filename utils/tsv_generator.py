import os
import random
import sys

NUM_ROWS = 500 
NUM_COLUMNS = 10

def generate_data(row_key):
    # Generate a row with NUM_COLUMNS columns of random integers
    columns = [str(random.randint(1, 100)) for _ in range(NUM_COLUMNS)]
    return f"{row_key}\t" + "\t".join(columns) + "\n"

def main(output_dir):
    tsv_file = os.path.join(output_dir, "output.tsv")

    # Write the TSV file
    with open(tsv_file, "w") as f:
        for i in range(NUM_ROWS):
            row_key = f"row{i}"  # Create a unique row key
            f.write(generate_data(row_key))

    print(f"TSV file generated at {tsv_file} with {NUM_ROWS} rows and {NUM_COLUMNS} columns.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python tsv_generator.py <output_directory>")
        sys.exit(1)

    output_directory = sys.argv[1]
    main(output_directory)