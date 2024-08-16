import csv
import hashlib
import os
from multiprocessing import Pool, cpu_count


def hash_value(value):
    return hashlib.sha256(value.encode()).hexdigest()


def process_chunk(chunk_id, start_row, end_row, input_file, output_file, header):
    with open(input_file, 'r') as infile, open(output_file, 'a', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=header)

        # Only the first process writes the header
        if chunk_id == 0:
            writer.writeheader()

        for i, row in enumerate(reader):
            if start_row <= i < end_row:
                row['first_name'] = hash_value(row['first_name'])
                row['last_name'] = hash_value(row['last_name'])
                row['address'] = hash_value(row['address'])
                writer.writerow(row)


def parallel_hash_csv(input_file, output_file):
    # calc number of lines in the file
    with open(input_file, 'r') as infile:
        # Subtract 1 for header
        row_count = sum(1 for row in infile) - 1

    num_processes = cpu_count()
    chunk_size = row_count // num_processes

    # get header
    with open(input_file, 'r') as infile:
        reader = csv.reader(infile)
        header = next(reader)

    # args for parallel processing
    chunks = [
        (
            chunk_id,
            # start_row
            chunk_id * chunk_size,
            # end_row
            (chunk_id + 1) * chunk_size if chunk_id != num_processes - 1 else row_count,
            input_file,
            output_file,
            header,
        )
        for chunk_id in range(num_processes)
    ]

    with Pool(processes=num_processes) as pool:
        pool.starmap(process_chunk, chunks)


if __name__ == "__main__":
    input_file = 'data/large_dataset.csv'
    output_file = 'data/output/anonymized_large_dataset.csv'

    # Clear output file before processing
    open(output_file, 'w').close()

    parallel_hash_csv(input_file, output_file)
