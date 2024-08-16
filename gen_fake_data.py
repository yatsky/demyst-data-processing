import csv
import os
from multiprocessing import Pool, cpu_count
import faker

fake = faker.Faker()


def generate_row(_):
    return {
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'address': fake.address().replace('\n', ', '),
        'date_of_birth': fake.date_of_birth(),
    }


def write_chunk(start, end, filename):
    with open(filename, 'a', newline='') as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=['first_name', 'last_name', 'address', 'date_of_birth']
        )
        if start == 0:
            writer.writeheader()
        for _ in range(start, end):
            writer.writerow(generate_row(None))


def parallel_generate_csv(rows, filename, num_processes=None):
    if num_processes is None:
        num_processes = cpu_count()

    chunk_size = rows // num_processes
    chunks = [
        (i * chunk_size, (i + 1) * chunk_size, filename) for i in range(num_processes)
    ]

    # remaining rows
    if rows % num_processes != 0:
        chunks[-1] = (chunks[-1][0], rows, filename)

    with Pool(processes=num_processes) as pool:
        pool.starmap(write_chunk, chunks)


if __name__ == "__main__":
    rows = 10**2
    filename = 'data/large_dataset.csv'
    # clear the target file
    open(filename, 'w').close()
    parallel_generate_csv(rows, filename)

    file_size_gb = os.path.getsize(filename) / 1e9
    print(f"Generated CSV file size: {file_size_gb:.2f} GB")
