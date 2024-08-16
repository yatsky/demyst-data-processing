# Intro
The main script only uses standard libraries.

To run `gen_fake_data.py`, please install `faker`.

# Usage
Use `gen_fake_data.py` to generate fake data.

Put required file in `data/` and rename to `large_dataset.csv`.
Run the following command to hash the dataset.
``` shell
python main.py
```

The hashed output file will be `data/output/anonymized_large_dataset.csv`.

