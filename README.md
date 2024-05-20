# Data Processing Performance Comparison

This repository compares the performance of different data processing libraries in Python: Pandas, Dask, Numba, Polars, and Spark (PySpark).

## Objectives

* Benchmark the performance of these libraries for common data processing tasks.
* Analyze the strengths and weaknesses of each library.
* Provide insights for choosing the most appropriate library based on specific requirements.

## Setup

1. **Create a virtual environment:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. **Install required packages:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Unzip Data8317.csv.zip**

* Go to the csv_import directory and unzip the "Data8317.csv.zip" file.


## Usage

* Each script (e.g., `analise_dask.py`, `analise_pandas.py`) contains the implementation of the benchmarks for a specific library.
* Run the scripts individually to execute the benchmarks.
* The results are saved to text files (e.g., `results_pandas_20240520102004_report.txt`).

## Results

The performance results for each library are documented in the corresponding output files.

## Contributing

Contributions are welcome! Feel free to:

* Improve the benchmarking scripts.
* Add additional libraries to the comparison.
* Provide insights and analysis based on the results.

## Todo

* Fix some code for better comparison
* Use s3, Iceberg and diferent files import types.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.