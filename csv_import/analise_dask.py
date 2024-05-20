import dask.dataframe as dd
import pandas as pd
import time
import gc

def load_and_clean_data(file_path, dtype):
    try:
        start_load_time = time.time()
        print("Loading data...")
        df = dd.read_csv(file_path, dtype=str)  # Load all data as strings initially
        print("Data loaded successfully.")
        end_load_time = time.time()
        load_duration = end_load_time - start_load_time

        start_clean_time = time.time()
        print("Cleaning data...")
        for col in dtype:
            df[col] = dd.to_numeric(df[col], errors='coerce')  # Convert columns to numeric, coercing errors to NaN
        df = df.dropna().astype(dtype)  # Drop rows with NaN and ensure the data types are correct
        df = df.compute()  # Convert Dask DataFrame to Pandas DataFrame for in-memory operations
        print("Data cleaned successfully.")
        end_clean_time = time.time()
        clean_duration = end_clean_time - start_clean_time

        return df, load_duration, clean_duration
    except Exception as e:
        print(f"An error occurred: {e}")
        return None, None, None

def sum_counts(counts):
    return counts.sum()

def count_elements(arr):
    return len(arr)

def groupby_sum_age(df):
    grouped = df.groupby('Age')['count'].sum()
    return grouped.index.values, grouped.values

def filter_age_gt_zero(df):
    return df[df['Age'] > 0]

def select_year_and_count(df):
    filtered_df = df[df['count'] > 0]
    return list(zip(filtered_df['Year'], filtered_df['count']))

def main():
    file_path = 'Data8317.csv'
    dtype = {
        'Year': 'int32',
        'Age': 'int32',
        'Ethnic': 'int32',
        'Sex': 'int32',
        'Area': 'int32',
        'count': 'int32'
    }
    current_time = time.strftime("%Y%m%d%H%M%S")

    df, load_duration, clean_duration = load_and_clean_data(file_path, dtype)

    if df is None:
        return

    ages = df['Age']
    counts = df['count']
    years = df['Year']

    operations = [
        ('Sum', lambda: sum_counts(counts)),
        ('Count', lambda: count_elements(counts)),
        ('Groupby Age Sum', lambda: groupby_sum_age(df)),
        ('Filter Age > 0', lambda: filter_age_gt_zero(df)),
        ('Select Year and Count', lambda: select_year_and_count(df))
    ]

    report_data = []
    report_data.append(["Load", load_duration])
    report_data.append(["Clean", clean_duration])
    with open(f'dask_{current_time}_resultados.txt', 'w') as f:
        for op_name, op in operations:
            start_time = time.time()
            result = op()
            result = str(result)[:500]
            duration = time.time() - start_time
            print(f"{op_name} completed in {duration} seconds.")
            f.write(f"{op_name} completed in {duration} seconds.\n\n")
            f.write(result)
            gc.collect()
            report_data.append([op_name, duration])

    report_df = pd.DataFrame(report_data, columns=['Operation', 'Time (seconds)'])
    report_df.to_csv(f'results_dask_{current_time}_report.txt', index=False)

if __name__ == '__main__':
    main()
