import pandas as pd
import time
import gc
import numpy as np

def load_and_clean_data(file_path, dtype):
    try:
        start_load_time = time.time()
        print("Loading data...")
        df = pd.read_csv(file_path, low_memory=False)
        print("Data loaded successfully.")
        end_load_time = time.time()
        load_duration = end_load_time - start_load_time

        start_clean_time = time.time()
        print("Cleaning data...")
        for col in dtype:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df.dropna(inplace=True)
        df = df.astype(dtype)
        print("Data cleaned successfully.")
        end_clean_time = time.time()
        clean_duration = end_clean_time - start_clean_time

        return df, load_duration, clean_duration
    except Exception as e:
        print(f"An error occurred: {e}")
        return None, None, None

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

    operations = [
        ('Sum', lambda df: df.sum()),
        ('Count', lambda df: df.count()),
        ('Groupby Age Sum', lambda df: df.groupby('Age').sum()),
        ('Filter Age > 0', lambda df: df[df['Age'] > 0]),
        ('Select Year and Count', lambda df: df[['Year', 'count']])
    ]

    report_data = []
    report_data.append(["Load", load_duration])
    report_data.append(["Clean", clean_duration])
    with open(f'pandas_{current_time}_resultados.txt', 'w') as f:
        for op_name, op in operations:
            start_time = time.time()
            result = op(df)
            result = str(result[:50])
            duration = time.time() - start_time
            print(f"{op_name} completed in {duration} seconds.")
            f.write(f"{op_name} completed in {duration} seconds.\n")
            f.write(result)
            gc.collect()
            report_data.append([op_name, duration])

    report_df = pd.DataFrame(report_data, columns=['Operation', 'Time (seconds)'])
    report_df.to_csv(f'results_pandas_{current_time}_report.txt', index=False)

if __name__ == '__main__':
    main()