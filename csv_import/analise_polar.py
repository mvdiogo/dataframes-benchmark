import polars as pl
import time
import gc

def load_and_clean_data(file_path, dtype):
    try:
        start_load_time = time.time()
        print("Loading data...")
        df = pl.read_csv(file_path, ignore_errors=True, null_values=['CMB07618'])
        print("Data loaded successfully.")
        end_load_time = time.time()
        load_duration = end_load_time - start_load_time

        start_clean_time = time.time()
        print("Cleaning data...")

        # Correct way to cast columns in Polars
        df = df.with_columns([pl.col(col).cast(dtype[col], strict=False) for col in dtype])

        df = df.drop_nulls()

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
        'Year': pl.Int32,
        'Age': pl.Int32,
        'Ethnic': pl.Int32,
        'Sex': pl.Int32,
        'Area': pl.Int32,
        'count': pl.Int32
    }
    current_time = time.strftime("%Y%m%d%H%M%S")

    df, load_duration, clean_duration = load_and_clean_data(file_path, dtype)

    if df is None:
        return

    operations = [
        ('Sum', lambda df: df.select(pl.all().sum())),
        ('Count', lambda df: df.select(pl.all().count())),
        ('Groupby Age Sum', lambda df: df.group_by('Age').agg(pl.all().sum())),
        ('Filter Age > 0', lambda df: df.filter(pl.col('Age') > 0)),
        ('Select Year and Count', lambda df: df.select(['Year', 'count']))
    ]

    report_data = []
    report_data.append(["Load", load_duration])
    report_data.append(["Clean", clean_duration])
    with open(f'polar_{current_time}_resultados.txt', 'w') as f:
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

    report_df = pl.DataFrame(report_data, schema=['Operation', 'Time (seconds)'])
    report_df.write_csv(f'results_polars_{current_time}_report.txt')

if __name__ == '__main__':
    main()