import time
import gc
import csv
from chromadb.config import Settings
from chromadb.utils import embedding_functions
from chromadb import Client  # Correct import


def load_and_clean_data(file_path, dtype):
    try:
        start_load_time = time.time()
        print("Loading data...")
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            data = [row for row in reader]
        print("Data loaded successfully.")
        end_load_time = time.time()
        load_duration = end_load_time - start_load_time

        start_clean_time = time.time()
        print("Cleaning data...")
        for col in dtype:
            for row in data:
                row[col] = int(row[col]) if row[col].strip().isdigit() else None
        data = [row for row in data if all(row[col] is not None for col in dtype)]
        print("Data cleaned successfully.")
        end_clean_time = time.time()
        clean_duration = end_clean_time - start_clean_time

        return data, load_duration, clean_duration
    except Exception as e:
        print(f"An error occurred: {e}")
        return None, None, None

def main():
    file_path = 'Data8317.csv'
    dtype = ['Year', 'Age', 'Ethnic', 'Sex', 'Area', 'count']
    current_time = time.strftime("%Y%m%d%H%M%S")

    data, load_duration, clean_duration = load_and_clean_data(file_path, dtype)

    if data is None:
        return

    # Connect to ChromaDB
    client = Client(Settings(
        chroma_db_impl="duckdb+parquet",
        persist_directory=".chromadb" 
    ))

    # Create a collection
    collection = client.get_or_create_collection(name="census_data")

    # Prepare data for embedding
    documents = [str(row) for row in data] 
    embeddings = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-mpnet-base-v2").embed(documents)

    # Add data to ChromaDB
    collection.add(
        documents=documents,
        embeddings=embeddings,
        metadatas=[{"row": row} for row in data] 
    )

    # Define your operations using ChromaDB's API
    def sum_op():
        results = collection.query(query_texts=["census data"], n_results=len(data))
        total_count = sum([int(result.metadata['row']['count']) for result in results])
        return total_count

    def count_op():
        return collection.count()

    def groupby_age_sum_op():
        age_counts = {}
        results = collection.query(query_texts=["census data"], n_results=len(data))
        for result in results:
            age = result.metadata['row']['Age']
            count = int(result.metadata['row']['count'])
            if age in age_counts:
                age_counts[age] += count
            else:
                age_counts[age] = count
        return age_counts

    # Example: Filter by 'Age' > 0
    def filter_age_gt_0_op():
        filtered_data = []
        results = collection.query(query_texts=["census data"], n_results=len(data))
        for result in results:
            if int(result.metadata['row']['Age']) > 0:
                filtered_data.append(result.metadata['row'])
        return filtered_data

    # Example: Select 'Year' and 'count'
    def select_year_and_count_op():
        year_count_data = []
        results = collection.query(query_texts=["census data"], n_results=len(data))
        for result in results:
            year_count_data.append({
                'Year': result.metadata['row']['Year'],
                'count': result.metadata['row']['count']
            })
        return year_count_data

    operations = [
        ('Sum', sum_op),
        ('Count', count_op),
        ('Groupby Age Sum', groupby_age_sum_op),
        ('Filter Age > 0', filter_age_gt_0_op),
        ('Select Year and Count', select_year_and_count_op)
    ]

    report_data = []
    report_data.append(["Load", load_duration])
    report_data.append(["Clean", clean_duration])
    with open(f'pika_{current_time}_resultados.txt', 'w') as f:
        for op_name, op in operations:
            start_time = time.time()
            result = op()
            result = str(result[:50])
            duration = time.time() - start_time
            print(f"{op_name} completed in {duration} seconds.")
            f.write(f"{op_name} completed in {duration} seconds.\n")
            f.write(result)
            gc.collect()
            report_data.append([op_name, duration])

    report_filename = f'results_chromadb_{current_time}_report.txt'
    with open(report_filename, 'w') as report_file:
        writer = csv.writer(report_file)
        writer.writerow(['Operation', 'Time (seconds)'])
        writer.writerows(report_data)

if __name__ == '__main__':
    main()