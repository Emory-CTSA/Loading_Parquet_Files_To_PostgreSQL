import os
import argparse
import csv
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Run the script from the command line with the necessary arguments:
#     python
#     process_parquet_files.py / path / to / your / main / directory
#     output_status.csv
#     postgresql: // user: password @ localhost / mydatabase - -chunk_size
#     500


def process_parquet_file(file_path, db_url, chunk_size):
    """
    Process the .parquet file in chunks, load each chunk to PostgreSQL, and return a status.
    The table name will be set based on the parent directory name (one level above the file).
    """
    try:
        # Read the .parquet file in chunks and process each chunk
        print(f"Processing file: {file_path}")

        # Extract the parent directory name (one level above the file) to use as table name
        parent_dir = os.path.basename(os.path.dirname(file_path))

        # Sanitize the parent directory name to ensure it's a valid PostgreSQL table name
        table_name = parent_dir.replace('-', '_').replace(' ', '_')

        # Create a SQLAlchemy engine to connect to the PostgreSQL database
        engine = create_engine(db_url)

        # Read the parquet file in chunks
        for chunk in pd.read_parquet(file_path, chunksize=chunk_size):
            # Load the current chunk into the PostgreSQL table
            chunk.to_sql(table_name, con=engine, if_exists='append', index=False)
            print(f"Inserted chunk of {len(chunk)} rows into table {table_name}.")

        return 'Success', table_name
    except Exception as e:
        # If there is an error, return failure status and error message
        print(f"Error processing {file_path}: {e}")
        return f'Error: {str(e)}', None


def load_processed_files(output_csv):
    """
    Load the existing processed files and their statuses from the CSV file.
    Returns a dictionary where the key is the file path and the value is the status.
    """
    processed_files = {}

    if os.path.exists(output_csv):
        with open(output_csv, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                processed_files[row['File Path']] = row['Status']

    return processed_files


def find_parquet_files(directory, output_csv, db_url, chunk_size):
    """
    Walk through the directory, find all .parquet files, and process each if it hasn't been processed successfully.
    Results will be written to the provided output CSV file.
    """
    # Load existing processed files from the CSV
    processed_files = load_processed_files(output_csv)

    # Open the output CSV file in append mode
    with open(output_csv, mode='a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['File Name', 'File Path', 'Table Name', 'Status']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # If the CSV is empty (no header), write the header row
        if os.path.getsize(output_csv) == 0:
            writer.writeheader()

        # Traverse the directory tree starting from the root directory
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith('.parquet'):
                    file_path = os.path.join(root, file)
                    file_name = file  # Extract the file name

                    # If the file has been processed successfully, skip it
                    if file_path in processed_files and processed_files[file_path] == 'Success':
                        print(f"Skipping already processed file: {file_path}")
                        continue

                    # Process the .parquet file and get the status
                    status, table_name = process_parquet_file(file_path, db_url, chunk_size)

                    # Write the file name, file path, table name, and status to the CSV file
                    writer.writerow(
                        {'File Name': file_name, 'File Path': file_path, 'Table Name': table_name, 'Status': status})


def main():
    # Create an ArgumentParser object to handle command-line arguments
    parser = argparse.ArgumentParser(
        description='Find all .parquet files in the specified directory, process them in chunks, and load to PostgreSQL.')

    # Add argument for the directory path
    parser.add_argument('directory', type=str, help='Path to the main directory to search for .parquet files')

    # Add argument for the output CSV file
    parser.add_argument('output_csv', type=str, help='Path to the output CSV file to log processing statuses')

    # Add argument for PostgreSQL database connection URL
    parser.add_argument('db_url', type=str,
                        help='PostgreSQL connection URL (e.g., postgresql://user:password@localhost/dbname)')

    # Add argument for the chunk size (default to 1000 rows per chunk)
    parser.add_argument('--chunk_size', type=int, default=1000,
                        help='Number of rows per chunk to process (default is 1000)')

    # Parse the command-line arguments
    args = parser.parse_args()

    # Call the function to find and process parquet files, and write statuses to CSV
    find_parquet_files(args.directory, args.output_csv, args.db_url, args.chunk_size)


if __name__ == "__main__":
    # Run the main function
    main()
