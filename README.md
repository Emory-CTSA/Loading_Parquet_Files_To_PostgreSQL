### Processing `.parquet` Files and Loading to PostgreSQL

#### Overview

This Python script walks through a specified directory (and its subdirectories), processes all `.parquet` files, and loads their content into a PostgreSQL database. It processes the files in **chunks** (to handle large datasets efficiently), uses the **parent directory** of the `.parquet` file as the table name in PostgreSQL, and logs the **status** of each file (whether successfully processed or encountered an error) into an **output CSV file**.

#### Features:
- Process `.parquet` files in chunks for efficient memory usage.
- Use the parent directory of each `.parquet` file as the table name in PostgreSQL.
- Skip files that have already been successfully processed (based on a previous run).
- Log the processing status of each file in an output CSV file, including the table name, file name, file path, and status.

---

### Requirements

- Python 3.x
- The following Python packages:
  - `pandas`
  - `sqlalchemy`
  - `psycopg2` (PostgreSQL adapter for Python)

You can install the required packages using `pip`:

```bash
pip install pandas sqlalchemy psycopg2
```

---

### Usage

1. **Save the script**: Save the Python code provided in this repository into a `.py` file (e.g., `process_parquet_files.py`).
  
2. **Run the script**: Open a terminal and run the script using the following command:

```bash
python process_parquet_files.py /path/to/directory output_status.csv postgresql://username:password@localhost/database_name --chunk_size 1000
```

### Arguments:

- **`directory`**: (Required) Path to the directory where the `.parquet` files are located. This can be the top-level directory containing subdirectories.
- **`output_csv`**: (Required) Path to the output CSV file where the status of the processed files will be logged.
- **`db_url`**: (Required) PostgreSQL connection URL in the format: `postgresql://username:password@host:port/database_name`.
- **`--chunk_size`**: (Optional) Number of rows to process in each chunk when loading the `.parquet` file to PostgreSQL. Defaults to `1000`.

### Example Command:

```bash
python process_parquet_files.py /path/to/data output_status.csv postgresql://user:password@localhost/mydatabase --chunk_size 500
```

- This command will process `.parquet` files in `/path/to/data` (and subdirectories), load them in chunks of 500 rows to PostgreSQL, and log the results to `output_status.csv`.

---

### Output

- **CSV File (`output_status.csv`)**: The script logs the **status** of each file processed, along with the **table name**, **file name**, and **file path** in the output CSV. The columns in the CSV are:

| File Name             | File Path                                              | Table Name  | Status          |
|-----------------------|--------------------------------------------------------|-------------|-----------------|
| `sales-2024.parquet`  | `/path/to/data/sales/sales-2024.parquet`               | sales       | Success         |
| `employee-info.parquet` | `/path/to/data/employee/employee-info.parquet`       | employee    | Success         |
| `customer-data.parquet` | `/path/to/data/customer/customer-data.parquet`      | customer    | Error: Some error occurred |

- **Table Name**: The name of the PostgreSQL table (derived from the parent directory of the `.parquet` file).
- **File Path**: The full file path of the `.parquet` file.
- **Status**: The processing status of the file, such as "Success" or an error message if an issue occurred.

---

### How It Works

1. **Directory Traversal**: The script recursively searches through the given directory for `.parquet` files. It processes each file found.
  
2. **File Processing**:
   - For each `.parquet` file, the script reads it in **chunks** using the `chunksize` parameter (default is 1000 rows per chunk).
   - Each chunk is loaded into a PostgreSQL table, which is named based on the parent directory of the `.parquet` file (e.g., if the file is in `/path/to/data/sales/`, the table name will be `sales`).
   - If the file has already been successfully processed (based on previous status in the CSV file), it is skipped.

3. **Error Handling**: If an error occurs during processing (e.g., an issue connecting to the database or reading the file), the error message is logged in the CSV file.

---

### Example Directory Structure:

```
/path/to/data/
    ├── sales/
    │   └── sales-2024.parquet
    ├── employee/
    │   └── employee-info.parquet
    └── customer/
        └── customer-data.parquet
```

- For the above directory structure:
  - Files in the `/sales/` directory will be loaded into the `sales` table.
  - Files in the `/employee/` directory will be loaded into the `employee` table.
  - Files in the `/customer/` directory will be loaded into the `customer` table.

---

### Logging

- **Output CSV File (`output_status.csv`)**:
  - Contains information about the **file name**, **file path**, **table name**, and **status** of each `.parquet` file processed.
  - You can refer to this CSV file to track the processing status of each file.

---

### Handling Large Files

- The script reads `.parquet` files in **chunks** using the `chunksize` parameter to avoid memory overload when processing large datasets.
- This helps with efficient memory management when dealing with large `.parquet` files.

---

### PostgreSQL Setup

Ensure that:
1. The PostgreSQL database exists and is accessible using the provided connection URL (`db_url`).
2. The user has appropriate permissions to create tables and insert data.

- Example PostgreSQL URL format: `postgresql://username:password@localhost/database_name`
  - `username`: The PostgreSQL user.
  - `password`: The PostgreSQL user's password.
  - `localhost`: The PostgreSQL host (can be replaced with an IP address or domain if not local).
  - `database_name`: The name of the PostgreSQL database.

---

### Troubleshooting

- **Database Connection Issues**:
  - Ensure that your PostgreSQL server is running and accessible.
  - Verify that the connection URL is correct (including username, password, and database name).
  
- **File Processing Errors**:
  - If a file cannot be processed, the error message will be logged in the CSV file.
  - Common issues might include corrupted `.parquet` files or permission issues with the file.

---

### License

This code is provided as-is and is free for use under the **MIT License**.
