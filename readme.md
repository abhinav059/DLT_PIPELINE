
---
# Data Pipeline for CSV to PostgreSQL with CRUD and Parquet Export

This project demonstrates a simple data pipeline that:
1. Loads two different CSV files into PostgreSQL.
2. Performs basic CRUD operations.
3. Exports the data to Parquet format.

## Features
- Load **Employee** and **Transaction** data from local CSV files.
- Create tables in PostgreSQL and perform **CRUD** operations (Create, Read, Update, Delete).
- Save the processed data as **Parquet** files.

## Prerequisites
- Python 3.x
- PostgreSQL
- Libraries: `dlt`, `pandas`

## Setup
1. Clone the repository:
   ```bash
   git clone git@github.com:abhinav059/DLT_PIPELINE.git
   cd your-repo
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## How to Run
1. **Run employee data pipeline**:
   ```bash
   python extract_from_local.py
   ```
2. **Run transaction data pipeline**:
   ```bash
   python extract_load_transaction.py
   ```

## Summary
This project loads data from CSV files, processes it in PostgreSQL with CRUD operations, and saves it as Parquet files for easy storage.

---