import dlt
from dlt.sources.filesystem import filesystem, read_csv
import pandas as pd

def extract_transaction() -> None:
    # file_path = "/Users/abhinavkumar/Downloads/fee_transaction.csv"
    try:

        # Load the specified CSV file
        source = filesystem(file_glob="/Users/abhinavkumar/Downloads/fee_transaction.csv") | read_csv()

        # Create the DLT pipeline for PostgreSQL
        pipeline = dlt.pipeline(
            pipeline_name="extract_transaction_data",
            destination="postgres",
            dataset_name="fee_transactions"
        )

        # Run the pipeline and load the data into PostgreSQL
        load_info = pipeline.run(source.with_name("fee_transaction_dataset"), write_disposition="replace")
        print("Transaction data load complete:", load_info)

    except Exception as e:
        print(f"Error while loading transaction data from source: {e}")

def save_as_parquet() -> None:
    try:
        print("Creating DLT pipeline for data reading...")
        pipeline = dlt.pipeline(
            pipeline_name="read_data_pipeline",
            destination="postgres",
            dataset_name="fee_transactions"
        )

        print("Reading data from 'fee_transaction_dataset' table...")
        with pipeline.sql_client() as client:
            # Modify the query as per your table name and structure
            query = "SELECT * FROM fee_transaction_dataset;"
            rows = client.execute_sql(query)
            
            if rows:
                # Dynamically fetch column names from the result
                df = pd.DataFrame(rows)

                # If the query returns column headers, extract them
                if not df.empty:
                    print("Data from 'fee_transaction_dataset':")
                    print(df)

                    # Save DataFrame to Parquet format
                    parquet_file_path = "/Users/abhinavkumar/Downloads/fee_transaction.parquet"
                    df.to_parquet(parquet_file_path, index=False)
                    print(f"Data saved to Parquet format at: {parquet_file_path}")
                else:
                    print("No data found in the table.")

    except Exception as e:
        print(f"Error saving as Parquet: {e}")




# extract_transaction()
save_as_parquet()
