import dlt
from dlt.sources.filesystem import filesystem, read_csv
import pandas as pd

def load_source_data() -> None:
    # Load CSV file from local filesystem
    try:
        print("Loading CSV file...")
        source = filesystem(file_glob="/Users/abhinavkumar/Downloads/employees.csv") | read_csv()

        # Create the DLT pipeline for PostgreSQL with a valid connection string
        print("Creating DLT pipeline...")
        pipeline = dlt.pipeline(
            pipeline_name="load_source_data",
            destination="postgres",
            dataset_name="employees"  # The table name where data will be loaded
        )

        # Run the pipeline and load the data into PostgreSQL
        print("Running the pipeline...")
        load_info = pipeline.run(source.with_name("employee-dataset"), write_disposition="replace")
        print("Data load complete:", load_info)

    except Exception as e:
        print(f"Error while loading data from source: {e}")


def create_new_table() -> None:
    try:
        # Create a DLT pipeline for PostgreSQL
        print("Creating DLT pipeline for table creation...")
        pipeline = dlt.pipeline(
            pipeline_name="create_new_table",
            destination="postgres",
            dataset_name="employees_newdata"
        )

        # Use the pipeline's SQL client to execute SQL commands
        print("Creating new table...")
        with pipeline.sql_client() as client:
            client.execute_sql("""
                CREATE TABLE IF NOT EXISTS employee_details (
                    employee_id INT PRIMARY KEY,
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    email VARCHAR(255),
                    phone_number VARCHAR(20),
                    hire_date DATE,
                    job_id VARCHAR(50),
                    salary DECIMAL(10, 2),
                    commission_pct DECIMAL(5, 2),
                    manager_id INT,
                    department_id INT
                );
            """)
            print("New table 'employee_details' created successfully!")
    except Exception as e:
        print(f"Error creating table: {e}")

def add_data() -> None:
    try:
        print("Creating DLT pipeline for data insertion...")
        pipeline = dlt.pipeline(
            pipeline_name="add_data_pipeline",
            destination="postgres",
            dataset_name="employees_newdata"
        )
        # Adding data to the table - "employee_details"
        print("Inserting new data into 'employee_details' table...")
        with pipeline.sql_client() as client:
            client.execute_sql(""" 
                INSERT INTO employee_details (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id)
                VALUES (901, 'akash', 'kumar', 'kumarakash@gmail.com', '900.507.9833', '2022-08-01', 'lead_eng', 1600, NULL, 13, 60);
            """)
            print("Data inserted successfully!")
    except Exception as e:
        print(f"Error inserting data: {e}")

def read_data() -> None:
    try:
        print("Creating DLT pipeline for data reading...")
        pipeline = dlt.pipeline(
            pipeline_name="read_data_pipeline",
            destination="postgres",
            dataset_name="employees_newdata"
        )
        # Reading the data from the "employee_details" table
        print("Reading data from 'employee_details' table...")
        with pipeline.sql_client() as client:
            with client.cursor() as cursor:
                cursor.execute("SELECT * FROM employee_details;")
                rows = cursor.fetchall()
                if rows:
                    print("Data from 'employee_details':")
                    for row in rows:
                        print(row)
                else:
                    print("No data found in the table.")
    except Exception as e:
        print(f"Error reading data: {e}")

def update_data() -> None:
    try:
        print("Creating DLT pipeline for data update...")
        pipeline = dlt.pipeline(
            pipeline_name="update_data_pipeline",
            destination="postgres",
            dataset_name="employees_newdata"
        )
        # Updating data in the "employee_details" table
        print("Updating data in 'employee_details' table...")
        with pipeline.sql_client() as client:
            client.execute_sql("""
                UPDATE employee_details
                SET email = 'donald@hotmail.com'
                WHERE employee_id = 198;
            """)
            print("Data updated successfully!")
    except Exception as e:
        print(f"Error updating data: {e}")

def delete_data() -> None:
    try:
        print("Creating DLT pipeline for data deletion...")
        pipeline = dlt.pipeline(
            pipeline_name="delete_data_pipeline",
            destination="postgres",
            dataset_name="employees_newdata"
        )
        # Deleting data from the "employee_details" table
        print("Deleting rows where department_id is 50 from 'employee_details' table...")
        with pipeline.sql_client() as client:
            delete_query = "DELETE FROM employee_details WHERE department_id = 50;"
            rows_deleted = client.execute_sql(delete_query)

            print(f"Deleted {rows_deleted} rows where department_id is 50.")
    
    except Exception as e:
        print(f"Error deleting data: {e}")

def save_as_parquet() -> None:
    # This function reads the data from the PostgreSQL database and saves it in Parquet format
    try:
        print("Creating DLT pipeline for data reading...")
        pipeline = dlt.pipeline(
            pipeline_name="read_data_pipeline",
            destination="postgres",
            dataset_name="employees_newdata"
        )

        print("Reading data from 'employee_details' table...")
        with pipeline.sql_client() as client:
            query = "SELECT * FROM employee_details;"
            rows = client.execute_sql(query)
            if rows:
                columns = ['employee_id', 'first_name', 'last_name', 'email', 'phone_number', 'hire_date', 'job_id', 'salary', 'commission_pct', 'manager_id', 'department_id'] 
                df = pd.DataFrame(rows, columns=columns)

                print("Data from 'employee_details':")
                print(df)

                # Save DataFrame to Parquet format
                parquet_file_path = "/Users/abhinavkumar/Downloads/employee_details.parquet"
                df.to_parquet(parquet_file_path, index=False)  
                print(f"Data saved to Parquet format at: {parquet_file_path}")
            else:
                print("No data found in the table.")

    except Exception as e:
        print(f"Error saving as Parquet: {e}")

# Operations on Employee data
# load_source_data()  # Load data from the CSV file
# create_new_table()  # Create the 'employee_details' table
add_data()          # Insert new data into the table
read_data()         # Read the data from the table
# update_data()       # Update specific data in the table
# delete_data()     # Uncomment to delete rows from the table
save_as_parquet()    # Save the table data to Parquet format



