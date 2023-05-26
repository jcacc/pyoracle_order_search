import csv
import cx_Oracle
import time


lib_dir=r"C:\Oracle_Instant_Client\instantclient_21_3"
cx_Oracle.init_oracle_client(lib_dir=lib_dir)

# Database connection details
db_username = "username"
db_password = "password"
db_host = "x.x.x.x"
db_port = "xxxx"
db_service = "tns_name"

# Path to the CSV file containing order numbers
csv_file_path = "/path/to/file"

# Oracle table and column information
table_name = "table_name"
order_number_column = "ORDER_NO"

# Establish a connection to the Oracle database
dsn = cx_Oracle.makedsn(db_host, db_port, service_name=db_service)
connection = cx_Oracle.connect(db_username, db_password, dsn)

# Create a cursor to execute SQL queries
cursor = connection.cursor()

# Open the CSV file and read order numbers
with open(csv_file_path, "r") as csv_file:
    csv_reader = csv.reader(csv_file)
    order_numbers = [row[0] for row in csv_reader]

# store non-existing order numbers to a list
non_existing_order_numbers = []

# Iterate through order numbers and check their existence in the table
for order_number in order_numbers:
    # Prepare the SQL query with wildcard
    sql_query = f"SELECT COUNT(*) FROM {table_name} WHERE {order_number_column} LIKE :order_num"

    # Add wildcard character to the order number
    order_number_with_wildcard = f"{order_number}%"

    # Execute the query
    cursor.execute(sql_query, order_num=order_number_with_wildcard)
    result = cursor.fetchone()[0]

    if result > 0:
        print(f"Order number {order_number} exists in the table.")
    else:
        print(f"Order number {order_number} does not exist in the table.")
        non_existing_order_numbers.append(order_number)

    time.sleep(1)

# Close the cursor and database connection
cursor.close()
connection.close()


output_csv_file = "/path/to/file2"
with open(output_csv_file, "w", newline="") as output_file:
    csv_writer = csv.writer(output_file)
    csv_writer.writerow(["Non-Existing Order Numbers"])
    csv_writer.writerows([[order_number] for order_number in non_existing_order_numbers])

print(f"Non-existing order numbers written to: {output_csv_file}")
