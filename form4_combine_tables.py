# This script combines non-derivative and derivative tables.

import os
import pandas as pd
from datetime import datetime

# Get the date for file names
start_date = '2022-02-28'
end_date = '2022-03-13'

# Set the path to the tables
save_directory = r"C:\Users\atala\SEC\tables\\"

# Get the list of tables
files =  os.listdir(save_directory)

# separate the derivative and non-derivative files
nderiv_files = [f for f in files if '-nderiv' in f]
deriv_files = [f for f in files if '-deriv' in f]


# Combine the non-derivative tables
nderiv_tables = pd.DataFrame()

file_no = 1
row_count = 0
for file in nderiv_files:
    print(f'Processing: {file}  row count: {row_count}')
    table = pd.read_csv(save_directory + file, dtype=str)
    row_count += len(table.index)
    nderiv_tables = pd.concat([nderiv_tables, table], ignore_index=True, sort=False)
    # Save the table to a file if it exceeds 900000 rows
    if row_count > 900000:
        # true values to 1 and false values to 0
        nderiv_tables = nderiv_tables.replace("true", '1')
        nderiv_tables = nderiv_tables.replace("false", '0')
        # Change the cusip to string
        nderiv_tables.cusip = nderiv_tables.cusip.astype(str)
        # Save the table to excel file
        nderiv_tables.to_excel(save_directory + f'non_deriv_{start_date}-{end_date}-{file_no}.xlsx', index=False)

        # Reset the table
        row_count = 0
        nderiv_tables = pd.DataFrame()
        file_no += 1
# Save the last table

# true values to 1 and false values to 0
nderiv_tables = nderiv_tables.replace("true", '1')
nderiv_tables = nderiv_tables.replace("false", '0')
# Change the cusip to string
nderiv_tables.cusip = nderiv_tables.cusip.astype(str)
# Save the table to excel file
nderiv_tables.to_excel(save_directory + f'non_deriv_{start_date}-{end_date}-{file_no}.xlsx', index=False)

# Clean up the last table to release memory
nderiv_tables = pd.DataFrame()


# Combine the derivative tables
file_no = 1
row_count = 0
deriv_tables = pd.DataFrame()
for file in deriv_files:
    print(f'Processing: {file}  row count: {row_count}')
    table = pd.read_csv(save_directory + file, dtype=str)
    row_count += len(table.index)
    deriv_tables = pd.concat([deriv_tables, table], ignore_index=True, sort=False)
    # Save the table to a file if it exceeds 900000 rows
    if row_count > 900000:
       

        # true values to 1 and false values to 0
        deriv_tables = deriv_tables.replace("true", '1')
        deriv_tables = deriv_tables.replace("false", '0')
        # Change the cusip to string
        deriv_tables.cusip = deriv_tables.cusip.astype(str)
        # Save the table to excel file
        deriv_tables.to_excel(save_directory + f'deriv_{start_date}-{end_date}-{file_no}.xlsx', index=False)
        row_count = 0
        deriv_tables = pd.DataFrame()
        file_no += 1
# Save the last table  

 # true values to 1 and false values to 0
deriv_tables = deriv_tables.replace("true", '1')
deriv_tables = deriv_tables.replace("false", '0')
# Change the cusip to string
deriv_tables.cusip = deriv_tables.cusip.astype(str)
# Save the table to excel file
deriv_tables.to_excel(save_directory + f'deriv_{start_date}-{end_date}-{file_no}.xlsx', index=False)

deriv_tables = pd.DataFrame()
