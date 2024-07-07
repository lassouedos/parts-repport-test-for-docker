import tkinter as tk
from tkinter import ttk
from tkcalendar import Calendar, DateEntry
from datetime import datetime
import pandas as pd
from collections import deque

import os
import sqlite3
import config
import Function
import threading

# Function to format StartTime column
def format_start_time(df):
    df['StartTime'] = pd.to_datetime(df['StartTime'])
    df['StartTime'] = df['StartTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

def update_collected_files():
    file_list.delete(0, tk.END)
    processed_files.reverse()  # Reverse the order to display the last collected file at the top
    for file_name in processed_files:
        file_list.insert(tk.END, file_name)
    num_files_label.config(text=f"Number of files collected: {len(processed_files)}")

def collect_data_thread():
    # Disable the "Collect Data" button to prevent multiple clicks
    collect_button.config(state="disabled")

    # Get selected dates from calendar widgets
    date_debut = date_debut_cal.get_date().strftime("%Y%m%d")
    date_fin = date_fin_cal.get_date().strftime("%Y%m%d")
    
    # Get selected start and end times
    start_time = start_hour_spin.get() + ":" + start_minute_spin.get()
    end_time = end_hour_spin.get() + ":" + end_minute_spin.get()

    # Combine date and time strings into datetime objects
    start_datetime = datetime.combine(datetime.strptime(date_debut, "%Y%m%d"), datetime.strptime(start_time, "%H:%M").time())
    end_datetime = datetime.combine(datetime.strptime(date_fin, "%Y%m%d"), datetime.strptime(end_time, "%H:%M").time())
    
    # Get selected server names
    selected_servers = [server_listbox.get(idx) for idx in server_listbox.curselection()]
    
    # Create SQLite database for collected data
    db_file = f'collected_data{date_debut}_{date_fin}.db'
    conn = sqlite3.connect(db_file)
    
    # Create list to store collected dataframes
    collected_data_frames = []
    global processed_files
    processed_files = []
    
    # Collect data for each selected server
    for server in selected_servers:
        file_names = Function.collecter_noms_fichiers_bases_de_donnees(config.chemins_dossiers_pc[server], date_debut, date_fin)
        for file_name in file_names:
            processed_files.append(file_name)  # Add the processed file to the list
            # Extract data from database file
            with sqlite3.connect(file_name) as db_conn:
                cursor = db_conn.cursor()
                # Get all tables starting with "Prod_NXT" or "Prod_XPF"
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'Prod_NXT%' OR name LIKE 'Prod_XPF%')")
                tables = cursor.fetchall()
                for table in tables:
                    table_name = table[0]
                    query = f"SELECT DateTime, Module, Data FROM {table_name}"
                    df = pd.read_sql_query(query, db_conn)
                    # Extract StartTime from DateTime column
                    df['StartTime'] = pd.to_datetime(df['DateTime'])
                    # Filter data based on start and end datetime
                    df = df[(df['StartTime'] >= start_datetime) & (df['StartTime'] <= end_datetime)]
                    # Add Line_name and Type columns
                    df['Line_name'] = server
                    df['Type'] = 'XPF' if 'Prod_XPF' in table_name else 'NXT'
                    # Split the "Data" column into multiple columns using tabs
                    split_columns = ['Last_Panel_ID_produced', 'Conveyor_name', 'Recipe_name', 'Operator_name', 'Stage_no', 'Group_key', 'Position_no', 'Sub-Position_no', 'Parts_pickup_count', 'Error_parts_count', 'Error_rejected_parts_count', 'Rejected_parts_count', 'Dislodged_parts_count', 'NoPickup_Number_of_parts_not_used', 'Used_parts_count', 'Rescan_count', 'PartName', 'Unit_position_ID', 'FIDL', 'Module_Number', 'vide']
                    split_data = df['Data'].str.split('\t', expand=True)
                    split_data.columns = split_columns[:len(split_data.columns)]  # Ensure correct number of columns
                    # Concatenate split data with original DataFrame
                    df = pd.concat([df, split_data], axis=1)
                    # Drop the original "Data" column and other unwanted columns
                    df.drop(columns=['Data', 'Last_Panel_ID_produced', 'Conveyor_name', 'Operator_name', 'Group_key', 'Position_no', 'Sub-Position_no', 'Rescan_count', 'Unit_position_ID', 'Module_Number'], inplace=True)
                    # Convert specified columns to integers
                    int_columns = ['Stage_no', 'Parts_pickup_count', 'Error_parts_count', 'Error_rejected_parts_count', 'Rejected_parts_count', 'Dislodged_parts_count', 'NoPickup_Number_of_parts_not_used', 'Used_parts_count']
                    df[int_columns] = df[int_columns].astype(int)
                    # Append the DataFrame to the list
                    collected_data_frames.append(df)
                    
    # Concatenate collected dataframes
    if collected_data_frames:
        collected_data = pd.concat(collected_data_frames, ignore_index=True)
        # Define the desired column order
        desired_columns = ["Line_name", "Type", "Module", "StartTime", "Recipe_name", "Stage_no", "Parts_pickup_count", "Error_parts_count", "Error_rejected_parts_count", "Rejected_parts_count", "Dislodged_parts_count", "NoPickup_Number_of_parts_not_used", "Used_parts_count", "PartName", "FIDL"]
        # Reorder columns in the DataFrame
        collected_data = collected_data.reindex(columns=desired_columns)
        # Format StartTime column
        collected_data = format_start_time(collected_data)
        # Save collected data to SQLite database
        collected_data.to_sql('collected_data', conn, if_exists='replace', index=False)
    
    # Close connection
    conn.close()
    
    # Execute the SQL query to aggregate the data
    aggregated_data = aggregate_data(db_file)

    # Save the aggregated data to a CSV file
    aggregated_data.to_csv(f'Report_generate{date_debut}_{date_fin}.csv', index=False)

    # Enable the "Collect Data" button after data collection is complete
    collect_button.config(state="normal")

    # Schedule the update of collected files and their count
    root.after(100, update_collected_files)

# Function to aggregate data using SQL query
def aggregate_data(db_file):
    conn = sqlite3.connect(db_file)
    query = """
    SELECT 
        Line_name,
        Type,
        Module,
        MIN(StartTime) AS StartTime,
        Recipe_name,
        MIN(Stage_no) AS Stage_no,
        SUM(Parts_pickup_count) AS Parts_pickup_count,
        SUM(Error_parts_count) AS Error_parts_count,
        SUM(Error_rejected_parts_count) AS Error_rejected_parts_count,
        SUM(Rejected_parts_count) AS Rejected_parts_count,
        SUM(Dislodged_parts_count) AS Dislodged_parts_count,
        SUM(NoPickup_Number_of_parts_not_used) AS NoPickup_Number_of_parts_not_used,
        SUM(Used_parts_count) AS Used_parts_count,
        PartName,
        FIDL 
    FROM collected_data 
    GROUP BY Line_name,Type,Module,Recipe_name,FIDL, PartName  ;
    """
    aggregated_data = pd.read_sql_query(query, conn)
    conn.close()
    return aggregated_data

# Create the main window
root = tk.Tk()
root.title("Data Collection")
root.geometry("900x600")

# Frame for the date and time inputs
datetime_frame = ttk.LabelFrame(root, text="Select Date and Time Range")
datetime_frame.place(relx=0.5, rely=0.05, relwidth=0.7, relheight=0.3, anchor="n")

# Calendar widgets for date selection
date_debut_label = ttk.Label(datetime_frame, text="Start Date:")
date_debut_label.grid(column=0, row=0, padx=(0, 10), pady=5)
date_debut_cal = DateEntry(datetime_frame, width=12, date_pattern="yyyy-mm-dd")
date_debut_cal.grid(column=1, row=0, padx=(0, 10), pady=5)

date_fin_label = ttk.Label(datetime_frame, text="End Date:")
date_fin_label.grid(column=2, row=0, padx=(0, 10), pady=5)
date_fin_cal = DateEntry(datetime_frame, width=12, date_pattern="yyyy-mm-dd")
date_fin_cal.grid(column=3, row=0, padx=(0, 10), pady=5)

# Spinboxes for selecting start time
start_hour_label = ttk.Label(datetime_frame, text="Start Hour:")
start_hour_label.grid(column=0, row=1, padx=(0, 10), pady=5)
start_hour_spin = ttk.Spinbox(datetime_frame, from_=0, to=23, width=2, wrap=True)
start_hour_spin.grid(column=1, row=1, padx=(0, 10), pady=5)
start_minute_label = ttk.Label(datetime_frame, text="Start Minute:")
start_minute_label.grid(column=2, row=1, padx=(0, 10), pady=5)
start_minute_spin = ttk.Spinbox(datetime_frame, from_=0, to=59, width=2, wrap=True)
start_minute_spin.grid(column=3, row=1, padx=(0, 10), pady=5)

# Spinboxes for selecting end time
end_hour_label = ttk.Label(datetime_frame, text="End Hour:")
end_hour_label.grid(column=0, row=2, padx=(0, 10), pady=5)
end_hour_spin = ttk.Spinbox(datetime_frame, from_=0, to=23, width=2, wrap=True)
end_hour_spin.grid(column=1, row=2, padx=(0, 10), pady=5)
end_minute_label = ttk.Label(datetime_frame, text="End Minute:")
end_minute_label.grid(column=2, row=2, padx=(0, 10), pady=5)
end_minute_spin = ttk.Spinbox(datetime_frame, from_=0, to=59, width=2, wrap=True)
end_minute_spin.grid(column=3, row=2, padx=(0, 10), pady=5)

# Frame for server selection
server_frame = ttk.LabelFrame(root, text="Select Server(s)")
server_frame.place(relx=0.5, rely=0.4, relwidth=0.7, relheight=0.2, anchor="n")

# Listbox to display server names with scrollbar
server_listbox = tk.Listbox(server_frame, selectmode="extended", height=len(config.chemins_dossiers_pc))
for server in config.chemins_dossiers_pc:
    server_listbox.insert(tk.END, server)
server_listbox.pack(side="left", fill="both", expand=True)

server_scrollbar = ttk.Scrollbar(server_frame, orient="vertical", command=server_listbox.yview)
server_scrollbar.pack(side="right", fill="y")

server_listbox.config(yscrollcommand=server_scrollbar.set)

# Button to start data collection
collect_button = ttk.Button(root, text="Collect Data", command=collect_data_thread)
collect_button.place(relx=0.5, rely=0.6, anchor="n")

# Frame for collected files
file_frame = ttk.LabelFrame(root, text="Collected Files")
file_frame.place(relx=0.5, rely=0.75, relwidth=0.8, relheight=0.2, anchor="n")

# Listbox to display collected files with scrollbar
file_list = tk.Listbox(file_frame, selectmode="extended")
file_list.pack(side="left", fill="both", expand=True)

file_scrollbar = ttk.Scrollbar(file_frame, orient="vertical", command=file_list.yview)
file_scrollbar.pack(side="right", fill="y")

file_list.config(yscrollcommand=file_scrollbar.set)

# Label to display the number of collected files
num_files_label = ttk.Label(root, text="")
num_files_label.place(relx=0.5, rely=0.95, anchor="n")

# Initialize the list of processed files
processed_files = []

# Call update_collected_files to display the collected files initially
update_collected_files()

root.mainloop()