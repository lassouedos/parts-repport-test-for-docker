import configparser
import os
from datetime import datetime
from tqdm import tqdm
import sqlite3
import pandas as pd
from PyQt5.QtWidgets import QMessageBox
from PyQt5.QtWidgets import QApplication,QMessageBox,QDialog, QLabel, QVBoxLayout, QProgressBar, QApplication
from PyQt5.QtCore import Qt

# Function to read configuration from config.ini
def read_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config



# Function to collect database files
def collecter_noms_fichiers_bases_de_donnees(chemin_dossier_pc, start_date, end_date):
    file_names = []
    if os.path.exists(chemin_dossier_pc):
        for nom_fichier in os.listdir(chemin_dossier_pc):
            if nom_fichier.endswith("[COUNT].db"):
                date_fichier = nom_fichier.split("_")[0]
                if start_date <= date_fichier <= end_date:
                    fichier_db = os.path.join(chemin_dossier_pc, nom_fichier)
                    file_names.append(fichier_db)
    return file_names




# Function to process collected data
def process_collected_data(start_date, end_date, start_time, end_time, servers):
    # Read configuration from config.ini
    config = read_config()
    collected_data_frames = []
    

    # Step 1: Iterate through each server
    for server in servers:
        # Step 2: Collect database files
        server_path = config['Paths'][server]
        collected_files = collecter_noms_fichiers_bases_de_donnees(server_path, start_date, end_date)
        

        # Step 3: For each database file
        for file_name in collected_files:
            try :
                # Step 4: Get all tables starting with "Prod_NXT" or "Prod_XPF"
                with sqlite3.connect(file_name) as db_conn:
                    cursor = db_conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'Prod_NXT%' OR name LIKE 'Prod_XPF%')")
                    tables = cursor.fetchall()
                    
                    # Step 5: For each table
                    for table in tables:
                        table_name = table[0]
                        # Step 6: Execute SQL query to select data
                        query = f"SELECT DateTime, Module, Data FROM {table_name} WHERE DateTime >= ? AND DateTime <= ?"
                        start_datetime = datetime.strptime(f"{start_date} {start_time}", "%Y%m%d %H:%M")
                        end_datetime = datetime.strptime(f"{end_date} {end_time}", "%Y%m%d %H:%M")

                        params = (start_datetime, end_datetime)
                        df = pd.read_sql_query(query, db_conn, params=params)
                        
                        # Step 7: Add Line_name and Type columns
                        df['Line_name'] = server
                        df['Type'] = 'XPF' if 'Prod_XPF' in table_name else 'NXT'
                        
                        # Step 8: Split the "Data" column into multiple columns using tabs
                        split_columns = ['Last_Panel_ID_produced', 'Conveyor_name', 'Recipe_name', 'Operator_name', 'Stage_no', 'Group_key', 'Position_no', 'Sub-Position_no', 'Parts_pickup_count', 'Error_parts_count', 'Error_rejected_parts_count', 'Rejected_parts_count', 'Dislodged_parts_count', 'NoPickup_Number_of_parts_not_used', 'Used_parts_count', 'Rescan_count', 'PartName', 'Unit_position_ID', 'FIDL', 'Module_Number', 'vide']
                        split_data = df['Data'].str.split('\t', expand=True)
                        split_data.columns = split_columns[:len(split_data.columns)]  # Ensure correct number of columns
                        # Concatenate split data with original DataFrame
                        df = pd.concat([df, split_data], axis=1)
                        # Drop the original "Data" column and other unwanted columns
                        columns_to_drop = ['Data', 'Last_Panel_ID_produced', 'Conveyor_name', 'Operator_name', 'Group_key', 'Sub-Position_no', 'Rescan_count', 'Unit_position_ID', 'Module_Number']
                        columns_to_drop = [col for col in columns_to_drop if col in df.columns]  # Filter existing columns
                        df.drop(columns=columns_to_drop, inplace=True)
                        # Convert specified columns to integers
                        int_columns = ['Position_no','Stage_no', 'Parts_pickup_count', 'Error_parts_count', 'Error_rejected_parts_count', 'Rejected_parts_count', 'Dislodged_parts_count', 'NoPickup_Number_of_parts_not_used', 'Used_parts_count']
                        df[int_columns] = df[int_columns].astype(int)
                        # Add StartTime column with proper formatting
                        df['StartTime'] = pd.to_datetime(df['DateTime']).dt.strftime('%Y-%m-%d %H:%M')
                        # Append the DataFrame to the list
                        collected_data_frames.append(df)
            except sqlite3.Error:
                # Display warning if the database file cannot be opened
                QMessageBox.warning(None, "Database Error", f"Failed to open database file: {file_name}. Skipping...")
                continue
                        
    # Step 5: Concatenate collected data frames
    if collected_data_frames:
        collected_data = pd.concat(collected_data_frames, ignore_index=True)
        
        # Step 6: Define the desired column order and reorder columns
        desired_columns = ["Line_name", "Type", "Module", "Recipe_name", "StartTime", "Stage_no",'Position_no', "Parts_pickup_count", "Error_parts_count", "Error_rejected_parts_count", "Rejected_parts_count", "Dislodged_parts_count", "NoPickup_Number_of_parts_not_used", "Used_parts_count", "PartName", "FIDL"]
        collected_data = collected_data.reindex(columns=desired_columns)
        
        # Step 7: Format the StartTime column
        collected_data['StartTime'] = pd.to_datetime(collected_data['StartTime']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Step 8: Save collected data to a SQLite database
        db_file = f'collected_data_{start_date}_{end_date}.db'
        conn = sqlite3.connect(db_file)
        collected_data.to_sql('collected_data', conn, if_exists='replace', index=False)
        conn.close()

        # Step 9: Execute SQL query to aggregate the data
        aggregated_data = aggregate_data(db_file)

        # Step 10: Save the aggregated data to a CSV file
        aggregated_data.to_csv(f'Report_generate_{start_date}_{end_date}{servers}.csv', index=False)
        # Step 11: remove db file collected
        os.remove(db_file)

def aggregate_data(db_file):
    conn = sqlite3.connect(db_file)
    query = """
    SELECT
        Line_name,
        Type,
        Module,
        Recipe_name,
        PartName,
        MIN(StartTime) AS StartTime,
        MAX(StartTime) AS EndTime,
        MAX(Position_no) AS Slot,
        MIN(Stage_no) AS Stage_no,
        SUM(Parts_pickup_count) AS PickupCount,
        SUM(Used_parts_count) AS TotalPartsUsed,
        SUM(Rejected_parts_count) AS RejectParts,
        SUM(NoPickup_Number_of_parts_not_used) AS PickupMiss,
        SUM(Error_parts_count) AS ErrorParts,
        SUM(Error_rejected_parts_count) AS Error_rejected_parts_count,
        SUM(Dislodged_parts_count) AS Dislodged_parts_count,
        FIDL
    FROM collected_data
    GROUP BY Line_name, Type, Module, Recipe_name, FIDL, PartName, strftime('%Y%m%d', StartTime);
    """

    aggregated_data = pd.read_sql_query(query, conn)
    conn.close()

    # Calculate errorPickupRate and ErrorRate
    aggregated_data['ErrorPickupRate'] = (aggregated_data['PickupMiss'] / aggregated_data['PickupCount']) * 100
    aggregated_data['ErrorRate'] = (aggregated_data['ErrorParts'] / aggregated_data['PickupCount']) * 100
    aggregated_data['RejectRate'] = (aggregated_data['RejectParts'] / aggregated_data['PickupCount']) * 100

    # Define the desired column order
    column_order = ["Line_name", "Type", "Module", "Recipe_name", "StartTime", "EndTime", "PartName", "Slot",
                    "Stage_no", "PickupCount", "TotalPartsUsed", "RejectParts", "PickupMiss", "ErrorParts",
                    "Error_rejected_parts_count", "Dislodged_parts_count","RejectRate", "ErrorPickupRate", "ErrorRate", "FIDL"]

    # Reorder the columns
    aggregated_data = aggregated_data[column_order]

    return aggregated_data





