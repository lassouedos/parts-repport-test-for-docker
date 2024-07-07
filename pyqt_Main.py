import sys
import os
import sqlite3
from datetime import datetime
import pandas as pd
from PyQt5.QtWidgets import (
     QApplication, QMainWindow, QLabel, QVBoxLayout, 
    QCalendarWidget, QPushButton, QWidget, QListWidget, QListView, QHBoxLayout, 
    QDateTimeEdit, QMessageBox, QScrollArea, QFileDialog
)
from PyQt5.QtCore import  QDateTime, QThread, pyqtSignal, pyqtSlot, QTimer
from PyQt5.QtGui import QIcon
import configparser

class DataProcessor:
    def __init__(self, start_date=None, end_date=None, start_time=None, end_time=None, selected_servers=None):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = start_time
        self.end_time = end_time
        self.selected_servers = selected_servers

    @staticmethod
    def read_config():
        config = configparser.ConfigParser()
        config.read('config.ini')
        return config

    @staticmethod
    def collect_database_files(server_path, start_date, end_date):
        file_names = []
        if os.path.exists(server_path):
            for file_name in os.listdir(server_path):
                if file_name.endswith("[COUNT].db"):
                    date_fichier = file_name.split("_")[0]
                    if start_date <= date_fichier <= end_date:
                        fichier_db = os.path.join(server_path, file_name)
                        file_names.append(fichier_db)
        return file_names

    def process_collected_data(self):
        # Read configuration from config.ini
        config = self.read_config()
        collected_data_frames = []

        # Step 1: Iterate through each server
        for server in self.selected_servers:
            # Step 2: Collect database files
            server_path = config['Paths'][server]
            collected_files = self.collect_database_files(server_path, self.start_date, self.end_date)

            # Step 3: For each database file
            for file_name in collected_files:
                print(f"file name = {file_name}\n ")
                try:
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
                            start_datetime = datetime.strptime(f"{self.start_date} {self.start_time}", "%Y%m%d %H:%M")
                            end_datetime = datetime.strptime(f"{self.end_date} {self.end_time}", "%Y%m%d %H:%M")

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
            db_file = f'collected_data_{self.start_date}_{self.end_date}.db'
            conn = sqlite3.connect(db_file)
            collected_data.to_sql('collected_data', conn, if_exists='replace', index=False)
            conn.close()

            # Step 9: Execute SQL query to aggregate the data
            aggregated_data = self.aggregate_data(db_file)
            
            # Step 9.1 Get the folder path where the CSV file will be saved
            folder_path = os.path.dirname(os.path.abspath(__file__))
            
            # Step 10: Save the aggregated data to a CSV file
            csv_file_path = os.path.join(folder_path, f'Report_generate_{self.start_date}_{self.end_date}{self.selected_servers}.csv')
            aggregated_data.to_csv(csv_file_path, index=False)
            
            # Step 11: remove db file collected
            os.remove(db_file)
            return  csv_file_path
    
    @staticmethod
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

class DataCollectionThread(QThread):
    finished = pyqtSignal(str)  # Change the signal type to str

    def __init__(self, start_date, end_date, start_time, end_time, selected_servers, parent=None):
        super(DataCollectionThread, self).__init__(parent)
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = start_time
        self.end_time = end_time
        self.selected_servers = selected_servers

    def run(self):
        
        processor = DataProcessor(self.start_date, self.end_date, self.start_time, self.end_time, self.selected_servers)
        csv_file_path = processor.process_collected_data()
        if csv_file_path is not None:
            self.finished.emit(csv_file_path)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Production Parts Report")
        self.setGeometry(100, 100, 1100, 850)  # Increased width for better display
        self.setMinimumWidth(800)
        # Initialize start_time_collecting_data attribute
        self.start_time_collecting_data = None
        # Central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # Main layout
        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)

        # Select Date Layout
        date_layout = QHBoxLayout()
        main_layout.addLayout(date_layout)

        # Select Start Date
        start_date_label = QLabel("Select Start Date:")
        date_layout.addWidget(start_date_label)
        self.start_calendar = QCalendarWidget()
        date_layout.addWidget(self.start_calendar)

        # Select End Date
        end_date_label = QLabel("Select End Date:")
        date_layout.addWidget(end_date_label)
        self.end_calendar = QCalendarWidget()
        date_layout.addWidget(self.end_calendar)

        # Select Time Layout
        time_layout = QHBoxLayout()
        main_layout.addLayout(time_layout)

        # Select Start Time
        start_time_label = QLabel("Start Time (HH:mm):")
        time_layout.addWidget(start_time_label)
        self.start_time_edit = QDateTimeEdit()
        self.start_time_edit.setDateTime(QDateTime.currentDateTime())
        self.start_time_edit.setDisplayFormat("HH:mm")
        time_layout.addWidget(self.start_time_edit)

        # Select End Time
        end_time_label = QLabel("End Time (HH:mm):")
        time_layout.addWidget(end_time_label)
        self.end_time_edit = QDateTimeEdit()
        self.end_time_edit.setDateTime(QDateTime.currentDateTime())
        self.end_time_edit.setDisplayFormat("HH:mm")
        time_layout.addWidget(self.end_time_edit)

        # Server Selection
        server_label = QLabel("Select Line(s):")
        main_layout.addWidget(server_label)

        # Scroll area for server list to accommodate many servers
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        main_layout.addWidget(scroll_area)

        server_widget = QWidget()
        scroll_area.setWidget(server_widget)

        server_layout = QVBoxLayout()
        server_widget.setLayout(server_layout)

        self.server_list = QListWidget()
        self.server_list.setSelectionMode(QListWidget.MultiSelection)
        self.server_list.setViewMode(QListView.ListMode)
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.server_list.addItems(config['Paths'].keys())
        server_layout.addWidget(self.server_list)

        # Sync Button
        self.sync_button = QPushButton("Collect Data")
        self.sync_button.clicked.connect(self.sync_files)
        main_layout.addWidget(self.sync_button)

        # File List
        file_list_label = QLabel("Files Found:")
        main_layout.addWidget(file_list_label)
        self.file_list_widget = QListWidget()
        main_layout.addWidget(self.file_list_widget)

        # File Count Label
        self.file_count_label = QLabel()
        main_layout.addWidget(self.file_count_label)

        # Data Processing Label
        self.processing_label = QLabel()
        self.processing_label.setStyleSheet("color: blue")
        main_layout.addWidget(self.processing_label)

        # Execution Time Label
        self.execution_time_label = QLabel()
        main_layout.addWidget(self.execution_time_label)

        # File Generation Label
        self.file_generation_label = QLabel()
        self.file_generation_label.setStyleSheet("color: green")
        main_layout.addWidget(self.file_generation_label)

        # Save Button
        self.save_button = QPushButton("Save")
        self.save_button.setEnabled(False)  # Initially disabled
        main_layout.addWidget(self.save_button)
        

        # Set Application Icon
        self.setWindowIcon(QIcon(r"D:\Dev\report reject asteel main\logo.jpg"))

        # QTimer for updating processing label text
        self.processing_timer = QTimer(self)
        self.processing_timer.timeout.connect(self.update_processing_label)

        # Initialize labels
        self.reset_labels()

        # Connect save button click event to save_csv method
        self.save_button.clicked.connect(self.save_csv)

    def reset_labels(self):
        self.file_list_widget.clear()
        self.file_count_label.clear()
        self.processing_label.clear()
        self.execution_time_label.clear()
        self.file_generation_label.clear()

    @pyqtSlot()
    def sync_files(self):
        # Reset labels
        self.reset_labels()

        try:
            # Get the selected start and end dates
            start_date = self.start_calendar.selectedDate().toString("yyyyMMdd")
            end_date = self.end_calendar.selectedDate().toString("yyyyMMdd")

            # Check for valid start and end dates
            if start_date > end_date:
                QMessageBox.warning(self, "Invalid Input", "Start date cannot be after end date.")
                return
        except AttributeError:
            QMessageBox.warning(self, "Invalid Input", "Please select start and end dates.")
            return

        # Get the selected start and end times
        try:
            start_time = self.start_time_edit.time().toString("HH:mm")
            end_time = self.end_time_edit.time().toString("HH:mm")
        except AttributeError:
            QMessageBox.warning(self, "Invalid Input", "Please select start and end times.")
            return

        # Check for valid start and end times
        if not start_time or not end_time:
            QMessageBox.warning(self, "Invalid Input", "Please enter start and end times.")
            return

        # Convert date-time strings to datetime objects for comparison
        start_datetime = datetime.strptime(start_date + start_time, "%Y%m%d%H:%M")
        end_datetime = datetime.strptime(end_date + end_time, "%Y%m%d%H:%M")

        # Check if start datetime is before end datetime
        if start_datetime >= end_datetime:
            QMessageBox.warning(self, "Invalid Input", "Start date and time must be before end date and time.")
            return

        # Set start_time_collecting_data attribute
        self.start_time_collecting_data = datetime.now()

        # Get the selected servers
        selected_servers = [item.text() for item in self.server_list.selectedItems()]
        if not selected_servers:
            QMessageBox.warning(self, "Invalid Input", "Please select at least one Line Production.")
            return

        file_names = []
        for server in selected_servers:
            config = configparser.ConfigParser()
            config.read('config.ini')
            path = config['Paths'].get(server, "")
            if os.path.exists(path):  # Check if the path exists
                files = DataProcessor.collect_database_files(path, start_date, end_date)
                if files:
                    file_names.extend(files)
                else:
                    QMessageBox.warning(self, "No Database Files Found", f"No database files found for server '{server}'.")
            else:
                QMessageBox.warning(self, "Path Not Found", f"The path for '{server}' does not exist.")

        if not file_names:
            QMessageBox.warning(self, "No Database Files", "No database files found for the selected servers.")
            return

        # Display file paths
        for file_name in file_names:
            self.file_list_widget.addItem(file_name)

        # Start data collection in a separate thread
        self.data_collection_thread = DataCollectionThread(start_date, end_date, start_time, end_time, selected_servers)
        self.data_collection_thread.finished.connect(self.process_execution_time)
        self.data_collection_thread.start()

        # Update file count label
        self.file_count_label.setText(f"Files Found: {len(file_names)}")

        # Disable collect data button
        self.sync_button.setEnabled(False)

        # Start QTimer to update processing label text
        self.processing_label.setText("Data Processing...")  # Moved here to start only after valid inputs
        self.processing_timer.start(1000)  # Update every second


   

    @pyqtSlot(str)
    def process_execution_time(self, csv_file_path):
        # Check if start_time_collecting_data is set
        if self.start_time_collecting_data is not None:
            # Get the start time recorded when data collection starts
            start_time = self.start_time_collecting_data

            # Get the end time when CSV file generation completes
            end_time = datetime.now()

            # Calculate the execution time in seconds
            execution_time_seconds = (end_time - start_time).total_seconds()

            # Convert the execution time to minutes and seconds
            minutes = int(execution_time_seconds // 60)
            seconds = int(execution_time_seconds % 60)

            # Display the execution time
            self.execution_time_label.setText(f"Execution Time: {minutes} minutes {seconds} seconds")

        else:
            # If start_time_collecting_data is not set, display an error message
            QMessageBox.warning(self, "Error", "Failed to calculate execution time: start time not recorded.")

        # Enable the collect data button
        self.sync_button.setEnabled(True)

        # Update the file generation label
        self.file_generation_label.setText("File Generation Complete")

        # Enable the save button
        self.save_button.setEnabled(True)

        # Stop the QTimer for updating processing label text
        self.processing_timer.stop()

        # Set csv_file_path attribute
        self.csv_file_path = csv_file_path


    @pyqtSlot()
    def update_processing_label(self):
        current_text = self.processing_label.text()
        if current_text.endswith("..."):
            self.processing_label.setText("Data Processing")
        else:
            self.processing_label.setText(current_text + ".")

    @pyqtSlot(bool)
    def save_csv(self, clicked):
        # Prompt the user to select the destination folder and enter the new file name
        file_path, _ = QFileDialog.getSaveFileName(self, "Save Report File", "", "Report Files (*.csv)")
        if file_path:
            # Move the CSV file to the selected destination folder with the specified name
            try:
                os.rename(self.csv_file_path, file_path)
                QMessageBox.information(self, "Save", f"CSV file saved to:\n{file_path}")
            except Exception as e:
                QMessageBox.warning(self, "Error", f"Failed to move the CSV file: {str(e)}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
