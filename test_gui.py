import sys
import time
import os
from PyQt5.QtWidgets import QFileDialog, QApplication, QMainWindow, QLabel, QVBoxLayout, QCalendarWidget, QPushButton, QWidget, QListWidget, QListView, QHBoxLayout, QDateTimeEdit, QMessageBox, QScrollArea
from PyQt5.QtCore import Qt, QDateTime, QThread, pyqtSignal, pyqtSlot
from PyQt5.QtGui import QIcon
import Function
import configparser
import shutil

class DataCollectionThread(QThread):
    finished = pyqtSignal(float)

    def __init__(self, start_date, end_date, start_time, end_time, selected_servers, parent=None):
        super(DataCollectionThread, self).__init__(parent)
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = start_time
        self.end_time = end_time
        self.selected_servers = selected_servers

    def run(self):
        start_time_processing = time.time()
        Function.process_collected_data(self.start_date, self.end_date, self.start_time, self.end_time, self.selected_servers)
        end_time_processing = time.time()
        execution_time_seconds = round(end_time_processing - start_time_processing, 2)
        self.finished.emit(execution_time_seconds)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Production Parts Report")
        self.setGeometry(100, 100, 1100, 900)  # Increased width for better display
        self.setMinimumWidth(800)  

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
        self.server_list.addItems(self.get_server_list())
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

        # Execution Time Label
        self.execution_time_label = QLabel()
        main_layout.addWidget(self.execution_time_label)

        # File Generation Label
        self.file_generation_label = QLabel()
        self.file_generation_label.setStyleSheet("color: green")
        main_layout.addWidget(self.file_generation_label)

        # Set Application Icon
        self.setWindowIcon(QIcon(r"D:\Dev\report reject asteel main\logo.jpg"))

        # Save report button
        self.save_button = QPushButton("Save Report")
        self.save_button.clicked.connect(self.save_file)
        main_layout.addWidget(self.save_button)

        # Initialize member variables
        self.generated_file_path = ""

    def get_server_list(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        return config['Paths'].keys()

    @pyqtSlot()
    def sync_files(self):
        self.file_list_widget.clear()

        start_date = self.start_calendar.selectedDate().toString("yyyyMMdd")
        end_date = self.end_calendar.selectedDate().toString("yyyyMMdd")
        start_time = self.start_time_edit.time().toString("HH:mm")
        end_time = self.end_time_edit.time().toString("HH:mm")

        selected_servers = [item.text() for item in self.server_list.selectedItems()]
        print("Selected Servers:", selected_servers)

        file_names = []
        for server in selected_servers:
            file_names += Function.collecter_noms_fichiers_bases_de_donnees(config['Paths'].get(server, ""), start_date, end_date)

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

    @pyqtSlot(float)
    def process_execution_time(self, execution_time_seconds):
        minutes = int(execution_time_seconds / 60)
        seconds = int(execution_time_seconds % 60)
        self.execution_time_label.setText(f"Execution Time: {minutes} minutes {seconds} seconds")

        # Enable collect data button
        self.sync_button.setEnabled(True)

        # Update file generation label
        self.file_generation_label.setText("File Generation Complete")

    @pyqtSlot()
    def save_file(self):
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        directory = QFileDialog.getExistingDirectory(self, "Select Directory", "", options=options)
        if directory and self.generated_file_path:
            try:
                shutil.move(self.generated_file_path, os.path.join(directory, f"Report_generate_{self.start_date}_{self.end_date}{self.selected_servers}.csv"))
                QMessageBox.information(self, "Success", "Report saved successfully.")
            except Exception as e:
                QMessageBox.warning(self, "Error", f"Failed to save report: {str(e)}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
