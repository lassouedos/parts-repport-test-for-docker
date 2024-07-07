import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QLabel, QVBoxLayout, QCalendarWidget, QPushButton, QWidget, QListWidget, QListView, QHBoxLayout, QDateTimeEdit, QMessageBox
from PyQt5.QtCore import Qt, QDateTime
from PyQt5.QtGui import QIcon
import Function
import config
import time
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Production Parts Report")
        self.setGeometry(100, 100, 800, 800)

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
        self.server_list = QListWidget()
        self.server_list.setSelectionMode(QListWidget.MultiSelection)
        self.server_list.setViewMode(QListView.ListMode)
        self.server_list.addItems(config.chemins_dossiers_pc.keys())
        main_layout.addWidget(self.server_list)

        # Sync Button
        sync_button = QPushButton("Collect Data")
        sync_button.clicked.connect(self.sync_files)
        main_layout.addWidget(sync_button)

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

        # Set Application Icon
        self.setWindowIcon(QIcon(r"D:\Dev\report reject asteel main\App.png"))

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
            file_names += Function.collecter_noms_fichiers_bases_de_donnees(config.chemins_dossiers_pc[server], start_date, end_date)

        # Display file paths
        for file_name in file_names:
            self.file_list_widget.addItem(file_name)

        
        
        start_time_processing = time.time()

        # Process collected data
        Function.process_collected_data(start_date, end_date, start_time, end_time, selected_servers)
        end_time_processing = time.time()
        execution_time = round(end_time_processing - start_time_processing, 2)

        # Convert execution time to minutes and seconds
        minutes = int(execution_time) // 60
        seconds = int(execution_time) % 60

        # Update file count label
        file_count_text = f"Number of Files Found: {len(file_names)}"
        self.file_count_label.setText(file_count_text)
        # Update file count label after processing
        self.file_count_label.setText("<font color='green'>Data processing completed.</font>")

        # Update file count label
        file_count_text = f"Number of Files Found: {len(file_names)}"
        self.file_count_label.setText(file_count_text)

        # Update execution time label
        self.execution_time_label.setText(f"Execution time: {minutes} minutes {seconds} seconds")
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
