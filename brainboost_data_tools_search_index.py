#!/usr/bin/env python3

import sys
import sqlite3
import time  # For measuring execution time
import json  # For parsing global.config
import subprocess  # For running external commands
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QLabel, QLineEdit, QPushButton, QVBoxLayout,
    QHBoxLayout, QTableWidget, QTableWidgetItem, QComboBox, QDateEdit, QMessageBox,
    QCheckBox, QSizePolicy, QFileDialog, QAction, QMenu, QTextEdit, QProgressBar
)
from PyQt5.QtCore import Qt, QDate, QSize, QPoint, QUrl, QMimeData, QTimer, QThread, pyqtSignal
from PyQt5.QtGui import QIcon, QPixmap, QPainter, QFont, QTextOption, QCursor
from PyQt5.QtSvg import QSvgRenderer
import os  # For path operations
import configparser  # For parsing rclone.config

# Embedded SVG Icon (Neon Yellow and Pink File Search Icon)
SVG_ICON = """
<svg width="256" height="256" viewBox="0 0 256 256" xmlns="http://www.w3.org/2000/svg">
  <!-- Background Circle -->
  <circle cx="128" cy="128" r="120" fill="#1a1a1a" stroke="#ff69b4" stroke-width="8"/>
  
  <!-- File Icon -->
  <rect x="76" y="80" width="80" height="100" fill="#ffffff" rx="8" ry="8" stroke="#ffff00" stroke-width="4"/>
  <polyline points="76,80 156,80 156,160" fill="none" stroke="#ffff00" stroke-width="4"/>
  
  <!-- Magnifying Glass -->
  <circle cx="176" cy="176" r="32" fill="none" stroke="#ffff00" stroke-width="8"/>
  <line x1="200" y1="200" x2="240" y2="240" stroke="#ff69b4" stroke-width="8" stroke-linecap="round"/>
  
  <!-- Glow Effects -->
  <circle cx="176" cy="176" r="40" fill="none" stroke="rgba(255, 105, 180, 0.3)" stroke-width="16" filter="url(#glow)"/>
  
  <!-- Filters for Glow -->
  <defs>
    <filter id="glow">
      <feGaussianBlur stdDeviation="10" result="coloredBlur"/>
      <feMerge>
        <feMergeNode in="coloredBlur"/>
        <feMergeNode in="coloredBlur"/>
        <feMergeNode in="coloredBlur"/>
      </feMerge>
    </filter>
  </defs>
</svg>
"""

# Default rclone config path
DEFAULT_RCLONE_CONFIG_PATH = "/brainboost/brainboost_server/server_rclone.conf"

# Default rclone_list_files.py script path
DEFAULT_SEARCH_INDEX_SCRIPT_PATH = "/brainboost/brainboost_data/data_source/brainboost_data_source_rclone/rclone_list_files.py"

# Path to global.config
GLOBAL_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "global.config")


class ScriptRunner(QThread):
    """
    Worker thread to execute external scripts asynchronously.
    Emits signals to update the UI in real-time.
    """
    output_signal = pyqtSignal(str)
    error_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(int)  # Emit return code

    def __init__(self, script_path):
        super().__init__()
        self.script_path = script_path

    def run(self):
        try:
            process = subprocess.Popen(
                ["python3", self.script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            # Read stdout
            for line in iter(process.stdout.readline, ''):
                if line:
                    self.output_signal.emit(line.strip())
            process.stdout.close()

            # Read stderr
            stderr = process.stderr.read()
            if stderr:
                self.error_signal.emit(stderr.strip())
            process.stderr.close()

            return_code = process.wait()
            self.finished_signal.emit(return_code)
        except Exception as e:
            self.error_signal.emit(str(e))
            self.finished_signal.emit(-1)


class FileSearchApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BrainBoost File Search")
        self.resize(1200, 800)  # Increased size for better layout

        # Initialize rclone config path
        self.rclone_config_path = DEFAULT_RCLONE_CONFIG_PATH

        # Initialize search index script path
        self.search_index_script_path = DEFAULT_SEARCH_INDEX_SCRIPT_PATH

        # Path to the SQLite database
        self.db_path = "/brainboost/brainboost_data/data_source/brainboost_data_source_rclone/search_rclone_index_db.sqlite"

        # Read global.config
        self.drives_dir = self.read_global_config()

        print("Initializing UI...")
        self.initUI()
        self.center_window()
        print("UI initialized successfully.")

        # **Set Focus on the Name Text Field After UI Initialization Using QTimer**
        QTimer.singleShot(0, self.name_input.setFocus)

    def read_global_config(self):
        """Read the global.config file to get configuration settings."""
        print("Reading global.config...")
        if not os.path.exists(GLOBAL_CONFIG_PATH):
            print(f"global.config not found at {GLOBAL_CONFIG_PATH}. Using default drives directory.")
            # Define a default drives directory
            default_drives_dir = "/brainboost/brainboost_data/data_storage/storage_clouds"
            os.makedirs(default_drives_dir, exist_ok=True)
            return default_drives_dir

        try:
            with open(GLOBAL_CONFIG_PATH, 'r') as config_file:
                config_data = json.load(config_file)
                drives_dir = config_data.get("drives_dir", "/brainboost/brainboost_data/data_storage/storage_clouds")
                os.makedirs(drives_dir, exist_ok=True)
                print(f"Drives directory set to: {drives_dir}")
                return drives_dir
        except json.JSONDecodeError as e:
            print(f"Error parsing global.config: {e}. Using default drives directory.")
            default_drives_dir = "/brainboost/brainboost_data/data_storage/storage_clouds"
            os.makedirs(default_drives_dir, exist_ok=True)
            return default_drives_dir
        except Exception as e:
            print(f"Unexpected error reading global.config: {e}. Using default drives directory.")
            default_drives_dir = "/brainboost/brainboost_data/data_storage/storage_clouds"
            os.makedirs(default_drives_dir, exist_ok=True)
            return default_drives_dir

    def initUI(self):
        # Set the window icon from the embedded SVG
        self.set_window_icon()

        # Create central widget and main layout
        central_widget = QWidget()
        main_layout = QVBoxLayout()
        main_layout.setAlignment(Qt.AlignTop)

        # Top Layout: Name Search Box centered and Icon at top right
        top_layout = QHBoxLayout()

        # Spacer to push the Search Box to center
        top_spacer_left = QHBoxLayout()
        top_spacer_left.addStretch()
        top_layout.addLayout(top_spacer_left)

        # Name Search Box (3 times bigger, centered text, increased text size, half width)
        name_layout = QVBoxLayout()
        name_label = QLabel("Name:")
        name_label.setFont(QFont("Arial", 19))  # Increased font size by ~30% from 14 to 19
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("Enter part or full file name")
        self.name_input.setFont(QFont("Arial", 26))  # Increased font size by ~30% from 20 to 26
        self.name_input.setAlignment(Qt.AlignCenter)  # Center the text within the search box
        self.name_input.setFixedWidth(600)  # Adjusted width to half (assuming original was 1200)
        self.name_input.setFixedHeight(60)  # Increased height for better visibility
        self.name_input.returnPressed.connect(self.perform_search)  # Trigger search on Enter
        name_layout.addWidget(name_label, alignment=Qt.AlignCenter)
        name_layout.addWidget(self.name_input, alignment=Qt.AlignCenter)
        top_layout.addLayout(name_layout)

        # Spacer between Search Box and Icon
        top_spacer_right = QHBoxLayout()
        top_spacer_right.addStretch()
        top_layout.addLayout(top_spacer_right)

        # Icon Display (Top Right, 50% smaller)
        icon_label = QLabel()
        icon_pixmap = self.render_svg_icon()
        icon_label.setPixmap(icon_pixmap.scaled(64, 64, Qt.KeepAspectRatio, Qt.SmoothTransformation))  # 50% smaller
        icon_label.setFixedSize(64, 64)
        top_layout.addWidget(icon_label, alignment=Qt.AlignRight | Qt.AlignTop)

        # Add top layout to main layout
        main_layout.addLayout(top_layout)

        # Filters Layout: Organized into two rows of two filters each
        filters_main_layout = QVBoxLayout()

        # First Row: Size and Drive Filters
        first_filter_row = QHBoxLayout()

        # Size Filter
        size_layout = QVBoxLayout()
        size_label = QLabel("Size > (bytes):")
        size_label.setFont(QFont("Arial", 14))  # Increased font size from 12 to 14
        self.size_input = QLineEdit()
        self.size_input.setPlaceholderText("Enter minimum size")
        self.size_input.setFont(QFont("Arial", 14))  # Increased font size from 12 to 14
        self.size_input.setFixedWidth(300)
        size_layout.addWidget(size_label)
        size_layout.addWidget(self.size_input)
        first_filter_row.addLayout(size_layout)

        # Drive Filter
        drive_layout = QVBoxLayout()
        drive_label = QLabel("Drive:")
        drive_label.setFont(QFont("Arial", 14))  # Increased font size from 12 to 14
        self.drive_combo = QComboBox()
        self.drive_combo.setFont(QFont("Arial", 14))  # Increased font size from 12 to 14
        self.drive_combo.addItem("Any")
        self.populate_drive_combo()
        self.drive_combo.setFixedWidth(300)
        drive_layout.addWidget(drive_label)
        drive_layout.addWidget(self.drive_combo)
        first_filter_row.addLayout(drive_layout)

        filters_main_layout.addLayout(first_filter_row)

        # Second Row: Modified Date and File Type Filters
        second_filter_row = QHBoxLayout()

        # Modified Date Filter with Checkbox
        date_layout = QVBoxLayout()
        self.date_checkbox = QCheckBox("Filter by Modified Date:")
        self.date_checkbox.setFont(QFont("Arial", 14))  # Increased font size from 12 to 14
        self.date_checkbox.setChecked(False)  # Default unchecked
        self.date_edit = QDateEdit()
        self.date_edit.setCalendarPopup(True)
        self.date_edit.setDate(QDate.currentDate())
        self.date_edit.setEnabled(False)  # Disabled by default
        self.date_edit.setFont(QFont("Arial", 14))  # Increased font size from 12 to 14
        self.date_edit.setFixedWidth(300)
        self.date_checkbox.stateChanged.connect(self.toggle_date_filter)
        date_layout.addWidget(self.date_checkbox)
        date_layout.addWidget(self.date_edit)
        second_filter_row.addLayout(date_layout)

        # File Type Filter
        file_type_layout = QVBoxLayout()
        file_type_label = QLabel("File Type:")
        file_type_label.setFont(QFont("Arial", 14))  # Increased font size from 12 to 14
        self.file_type_combo = QComboBox()
        self.file_type_combo.setFont(QFont("Arial", 14))  # Increased font size from 12 to 14
        self.file_type_combo.addItem("Any")
        self.populate_file_type_combo()
        self.file_type_combo.setFixedWidth(300)
        file_type_layout.addWidget(file_type_label)
        file_type_layout.addWidget(self.file_type_combo)
        second_filter_row.addLayout(file_type_layout)

        filters_main_layout.addLayout(second_filter_row)

        # Add filters layout to main layout
        main_layout.addLayout(filters_main_layout)

        # **Rearranged: Search, Clear Filters, and Update Index Buttons Centered and Right-Aligned**
        button_layout = QHBoxLayout()

        # Stretch to push buttons to the center/right
        button_layout.addStretch()

        self.search_button = QPushButton("Search")
        self.search_button.setFont(QFont("Arial", 14))  # Increased font size from 12 to 14
        self.search_button.clicked.connect(self.perform_search)
        self.search_button.setFixedWidth(150)  # Increased width from 120 to 150
        button_layout.addWidget(self.search_button)

        self.clear_button = QPushButton("Clear Filters")
        self.clear_button.setFont(QFont("Arial", 14))  # Increased font size from 12 to 14
        self.clear_button.clicked.connect(self.clear_filters)
        self.clear_button.setFixedWidth(180)  # Increased width from 150 to 180
        button_layout.addWidget(self.clear_button)

        # **New: Update Index Button Positioned Beside Clear Filters Button**
        self.update_index_button = QPushButton("Update Index")
        self.update_index_button.setFont(QFont("Arial", 14))  # Increased font size
        self.update_index_button.setFixedWidth(150)
        self.update_index_button.clicked.connect(self.update_index)
        button_layout.addWidget(self.update_index_button)
        # **End of New Section**

        button_layout.addStretch()
        main_layout.addLayout(button_layout)
        # **End of Rearranged Section**

        # Results Table
        self.results_table = QTableWidget()
        # Update column count and headers by replacing 'Full Path' with 'File Name'
        self.results_table.setColumnCount(5)  # Reduced by hiding 'ID'
        # New order: ["File Name", "Drive", "Size (bytes)", "File Type", "Modified Date"]
        self.results_table.setHorizontalHeaderLabels(["File Name", "Drive", "Size (bytes)", "File Type", "Modified Date"])
        self.results_table.horizontalHeader().setStretchLastSection(True)
        self.results_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.results_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.results_table.setFont(QFont("Arial", 14))  # Increased font size from 12 to 14
        self.results_table.horizontalHeader().setFont(QFont("Arial", 14))  # Increased font size from 12 to 14
        self.results_table.verticalHeader().setVisible(False)
        self.results_table.setSortingEnabled(True)  # Enable sorting
        self.results_table.setWordWrap(True)  # Enable word wrap

        # Set a fixed row height to accommodate two lines of text
        font_metrics = self.results_table.fontMetrics()
        line_height = font_metrics.lineSpacing()
        max_height = line_height * 2 + 10  # Two lines plus padding
        self.results_table.verticalHeader().setDefaultSectionSize(max_height)

        # Disable automatic resizing based on contents
        # self.results_table.resizeColumnsToContents()  # Removed to prevent changing column widths on data load

        # Enable custom context menu
        self.results_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.results_table.customContextMenuRequested.connect(self.open_context_menu)

        # Connect double-click to show folder
        self.results_table.cellDoubleClicked.connect(self.show_folder)

        main_layout.addWidget(self.results_table)

        # **New: Text Area for Update Index Output**
        self.update_output_text = QTextEdit()
        self.update_output_text.setReadOnly(True)
        self.update_output_text.setStyleSheet("background-color: black; color: green;")
        self.update_output_text.setFont(QFont("Courier", 12))  # Monospaced font for better readability
        self.update_output_text.hide()  # Initially hidden
        main_layout.addWidget(self.update_output_text)
        # **End of New Section**

        # **New: Progress Bar for Database Queries**
        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(0)  # Indeterminate
        self.progress_bar.setVisible(False)  # Initially hidden
        main_layout.addWidget(self.progress_bar)
        # **End of New Section**

        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)

        # Initialize status bar
        self.init_status_bar()

        # Initialize menu
        self.init_menu()

        # Adjust column widths initially
        self.adjust_column_widths()

    def center_window(self):
        """Center the window on the screen."""
        frame_gm = self.frameGeometry()
        screen = QApplication.desktop().screenNumber(QApplication.desktop().cursor().pos())
        center_point = QApplication.desktop().screenGeometry(screen).center()
        frame_gm.moveCenter(center_point)
        self.move(frame_gm.topLeft())
        print("Window centered on the screen.")

    def init_status_bar(self):
        """Initialize the status bar to display the rclone config path and drives_dir."""
        self.statusBar().showMessage(f"rclone.config path: {self.rclone_config_path} | Mount Root: {self.drives_dir}")
        print(f"Status bar initialized with rclone.config path: {self.rclone_config_path} and Mount Root: {self.drives_dir}")

    def init_menu(self):
        """Initialize the menu bar with options to change the rclone config path and mount root."""
        menubar = self.menuBar()
        settings_menu = menubar.addMenu("Settings")

        # Action to change rclone config path
        change_config_action = QAction("Change rclone.config Path", self)
        change_config_action.setShortcut("Ctrl+O")
        change_config_action.setStatusTip("Change the path to the rclone.config file")
        change_config_action.triggered.connect(self.change_rclone_config_path)
        settings_menu.addAction(change_config_action)

        # Action to change mount root directory
        change_mount_root_action = QAction("Change Mount Root Directory", self)
        change_mount_root_action.setShortcut("Ctrl+M")
        change_mount_root_action.setStatusTip("Change the root directory for mounting drives")
        change_mount_root_action.triggered.connect(self.change_mount_root_directory)
        settings_menu.addAction(change_mount_root_action)

    def change_rclone_config_path(self):
        """Open a file dialog to allow the user to select a new rclone config file path."""
        options = QFileDialog.Options()
        options |= QFileDialog.ReadOnly
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select rclone.config File",
            "",
            "Configuration Files (*.conf);;All Files (*)",
            options=options
        )
        if file_path:
            # Update the config path
            self.rclone_config_path = file_path
            # Update the status bar
            self.statusBar().showMessage(f"rclone.config path: {self.rclone_config_path} | Mount Root: {self.drives_dir}")
            print(f"rclone.config path changed to: {self.rclone_config_path}")
            # Optionally, you can add logic here to reload or apply the new config

    def change_mount_root_directory(self):
        """Open a directory dialog to allow the user to select a new mount root directory."""
        options = QFileDialog.Options()
        options |= QFileDialog.ShowDirsOnly
        directory = QFileDialog.getExistingDirectory(
            self,
            "Select New Mount Root Directory",
            self.drives_dir,
            options=options
        )
        if directory:
            # Update the drives_dir
            self.drives_dir = directory
            # Update the status bar
            self.statusBar().showMessage(f"rclone.config path: {self.rclone_config_path} | Mount Root: {self.drives_dir}")
            print(f"Mount Root Directory changed to: {self.drives_dir}")

            # Update global.config
            self.update_global_config()

    def update_global_config(self):
        """Update the global.config file with the new drives_dir."""
        try:
            config_data = {"drives_dir": self.drives_dir}
            with open(GLOBAL_CONFIG_PATH, 'w') as config_file:
                json.dump(config_data, config_file, indent=4)
            print(f"global.config updated with drives_dir: {self.drives_dir}")
        except Exception as e:
            self.show_error(f"Failed to update global.config: {e}")

    def set_window_icon(self):
        """Set the window icon from the embedded SVG data."""
        try:
            # Initialize QSvgRenderer with the SVG data
            renderer = QSvgRenderer(SVG_ICON.encode('utf-8'))
            pixmap = QPixmap(256, 256)
            pixmap.fill(Qt.transparent)

            # Render the SVG onto the pixmap
            painter = QPainter(pixmap)
            renderer.render(painter)
            painter.end()

            # Set the window icon
            self.setWindowIcon(QIcon(pixmap))
            print("Window icon set successfully.")
        except Exception as e:
            print(f"Error setting window icon: {e}")

    def render_svg_icon(self):
        """Render the embedded SVG icon and return a QPixmap."""
        try:
            renderer = QSvgRenderer(SVG_ICON.encode('utf-8'))
            pixmap = QPixmap(64, 64)  # Adjusted size to 64x64 (50% smaller)
            pixmap.fill(Qt.transparent)
            painter = QPainter(pixmap)
            renderer.render(painter)
            painter.end()
            print("SVG icon rendered successfully.")
            return pixmap
        except Exception as e:
            print(f"Error rendering SVG icon: {e}")
            return QPixmap()

    def toggle_date_filter(self, state):
        """Enable or disable the modified date filter based on the checkbox."""
        if state == Qt.Checked:
            self.date_edit.setEnabled(True)
            print("Modified Date filter enabled.")
        else:
            self.date_edit.setEnabled(False)
            print("Modified Date filter disabled.")

    def populate_drive_combo(self):
        """Populate the drive combo box with distinct drives from the database."""
        print("Populating Drive ComboBox...")
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT drive FROM files ORDER BY drive")
            drives = cursor.fetchall()
            print(f"Found {len(drives)} distinct drives.")
            for drive in drives:
                if drive[0]:  # Ensure drive is not None or empty
                    self.drive_combo.addItem(drive[0])
            conn.close()
            print("Drive ComboBox populated successfully.")
        except sqlite3.OperationalError as e:
            print(f"Error populating drive combo: {e}")
            self.show_error(f"Database Error: {e}")
        except Exception as e:
            print(f"Unexpected error populating drive combo: {e}")
            self.show_error(f"Unexpected Error: {e}")

    def populate_file_type_combo(self):
        """Populate the file type combo box with distinct file types from the database."""
        print("Populating File Type ComboBox...")
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            # Limiting to top 1000 to prevent performance issues
            cursor.execute("SELECT DISTINCT file_type FROM files ORDER BY file_type LIMIT 1000")
            file_types = cursor.fetchall()
            print(f"Found {len(file_types)} distinct file types (Top 1000).")
            for ft in file_types:
                if ft[0]:  # Ensure file_type is not None or empty
                    self.file_type_combo.addItem(ft[0])
            conn.close()
            print("File Type ComboBox populated successfully.")
        except sqlite3.OperationalError as e:
            print(f"Error populating file type combo: {e}")
            self.show_error(f"Database Error: {e}")
        except Exception as e:
            print(f"Unexpected error populating file type combo: {e}")
            self.show_error(f"Unexpected Error: {e}")

    def perform_search(self):
        """Generate and execute the SQL query based on the filters, then display the results."""
        print("\nPerforming search with the following criteria:")
        name = self.name_input.text().strip()
        size = self.size_input.text().strip()
        drive = self.drive_combo.currentText()
        file_type = self.file_type_combo.currentText()

        # Handle Modified Date based on the checkbox
        if self.date_checkbox.isChecked():
            modified_date = self.date_edit.date().toString("yyyy-MM-dd")
            print(f"Modified After: '{modified_date}'")
        else:
            modified_date = None
            print("Modified After: Not applied")

        print(f"Name: '{name}'")
        print(f"Size > (bytes): '{size}'")
        print(f"Drive: '{drive}'")
        print(f"File Type: '{file_type}'")

        # Modify the query to search within 'full_path' instead of 'file_name'
        query = "SELECT full_path, drive, size, file_type, modified_date FROM files"
        conditions = []
        params = []

        if name:
            conditions.append("full_path LIKE ?")
            params.append(f"%{name}%")
            print(f"Added condition: full_path LIKE '%{name}%'")
        if size:
            try:
                size_int = int(size)
                conditions.append("size > ?")
                params.append(size_int)
                print(f"Added condition: size > {size_int}")
            except ValueError:
                print("Error: Size must be a number representing bytes.")
                self.show_error("Size must be a number representing bytes.")
                return
        if drive != "Any":
            conditions.append("drive = ?")
            params.append(drive)
            print(f"Added condition: drive = '{drive}'")
        if modified_date:
            conditions.append("DATE(modified_date) > DATE(?)")
            params.append(modified_date)
            print(f"Added condition: DATE(modified_date) > DATE('{modified_date}')")
        if file_type != "Any":
            conditions.append("file_type = ?")
            params.append(file_type)
            print(f"Added condition: file_type = '{file_type}'")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            print(f"Final Query with WHERE clause: {query}")
        else:
            print("Final Query without WHERE clause.")

        query += " ORDER BY modified_date DESC"
        print(f"Final Query after ORDER BY: {query}")
        print(f"Parameters: {params}")

        # **Show Progress Bar**
        self.progress_bar.setVisible(True)

        start_time = time.time()
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            print("Executing query...")
            cursor.execute(query, params)
            results = cursor.fetchall()
            conn.close()
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Query executed successfully in {elapsed_time:.4f} seconds.")
            print(f"Number of results fetched: {len(results)}")
            self.display_results(results)
        except Exception as e:
            print(f"Error executing query: {e}")
            self.show_error(f"Error executing query: {e}")
        finally:
            # **Hide Progress Bar**
            self.progress_bar.setVisible(False)

    def display_results(self, results):
        """Display the query results in the table widget."""
        print("Displaying results in the table...")
        self.results_table.setRowCount(0)  # Clear existing results

        for row_data in results:
            full_path = row_data[0]
            drive = row_data[1]
            size = row_data[2]
            file_type = row_data[3]
            modified_date = row_data[4]

            # Determine if the item is a folder based on 'file_type'
            is_folder = file_type.lower() == 'folder'

            # Extract the relative path after the colon
            if ':' in full_path:
                _, _, relative_path = full_path.partition(':')
            else:
                relative_path = full_path  # Fallback if ':' not present

            relative_path = relative_path.lstrip('/\\')

            if is_folder:
                display_name = os.path.join(drive, relative_path)
            else:
                display_name = os.path.basename(full_path)

            row_number = self.results_table.rowCount()
            self.results_table.insertRow(row_number)

            # File Name or Folder Path
            file_name_item = QTableWidgetItem(display_name)
            file_name_item.setToolTip(full_path)  # Set tooltip with full path
            file_name_item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)  # Align left
            file_name_item.setFlags(file_name_item.flags() | Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self.results_table.setItem(row_number, 0, file_name_item)

            # Drive
            drive_item = QTableWidgetItem(drive)
            drive_item.setTextAlignment(Qt.AlignCenter | Qt.AlignVCenter)
            self.results_table.setItem(row_number, 1, drive_item)

            # Size (bytes)
            size_item = QTableWidgetItem(str(size))
            size_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.results_table.setItem(row_number, 2, size_item)

            # File Type
            file_type_item = QTableWidgetItem(file_type)
            file_type_item.setTextAlignment(Qt.AlignCenter | Qt.AlignVCenter)
            self.results_table.setItem(row_number, 3, file_type_item)

            # Modified Date
            modified_date_item = QTableWidgetItem(modified_date)
            modified_date_item.setTextAlignment(Qt.AlignCenter | Qt.AlignVCenter)
            self.results_table.setItem(row_number, 4, modified_date_item)

            # Set tooltip for the entire row
            for column in range(self.results_table.columnCount()):
                self.results_table.item(row_number, column).setToolTip(full_path)

        # Prevent the "File Name" column from resizing when data loads
        # Ensure that column widths remain as set in adjust_column_widths

        # Remove resizeColumnsToContents to prevent dynamic resizing
        # self.results_table.resizeColumnsToContents()  # Already removed

        print("Results displayed successfully.")

        if not results:
            print("No results found.")
            QMessageBox.information(self, "No Results", "No files found matching the search criteria.")

        # **Ensure that the text area is hidden when displaying search results**
        self.update_output_text.hide()
        self.results_table.show()

    def clear_filters(self):
        """Clear all search filters."""
        print("Clearing all filters...")
        self.name_input.clear()
        self.size_input.clear()
        self.drive_combo.setCurrentIndex(0)
        self.file_type_combo.setCurrentIndex(0)
        self.date_checkbox.setChecked(False)
        self.date_edit.setDate(QDate.currentDate())
        print("All filters cleared.")

    def show_error(self, message):
        """Display an error message to the user."""
        print(f"Displaying error message: {message}")
        QMessageBox.critical(self, "Error", message)

    def resizeEvent(self, event):
        """Handle window resize events to adjust column widths."""
        super().resizeEvent(event)
        self.adjust_column_widths()

    def adjust_column_widths(self):
        """Adjust the column widths based on the current window width."""
        total_width = self.results_table.viewport().width()
        full_name_width = int(total_width * 0.7)
        remaining_width = total_width - full_name_width

        # Assuming 4 remaining columns: Drive, Size (bytes), File Type, Modified Date
        # Distribute remaining width equally
        other_columns = ["Drive", "Size (bytes)", "File Type", "Modified Date"]
        num_other_columns = len(other_columns)
        if num_other_columns == 0:
            return
        each_other_width = int(remaining_width / num_other_columns)

        # Set the width for each column
        self.results_table.setColumnWidth(0, full_name_width)  # File Name or Folder Path

        for i in range(1, self.results_table.columnCount()):
            self.results_table.setColumnWidth(i, each_other_width)

    def showEvent(self, event):
        """Handle the show event to adjust column widths initially."""
        super().showEvent(event)
        self.adjust_column_widths()

    def open_context_menu(self, position: QPoint):
        """Open a contextual menu on right-click with options to copy paths and show folder."""
        # Get the index of the item that was clicked
        index = self.results_table.indexAt(position)
        if not index.isValid():
            return

        row = index.row()

        # Retrieve data from the selected row
        file_name_item = self.results_table.item(row, 0)
        drive_item = self.results_table.item(row, 1)
        size_item = self.results_table.item(row, 2)
        file_type_item = self.results_table.item(row, 3)
        modified_date_item = self.results_table.item(row, 4)

        if not file_name_item:
            return

        # Assuming that if display_name is full_path (for folders), extract accordingly
        display_name = file_name_item.text()
        full_path = file_name_item.toolTip()

        # Extract file name
        if ':' in full_path:
            _, _, relative_path = full_path.partition(':')
            relative_path = relative_path.lstrip('/\\')
        else:
            relative_path = full_path  # Fallback if ':' not present

        is_folder = file_type_item.text().lower() == 'folder'

        if is_folder:
            nautilus_path = os.path.join(self.drives_dir, drive_item.text(), relative_path)
        else:
            nautilus_path = os.path.dirname(os.path.join(self.drives_dir, drive_item.text(), relative_path))

        # Create the context menu
        context_menu = QMenu(self)

        copy_fullpath_action = context_menu.addAction("Copy FullPath")
        copy_filename_action = context_menu.addAction("Copy FileName")
        copy_file_action = context_menu.addAction("Copy File")
        show_folder_action = context_menu.addAction("Show Folder")  # New action

        # Execute the context menu and get the selected action
        selected_action = context_menu.exec_(self.results_table.viewport().mapToGlobal(position))

        if selected_action == copy_fullpath_action:
            self.copy_fullpath(full_path)
        elif selected_action == copy_filename_action:
            file_name = os.path.basename(full_path.split(':', 1)[-1]) if ':' in full_path else full_path
            self.copy_filename(file_name)
        elif selected_action == copy_file_action:
            self.copy_file(full_path)
        elif selected_action == show_folder_action:
            self.show_folder(row)

    def copy_fullpath(self, full_path: str):
        """Copy the full path to the clipboard."""
        clipboard = QApplication.clipboard()
        clipboard.setText(full_path)
        print(f"Copied FullPath to clipboard: {full_path}")

    def copy_filename(self, file_name: str):
        """Copy the file name to the clipboard."""
        clipboard = QApplication.clipboard()
        clipboard.setText(file_name)
        print(f"Copied FileName to clipboard: {file_name}")

    def copy_file(self, full_path: str):
        """Copy the file or folder as a file reference to the clipboard."""
        mime_data = QMimeData()
        # Convert the path to a QUrl
        file_url = QUrl.fromLocalFile(full_path)
        mime_data.setUrls([file_url])
        clipboard = QApplication.clipboard()
        clipboard.setMimeData(mime_data)
        print(f"Copied File to clipboard: {full_path}")

    def show_folder(self, row=None):
        """
        Show the selected folder in Nemo File Manager.
        If the drive is 'localdrive', open directly without mounting.
        Otherwise, perform mounting as needed and then open.
        """
        print("Attempting to show folder in Nemo...")

        if row is None:
            # If row is not provided, get the currently selected row
            selected_items = self.results_table.selectedItems()
            if not selected_items:
                self.show_error("No row selected.")
                return
            row = selected_items[0].row()

        # Retrieve data from the selected row
        file_name_item = self.results_table.item(row, 0)
        drive_item = self.results_table.item(row, 1)
        file_type_item = self.results_table.item(row, 3)

        if not file_name_item or not drive_item or not file_type_item:
            self.show_error("Invalid row data.")
            return

        full_path = file_name_item.toolTip()
        drive_name = drive_item.text()
        is_folder = file_type_item.text().lower() == 'folder'

        print(f"Selected Drive: {drive_name}")
        print(f"Full Path: {full_path}")

        # Extract the relative path after the colon
        if ':' in full_path:
            _, _, relative_path = full_path.partition(':')
        else:
            relative_path = full_path  # Fallback if ':' not present

        relative_path = relative_path.lstrip('/\\')

        # Determine if the drive is 'localdrive'
        if drive_name.lower() == "localdrive":
            # Construct the local path
            if is_folder:
                local_path = os.path.join(self.drives_dir, relative_path)
            else:
                local_path = os.path.dirname(os.path.join(self.drives_dir, relative_path))
            
            print(f"Local path to open: {local_path}")

            # Verify that the path exists
            if not os.path.exists(local_path):
                self.show_error(f"The path '{local_path}' does not exist.")
                return

            # Open Nemo at the specific path
            try:
                print(f"Opening Nemo at '{local_path}'...")
                subprocess.Popen(['nemo', local_path])
                print("Nemo opened successfully.")
            except Exception as e:
                self.show_error(f"Failed to open Nemo: {e}")
                return
        else:
            # Handle non-local drives (remote drives)
            # Create subdirectory for the drive if it doesn't exist
            drive_mount_path = os.path.join(self.drives_dir, drive_name)
            if not os.path.exists(drive_mount_path):
                try:
                    os.makedirs(drive_mount_path, exist_ok=True)
                    print(f"Created directory for drive '{drive_name}' at: {drive_mount_path}")
                except Exception as e:
                    self.show_error(f"Failed to create drive directory: {e}")
                    return

            # Check if the drive is already mounted by checking if the directory is empty
            # If empty, mount the drive
            if not os.listdir(drive_mount_path):
                print(f"Directory '{drive_mount_path}' is empty. Attempting to mount the drive.")
                # Parse rclone.config to find the remote matching the drive_name
                config = configparser.ConfigParser()
                if not os.path.exists(self.rclone_config_path):
                    self.show_error(f"rclone.config not found at {self.rclone_config_path}.")
                    return

                try:
                    config.read(self.rclone_config_path)
                    if drive_name not in config.sections():
                        self.show_error(f"Drive '{drive_name}' not found in rclone.config.")
                        return
                    remote = drive_name  # Assuming the section name matches the remote name
                    print(f"Found remote '{remote}' in rclone.config.")
                except Exception as e:
                    self.show_error(f"Error parsing rclone.config: {e}")
                    return

                # Mount the drive using rclone
                mount_command = [
                    "rclone",
                    "mount",
                    remote,
                    drive_mount_path,
                    "--daemon"  # Run in the background
                ]

                try:
                    print(f"Mounting drive with command: {' '.join(mount_command)}")
                    subprocess.Popen(mount_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    print(f"Drive '{remote}' mounted at '{drive_mount_path}'.")
                    # Wait briefly to ensure the mount has time to establish
                    time.sleep(5)
                except Exception as e:
                    self.show_error(f"Failed to mount drive '{remote}': {e}")
                    return

            # Construct the path to open in Nemo
            if is_folder:
                nemo_path = os.path.join(self.drives_dir, drive_name, relative_path)
            else:
                nemo_path = os.path.dirname(os.path.join(self.drives_dir, drive_name, relative_path))

            print(f"Constructed Nemo path: {nemo_path}")

            # Verify that the path exists
            if not os.path.exists(nemo_path):
                self.show_error(f"The path '{nemo_path}' does not exist.")
                return

            # Open Nemo at the specific path
            try:
                print(f"Opening Nemo at '{nemo_path}'...")
                subprocess.Popen(['nemo', nemo_path])
                print("Nemo opened successfully.")
            except Exception as e:
                self.show_error(f"Failed to open Nemo: {e}")
                return

    def update_index(self):
        """Handle the Update Index button click to execute rclone_list_files.py and display output."""
        print("\nUpdate Index button clicked.")

        script_path = self.search_index_script_path

        if not os.path.isfile(script_path):
            print(f"Script not found at {script_path}. Prompting user to locate the script.")
            QMessageBox.warning(
                self, 
                "Script Not Found",
                f"Update script not found at {script_path}.\nPlease locate the 'rclone_list_files.py' script."
            )
            options = QFileDialog.Options()
            options |= QFileDialog.ReadOnly
            file_path, _ = QFileDialog.getOpenFileName(
                self,
                "Locate rclone_list_files.py",
                "",
                "Python Files (*.py);;All Files (*)",
                options=options
            )
            if file_path:
                script_path = file_path
                self.search_index_script_path = script_path  # Update the path
                print(f"User selected script at {script_path}.")
            else:
                print("User did not select a script. Aborting Update Index.")
                QMessageBox.information(
                    self, 
                    "Update Index", 
                    "No script selected. Update Index aborted."
                )
                return

        # Check if the script is executable or can be run with python3
        if not os.access(script_path, os.X_OK):
            print(f"Script at {script_path} is not executable. Attempting to run with python3.")

        # **Replace the table with the text area**
        self.results_table.hide()
        self.update_output_text.show()
        self.update_output_text.clear()
        self.update_output_text.append(f"Executing: {' '.join(['python3', script_path])}\n\n")

        # Initialize and start the worker thread
        self.thread = ScriptRunner(script_path)
        self.thread.output_signal.connect(self.append_output)
        self.thread.error_signal.connect(self.append_error)
        self.thread.finished_signal.connect(self.handle_script_finished)
        self.thread.start()

    def append_output(self, text):
        """Append standard output to the text area."""
        self.update_output_text.append(text)

    def append_error(self, text):
        """Append error output to the text area."""
        # Display errors in red
        self.update_output_text.append(f"<span style='color:red;'>{text}</span>")

    def handle_script_finished(self, return_code):
        """Handle the completion of the script execution."""
        if return_code == 0:
            print("Script executed successfully.")
            QMessageBox.information(self, "Update Index", "Index updated successfully.")
        else:
            print(f"Script executed with errors. Return code: {return_code}")
            QMessageBox.critical(
                self, 
                "Update Index Failed",
                f"An error occurred while updating the index.\nReturn Code: {return_code}"
            )

    # **Optional Method: Refresh Filters After Update**
    def refresh_filters(self):
        """Refresh the drive and file type combo boxes after updating the index."""
        print("Refreshing Drive and File Type filters after index update.")
        self.drive_combo.clear()
        self.drive_combo.addItem("Any")
        self.populate_drive_combo()

        self.file_type_combo.clear()
        self.file_type_combo.addItem("Any")
        self.populate_file_type_combo()
    # **End of Optional Method**

def main():
    app = QApplication(sys.argv)
    window = FileSearchApp()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
