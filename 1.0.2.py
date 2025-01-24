import os
import platform
import threading
from datetime import datetime
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import pandas as pd
import pytz
from influxdb import InfluxDBClient

# =======================================================
# InfluxDB Query Functionality (Tab 1)
# =======================================================

def load_measurements_fields(filename):
    """
    Load measurement fields from a file.
    Expected file format:
      - A measurement block starts with a non-indented line: measurement_name<tab>first_field
      - Subsequent indented lines contain additional field names.
      - Blocks are separated by blank lines.
    Returns a dictionary mapping measurement names to lists of field keys.
    """
    measurements_fields = {}
    current_measurement = None

    try:
        with open(filename, 'r') as f:
            for line in f:
                stripped_line = line.strip()
                # Skip blank lines
                if not stripped_line:
                    current_measurement = None
                    continue

                if line[0].isspace() and current_measurement:
                    field = stripped_line
                    measurements_fields[current_measurement].append(field)
                else:
                    parts = stripped_line.split('\t')
                    if len(parts) >= 2:
                        current_measurement = parts[0]
                        measurements_fields[current_measurement] = [parts[1]]
                    else:
                        current_measurement = parts[0]
                        measurements_fields[current_measurement] = []
    except Exception as e:
        raise Exception(f"Error reading measurements file: {e}")

    return measurements_fields

def fetch_measurements_and_fields(client):
    """
    Retrieve all measurements and their field keys from InfluxDB.
    Returns a dictionary where keys are measurement names and values are lists of field keys.
    """
    measurements_query = 'SHOW MEASUREMENTS'
    measurements_result = client.query(measurements_query)
    measurements = list(measurements_result.get_points())

    measurements_fields = {}
    for measurement in measurements:
        measurement_name = measurement['name']
        fields_query = f'SHOW FIELD KEYS FROM "{measurement_name}"'
        fields_result = client.query(fields_query)
        fields = list(fields_result.get_points())
        field_names = [field['fieldKey'] for field in fields]
        measurements_fields[measurement_name] = field_names

    return measurements_fields

def get_count_for_field(client, measurement, field, vehicle_id, start_dt, end_dt, output_func):
    """
    Count non-null entries for a specific field in a measurement for a given vehicle and time period.
    """
    query = f'''
    SELECT COUNT("{field}") 
    FROM "{measurement}"
    WHERE vehicle_id='{vehicle_id}' 
      AND time >= '{start_dt.isoformat()}'
      AND time < '{end_dt.isoformat()}'
    '''
    output_func(f"Executing query:\n{query}\n")
    try:
        result = client.query(query)
        points = list(result.get_points())
    except Exception as e:
        output_func(f"Error executing query for {measurement}.{field}: {e}\n")
        return 0

    if points:
        for key in points[0]:
            if key.startswith("count"):
                count_val = points[0][key]
                output_func(f"Count for {measurement}.{field}: {count_val}\n")
                return count_val
    return 0

def run_queries(params, output_func):
    """
    Connects to InfluxDB, loads measurement fields (from CSV or dynamically), performs queries,
    and then saves the results to a CSV file.
    """
    try:
        client = InfluxDBClient(
            host=params['host'],
            port=int(params['port']),
            username=params['username'],
            password=params['password'],
            database=params['database']
        )
        output_func("Connected to InfluxDB.\n")
    except Exception as e:
        output_func(f"Failed to connect to InfluxDB: {e}\n")
        return

    try:
        local_tz = pytz.timezone(params['timezone'])
        start_dt = pd.Timestamp(f"{params['start_date']} {params['start_time']}").tz_localize(local_tz)
        end_dt = pd.Timestamp(f"{params['end_date']} {params['end_time']}").tz_localize(local_tz)
    except Exception as e:
        output_func(f"Error creating timestamps: {e}\n")
        return

    vehicle_id = params['vehicle_id']

    # Load measurements from CSV file if selected; otherwise query InfluxDB
    if params['use_csv']:
        csv_filename = params['csv_filename']
        if not os.path.exists(csv_filename):
            output_func(f"CSV file '{csv_filename}' not found.\n")
            return
        try:
            measurements_fields = load_measurements_fields(csv_filename)
            output_func("Loaded measurements from CSV file.\n")
        except Exception as e:
            output_func(f"Error loading CSV: {e}\n")
            return
    else:
        try:
            measurements_fields = fetch_measurements_and_fields(client)
            output_func("Fetched measurements and fields from InfluxDB.\n")
        except Exception as e:
            output_func(f"Error fetching measurements/fields: {e}\n")
            return

    results_list = []
    for measurement, fields in measurements_fields.items():
        output_func(f"\nProcessing Measurement: {measurement}\n")
        if not fields:
            output_func("  No fields found for this measurement.\n")
            continue
        for field in fields:
            count_value = get_count_for_field(client, measurement, field, vehicle_id, start_dt, end_dt, output_func)
            combined_key = f"{measurement}.count_{field}"
            results_list.append({
                'Measurement_Field': combined_key,
                'Count': count_value
            })

    if results_list:
        df = pd.DataFrame(results_list)
        csv_output = (f"measurements_field_counts_{vehicle_id}_"
                      f"{params['start_date'].replace('-', '')}_"
                      f"{params['start_time'].replace(':', '')}_to_"
                      f"{params['end_time'].replace(':', '')}.csv")
        try:
            df.to_csv(csv_output, index=False)
            output_func(f"\nData saved to {csv_output}\n")
        except Exception as e:
            output_func(f"Error saving CSV file: {e}\n")
    else:
        output_func("No data to save.\n")

class InfluxDBQueryFrame(tk.Frame):
    """
    A Frame widget that encapsulates the InfluxDB Query interface.
    """
    def __init__(self, parent):
        super().__init__(parent)
        self.create_widgets()

    def create_widgets(self):
        # --- InfluxDB Connection Parameters ---
        frame_conn = ttk.LabelFrame(self, text="InfluxDB Connection")
        frame_conn.grid(column=0, row=0, padx=10, pady=5, sticky="W")
        self.host_var = tk.StringVar(value="104.154.190.81")
        self.port_var = tk.StringVar(value="15086")
        self.username_var = tk.StringVar(value="boson_hmi")
        self.password_var = tk.StringVar(value="hmi@boson76$")
        self.database_var = tk.StringVar(value="HMI_test")
        ttk.Label(frame_conn, text="Host:").grid(column=0, row=0, sticky="W")
        ttk.Entry(frame_conn, width=20, textvariable=self.host_var).grid(column=1, row=0, padx=5, pady=2)
        ttk.Label(frame_conn, text="Port:").grid(column=0, row=1, sticky="W")
        ttk.Entry(frame_conn, width=20, textvariable=self.port_var).grid(column=1, row=1, padx=5, pady=2)
        ttk.Label(frame_conn, text="Username:").grid(column=0, row=2, sticky="W")
        ttk.Entry(frame_conn, width=20, textvariable=self.username_var).grid(column=1, row=2, padx=5, pady=2)
        ttk.Label(frame_conn, text="Password:").grid(column=0, row=3, sticky="W")
        ttk.Entry(frame_conn, width=20, textvariable=self.password_var, show="*").grid(column=1, row=3, padx=5, pady=2)
        ttk.Label(frame_conn, text="Database:").grid(column=0, row=4, sticky="W")
        ttk.Entry(frame_conn, width=20, textvariable=self.database_var).grid(column=1, row=4, padx=5, pady=2)

        # --- Filtering Parameters ---
        frame_filter = ttk.LabelFrame(self, text="Filtering Parameters")
        frame_filter.grid(column=0, row=1, padx=10, pady=5, sticky="W")
        self.vehicle_id_var = tk.StringVar(value="VT-Box-T1")
        self.start_date_var = tk.StringVar(value="2025-01-08")
        self.end_date_var = tk.StringVar(value="2025-01-08")
        self.start_time_var = tk.StringVar(value="11:11:59")
        self.end_time_var = tk.StringVar(value="12:12:00")
        self.timezone_var = tk.StringVar(value="Asia/Kolkata")
        ttk.Label(frame_filter, text="Vehicle ID:").grid(column=0, row=0, sticky="W")
        ttk.Entry(frame_filter, width=20, textvariable=self.vehicle_id_var).grid(column=1, row=0, padx=5, pady=2)
        ttk.Label(frame_filter, text="Start Date (YYYY-MM-DD):").grid(column=0, row=1, sticky="W")
        ttk.Entry(frame_filter, width=20, textvariable=self.start_date_var).grid(column=1, row=1, padx=5, pady=2)
        ttk.Label(frame_filter, text="Start Time (HH:MM:SS):").grid(column=0, row=2, sticky="W")
        ttk.Entry(frame_filter, width=20, textvariable=self.start_time_var).grid(column=1, row=2, padx=5, pady=2)
        ttk.Label(frame_filter, text="End Date (YYYY-MM-DD):").grid(column=0, row=3, sticky="W")
        ttk.Entry(frame_filter, width=20, textvariable=self.end_date_var).grid(column=1, row=3, padx=5, pady=2)
        ttk.Label(frame_filter, text="End Time (HH:MM:SS):").grid(column=0, row=4, sticky="W")
        ttk.Entry(frame_filter, width=20, textvariable=self.end_time_var).grid(column=1, row=4, padx=5, pady=2)
        ttk.Label(frame_filter, text="Timezone:").grid(column=0, row=5, sticky="W")
        ttk.Entry(frame_filter, width=20, textvariable=self.timezone_var).grid(column=1, row=5, padx=5, pady=2)

        # --- Measurement Source Options ---
        frame_csv = ttk.LabelFrame(self, text="Measurement Source Options")
        frame_csv.grid(column=0, row=2, padx=10, pady=5, sticky="W")
        self.use_csv_var = tk.BooleanVar(value=True)
        self.csv_filename_var = tk.StringVar(value="")
        ttk.Checkbutton(frame_csv, text="Use CSV for Measurement Fields", variable=self.use_csv_var).grid(column=0, row=0, sticky="W", padx=5, pady=2)
        ttk.Label(frame_csv, text="CSV Filename:").grid(column=0, row=1, sticky="W", padx=5)
        csv_filename_entry = ttk.Entry(frame_csv, width=40, textvariable=self.csv_filename_var)
        csv_filename_entry.grid(column=1, row=1, padx=5, pady=2)
        select_csv_btn = ttk.Button(frame_csv, text="Select File", command=self.select_csv_file)
        select_csv_btn.grid(column=2, row=1, padx=5, pady=2)

        # --- Run Button and Output Display ---
        self.run_button = ttk.Button(self, text="Run Query", command=self.on_run_query)
        self.run_button.grid(column=0, row=3, padx=10, pady=5, sticky="W")
        self.output_text = scrolledtext.ScrolledText(self, width=90, height=15, wrap=tk.WORD)
        self.output_text.grid(column=0, row=4, padx=10, pady=5)

    def select_csv_file(self):
        filename = filedialog.askopenfilename(
            title="Select Measurement Fields File",
            filetypes=[("Text Files", "*.txt"), ("CSV Files", "*.csv"), ("All Files", "*.*")]
        )
        if filename:
            self.csv_filename_var.set(filename)

    def append_output(self, message):
        self.output_text.insert(tk.END, message)
        self.output_text.see(tk.END)

    def on_run_query(self):
        self.output_text.delete(1.0, tk.END)
        self.run_button.config(state=tk.DISABLED)
        params = {
            'host': self.host_var.get(),
            'port': self.port_var.get(),
            'username': self.username_var.get(),
            'password': self.password_var.get(),
            'database': self.database_var.get(),
            'vehicle_id': self.vehicle_id_var.get(),
            'start_date': self.start_date_var.get(),
            'end_date': self.end_date_var.get(),
            'start_time': self.start_time_var.get(),
            'end_time': self.end_time_var.get(),
            'timezone': self.timezone_var.get(),
            'use_csv': self.use_csv_var.get(),
            'csv_filename': self.csv_filename_var.get()
        }
        threading.Thread(target=self.run_query_thread, args=(params,)).start()

    def run_query_thread(self, params):
        try:
            run_queries(params, self.append_output)
        except Exception as e:
            self.append_output(f"Error during query execution: {e}\n")
        finally:
            self.run_button.config(state=tk.NORMAL)

# =======================================================
# Data Processor / Metadata Functionality (Tab 2)
# =======================================================

# Global variables for file paths and user input
input_file = None          # Input data file path
valid_file = None          # Valid file (checklist) path
save_file = None           # Save file path
user_hours = None          # User-specified hours for processing

# GUI widget variables (will be assigned in the DataProcessorFrame)
metadata_entries = {}
comments_text = None
hours_entry = None
input_file_label = None
valid_file_label = None
save_file_label = None
missing_input_label = None
missing_valid_label = None

TEMPLATE_FILE = "t4.csv"
metadata_fields = [
    "Uploaded By", "Test Data", "Test Time", "Test Conducted", "Test Device",
    "Boson App Version", "VT-Box Version", "VCU Hardware Version", "VCU Software Version",
    "MC Front Firmware Version", "MC Rear Firmware Version", "MC Front Profile Checksum",
    "MC Rear Profile Checksum", "BMS Firmware Version", "BMS Profile Checksum"
]

def get_file_path(prompt):
    file_path = filedialog.askopenfilename(
        title=prompt,
        filetypes=[
            ("Excel and CSV files", "*.xlsx *.xls *.csv"),
            ("Excel files", "*.xlsx *.xls"),
            ("CSV files", "*.csv")
        ]
    )
    if not file_path:
        messagebox.showerror("Error", "No file selected.")
        return None
    if not os.path.isfile(file_path):
        messagebox.showerror("Error", "Invalid file path.")
        return None
    return file_path

def get_save_path(prompt):
    save_path = filedialog.asksaveasfilename(
        title=prompt,
        defaultextension=".xlsx",
        filetypes=[("Excel files", "*.xlsx"), ("CSV files", "*.csv")]
    )
    if not save_path:
        messagebox.showerror("Error", "No save path selected.")
        return None
    return save_path

def check_valid_file(row, valid_data):
    if row['InfluxDB Field Name'] in valid_data['InfluxDB Field Name'].values:
        if pd.isnull(row['Value']):
            return 'No'
        return 'Yes'
    else:
        return 'No'

def validate_files(input_data, valid_data):
    required_columns = ['InfluxDB Field Name', 'CAN Dictionary MAP', 'Time ']
    if not all(column in valid_data.columns for column in required_columns):
        missing = ', '.join(set(required_columns) - set(valid_data.columns))
        raise ValueError(f"The valid file is missing the following columns: {missing}")
    return valid_data.drop_duplicates(subset='InfluxDB Field Name')

def map_values(input_data, valid_data):
    can_dict_map = valid_data.set_index('InfluxDB Field Name')['CAN Dictionary MAP'].to_dict()
    time_map = valid_data.set_index('InfluxDB Field Name')['Time '].to_dict()
    input_data['CAN Dictionary MAP'] = input_data['InfluxDB Field Name'].map(can_dict_map)
    input_data['Time '] = input_data['InfluxDB Field Name'].map(time_map)

def calculate_expected_count(input_data, user_hours):
    input_data['Expected Count'] = input_data.apply(
        lambda row: (60000 / row['Time ']) * 60 * user_hours if pd.notnull(row['Time ']) and row['Time '] > 0 else None,
        axis=1
    )
    if 'Value' in input_data.columns:
        input_data['Loss'] = input_data['Expected Count'] - input_data['Value']
        input_data['Percentage Loss'] = input_data.apply(
            lambda row: (row['Loss'] / row['Expected Count'] * 100)
            if pd.notnull(row['Loss']) and row['Expected Count'] > 0 else None,
            axis=1
        )

def load_metadata_from_csv():
    global metadata_entries, comments_text
    try:
        metadata_df = pd.read_csv(TEMPLATE_FILE)
        metadata_dict = metadata_df.set_index("Meta Data")["Meta Value"].to_dict()
        for field, entry in metadata_entries.items():
            if field in ["Missing in Valid File", "Missing in Input Data"]:
                continue
            entry.delete(0, tk.END)
            entry.insert(0, metadata_dict.get(field, ""))
        comments = metadata_dict.get("Comments", "")
        comments_text.delete("1.0", tk.END)
        comments_text.insert(tk.END, comments)
        messagebox.showinfo("Success", "Metadata loaded successfully!")
    except FileNotFoundError:
        messagebox.showerror("Error", "No metadata template found.")
    except Exception as e:
        messagebox.showerror("Error", f"An error occurred: {e}")

def reset_form():
    global input_file, valid_file, save_file, user_hours
    input_file = None
    valid_file = None
    save_file = None
    input_file_label.config(text="Input data file", foreground="gray")
    valid_file_label.config(text="Check list file", foreground="gray")
    save_file_label.config(text="Output file", foreground="gray")
    for field, entry in metadata_entries.items():
        entry.delete(0, tk.END)
    comments_text.delete("1.0", tk.END)
    hours_entry.delete(0, tk.END)
    missing_input_label.config(text="None", foreground="red")
    missing_valid_label.config(text="None", foreground="red")

def process_file():
    global input_file, valid_file, save_file, user_hours
    try:
        if not input_file or not valid_file or not save_file:
            messagebox.showerror("Error", "Please select all required files.")
            return

        try:
            user_hours = float(hours_entry.get())
            if user_hours <= 0:
                raise ValueError("Hours must be a positive number.")
        except ValueError:
            messagebox.showerror("Error", "Please enter a valid number of hours.")
            return

        input_data = pd.read_csv(input_file) if input_file.endswith('.csv') else pd.read_excel(input_file)
        valid_data = pd.read_csv(valid_file) if valid_file.endswith('.csv') else pd.read_excel(valid_file)
        valid_data = validate_files(input_data, valid_data)

        if 'Metric' not in input_data.columns:
            raise ValueError("The input file does not contain a 'Metric' column.")
        
        input_data[['Measurement', 'InfluxDB Field Name']] = input_data['Metric'].str.split('.', n=1, expand=True)
        if input_data['InfluxDB Field Name'].isnull().any():
            raise ValueError("Some 'Metric' entries do not contain a '.' to split.")
        input_data['InfluxDB Field Name'] = input_data['InfluxDB Field Name'].str.replace('count_', '', regex=False)
        input_data['Available in Valid File?'] = input_data.apply(lambda row: check_valid_file(row, valid_data), axis=1)

        missing_feaids_input_missing_valid = input_data[input_data['Available in Valid File?'] == 'No']['InfluxDB Field Name'].unique().tolist()
        missing_feaids_valid_missing_input = valid_data[~valid_data['InfluxDB Field Name'].isin(input_data['InfluxDB Field Name'])]['InfluxDB Field Name'].unique().tolist()

        map_values(input_data, valid_data)
        calculate_expected_count(input_data, user_hours)
        missing_input_label.config(text=", ".join(map(str, missing_feaids_input_missing_valid)) if missing_feaids_input_missing_valid else "None")
        missing_valid_label.config(text=", ".join(map(str, missing_feaids_valid_missing_input)) if missing_feaids_valid_missing_input else "None")

        metadata = {
            "Meta Data": ["Upload Time"] + metadata_fields + ["Comments"],
            "Meta Value": [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ] + [
                metadata_entries[field].get() for field in metadata_fields
            ] + [
                comments_text.get("1.0", tk.END).strip()
            ]
        }
        metadata_df = pd.DataFrame(metadata)

        if save_file.endswith(".xlsx"):
            with pd.ExcelWriter(save_file, engine='openpyxl') as writer:
                metadata_df.to_excel(writer, sheet_name='Metadata', index=False)
                input_data.to_excel(writer, sheet_name='Processed Data', index=False)
                pd.DataFrame({"Missing in Valid File": missing_feaids_input_missing_valid}).to_excel(writer, sheet_name='Missing in Valid File', index=False)
                pd.DataFrame({"Missing in Input Data": missing_feaids_valid_missing_input}).to_excel(writer, sheet_name='Missing in Input Data', index=False)
        elif save_file.endswith(".csv"):
            with open(save_file, 'w', newline='') as f:
                f.write("=== Metadata ===\n")
                metadata_df.to_csv(f, index=False)
                f.write('\n=== Processed Data ===\n')
                input_data.to_csv(f, index=False, header=True, lineterminator="\n")
                f.write('\n=== Missing in Valid File ===\n')
                pd.DataFrame({"Missing in Valid File": missing_feaids_input_missing_valid}).to_csv(f, index=False, header=True, lineterminator="\n")
                f.write('\n=== Missing in Input Data ===\n')
                pd.DataFrame({"Missing in Input Data": missing_feaids_valid_missing_input}).to_csv(f, index=False, header=True, lineterminator="\n")
        else:
            raise ValueError("Unsupported save file format. Use '.xlsx' or '.csv'.")

        messagebox.showinfo("Success", f"Updated file saved as {save_file}")
        open_saved_file(save_file)
        reset_form()

    except Exception as e:
        messagebox.showerror("Error", f"An error occurred: {e}")
        print(e)

def save_metadata_to_csv():
    try:
        metadata = {
            "Meta Data": metadata_fields[:-2] + ["Comments"],
            "Meta Value": [metadata_entries[field].get() for field in metadata_fields[:-2]] + [comments_text.get("1.0", tk.END).strip()]
        }
        pd.DataFrame(metadata).to_csv(TEMPLATE_FILE, index=False)
        messagebox.showinfo("Success", "Metadata template saved successfully!")
    except Exception as e:
        messagebox.showerror("Error", f"An error occurred: {e}")

def open_saved_file(file_path):
    try:
        system_platform = platform.system()
        if system_platform == "Windows":
            os.startfile(file_path)
        elif system_platform == "Darwin":
            os.system(f"open \"{file_path}\"")
        else:
            os.system(f"xdg-open \"{file_path}\"")
    except Exception as e:
        messagebox.showerror("Error", f"Could not open the file: {e}")

def select_input_file():
    global input_file
    input_file = get_file_path("Select the input Excel or CSV file")
    if input_file:
        input_file_label.config(text=os.path.basename(input_file), foreground="black")

def select_valid_file():
    global valid_file
    valid_file = get_file_path("Select the file containing valid InfluxDB Field Names")
    if valid_file:
        valid_file_label.config(text=os.path.basename(valid_file), foreground="black")

def select_save_file():
    global save_file
    save_file = get_save_path("Select where to save the updated file")
    if save_file:
        save_file_label.config(text=os.path.basename(save_file), foreground="black")

class DataProcessorFrame(tk.Frame):
    """
    A Frame widget that encapsulates the Data Processor / Metadata interface.
    """
    def __init__(self, parent):
        super().__init__(parent)
        self.create_widgets()

    def create_widgets(self):
        global hours_entry, input_file_label, valid_file_label, save_file_label
        global comments_text, missing_input_label, missing_valid_label, metadata_entries

        # --- File Selection ---
        file_frame = ttk.LabelFrame(self, text="Input Files")
        file_frame.grid(row=0, column=0, sticky="ew", padx=5, pady=5)
        input_file_label = ttk.Label(file_frame, text="Input data file", foreground="gray")
        input_file_label.grid(row=0, column=0, sticky="w", padx=5, pady=2)
        ttk.Button(file_frame, text="Select", command=select_input_file).grid(row=0, column=1, padx=5, pady=2)
        valid_file_label = ttk.Label(file_frame, text="Check list file", foreground="gray")
        valid_file_label.grid(row=1, column=0, sticky="w", padx=5, pady=2)
        ttk.Button(file_frame, text="Select", command=select_valid_file).grid(row=1, column=1, padx=5, pady=2)
        save_file_label = ttk.Label(file_frame, text="Output file", foreground="gray")
        save_file_label.grid(row=2, column=0, sticky="w", padx=5, pady=2)
        ttk.Button(file_frame, text="Select", command=select_save_file).grid(row=2, column=1, padx=5, pady=2)

        # --- Metadata Entries ---
        metadata_frame = ttk.LabelFrame(self, text="Metadata")
        metadata_frame.grid(row=1, column=0, sticky="ew", padx=5, pady=5)
        for idx, field in enumerate(metadata_fields):
            ttk.Label(metadata_frame, text=field + ":").grid(row=idx, column=0, sticky="w", padx=5, pady=2)
            entry = ttk.Entry(metadata_frame, width=50)
            entry.grid(row=idx, column=1, padx=5, pady=2)
            metadata_entries[field] = entry

        missing_input_label = ttk.Label(metadata_frame, text="None", foreground="red")
        missing_valid_label = ttk.Label(metadata_frame, text="None", foreground="red")
        ttk.Label(metadata_frame, text="Missing in Valid File:").grid(row=len(metadata_fields)+1, column=0, sticky="w", padx=5, pady=2)
        missing_input_label.grid(row=len(metadata_fields)+1, column=1, sticky="w", padx=5, pady=2)
        ttk.Label(metadata_frame, text="Missing in Input Data:").grid(row=len(metadata_fields)+2, column=0, sticky="w", padx=5, pady=2)
        missing_valid_label.grid(row=len(metadata_fields)+2, column=1, sticky="w", padx=5, pady=2)

        # --- Comments ---
        comments_frame = ttk.LabelFrame(self, text="Comments")
        comments_frame.grid(row=2, column=0, sticky="ew", padx=5, pady=5)
        comments_text = scrolledtext.ScrolledText(comments_frame, width=60, height=5)
        comments_text.grid(row=0, column=0, padx=5, pady=5)

        # --- User Input Hours ---
        user_frame = ttk.LabelFrame(self, text="User Input")
        user_frame.grid(row=3, column=0, sticky="ew", padx=5, pady=5)
        ttk.Label(user_frame, text="Enter Hours:").grid(row=0, column=0, padx=5, pady=2)
        hours_entry = ttk.Entry(user_frame, width=20)
        hours_entry.grid(row=0, column=1, padx=5, pady=2)

        # --- Action Buttons ---
        action_frame = ttk.Frame(self)
        action_frame.grid(row=4, column=0, sticky="ew", padx=5, pady=5)
        ttk.Button(action_frame, text="Process File", command=process_file).pack(side="left", padx=5, pady=5)
        ttk.Button(action_frame, text="Load Metadata", command=load_metadata_from_csv).pack(side="left", padx=5, pady=5)
        ttk.Button(action_frame, text="Save Metadata", command=save_metadata_to_csv).pack(side="left", padx=5, pady=5)
        ttk.Button(action_frame, text="Exit", command=self.master.destroy).pack(side="right", padx=5, pady=5)

# =======================================================
# Main Application with Notebook Tabs
# =======================================================

class CombinedApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Combined InfluxDB Query and Data Processor Tool")
        self.geometry("900x700")
        self.create_tabs()

    def create_tabs(self):
        notebook = ttk.Notebook(self)
        notebook.pack(expand=True, fill="both")

        influx_tab = InfluxDBQueryFrame(notebook)
        processor_tab = DataProcessorFrame(notebook)

        notebook.add(influx_tab, text="InfluxDB Query")
        notebook.add(processor_tab, text="Data Processor")

if __name__ == "__main__":
    app = CombinedApp()
    app.mainloop()
