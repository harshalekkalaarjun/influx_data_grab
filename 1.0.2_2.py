import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog, messagebox
from influxdb import InfluxDBClient
import pandas as pd
import pytz
import threading
import os
from datetime import datetime
import logging

# ==================================================
# Helper Functions for Loading Measurement Fields
# ==================================================

def load_measurements_fields(filename):
    """
    Load measurement fields from a file.

    Expected file format:
      - A measurement block starts with a non-indented line containing:
            measurement_name<tab>first_field
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

                # If the line begins with whitespace and we already have a measurement,
                # treat it as an additional field.
                if line[0].isspace() and current_measurement:
                    field = stripped_line
                    measurements_fields[current_measurement].append(field)
                else:
                    # Otherwise, assume it's the start of a new measurement block.
                    # Expect at least two parts, measurement name and the first field.
                    parts = stripped_line.split('\t')
                    if len(parts) >= 2:
                        current_measurement = parts[0]
                        measurements_fields[current_measurement] = [parts[1]]
                    else:
                        # In case there is only one part, create an entry with an empty field list.
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

def get_count_for_field(client, measurement, field, vehicle_id, start_dt, end_dt, logger):
    """
    Count non-null entries for a specific field in a measurement given vehicle and time filters.
    Returns the count value (an integer).
    """
    query = f'''
    SELECT COUNT("{field}") 
    FROM "{measurement}"
    WHERE vehicle_id='{vehicle_id}' 
      AND time >= '{start_dt.isoformat()}Z'
      AND time < '{end_dt.isoformat()}Z'
    '''
    logger.debug(f"Executing query:\n{query}")
    try:
        result = client.query(query)
        points = list(result.get_points())
    except Exception as e:
        logger.error(f"Error executing query for {measurement}.{field}: {e}")
        return 0

    # The count value may be stored in a key like 'count' or 'count_<field>'.
    if points:
        for key in points[0]:
            if key.startswith("count"):
                count_val = points[0][key]
                logger.debug(f"Count for {measurement}.{field}: {count_val}")
                return count_val
    return 0

# ==================================================
# Custom Logging Handler for Tkinter ScrolledText
# ==================================================

class TextHandler(logging.Handler):
    """
    This class allows logging to a Tkinter Text or ScrolledText widget.
    """
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)
        def append():
            self.text_widget.configure(state='normal')
            self.text_widget.insert(tk.END, msg + '\n')
            self.text_widget.configure(state='disabled')
            self.text_widget.see(tk.END)
        self.text_widget.after(0, append)

# ==================================================
# Logging Setup Function
# ==================================================

def setup_logging(output_text_widget):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Capture all levels of logs

    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # File Handler
    file_handler = logging.FileHandler('app.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # GUI Handler
    gui_handler = TextHandler(output_text_widget)
    gui_handler.setLevel(logging.DEBUG)
    gui_handler.setFormatter(formatter)
    logger.addHandler(gui_handler)
    
    return logger

# ==================================================
# New Helper Function to Calculate Data Hours
# ==================================================

def calculate_data_hours(client, params, logger):
    """
    Calculate the total number of hours of data available for the given vehicle_id
    within the specified time range.

    Returns the total hours as a float. If no data is found, returns 0.
    """
    vehicle_id = params['vehicle_id']
    timezone = params['timezone']
    
    try:
        local_tz = pytz.timezone(timezone)
    except pytz.UnknownTimeZoneError:
        logger.error(f"Unknown timezone: {timezone}. Please provide a valid timezone.")
        return 0.0

    try:
        # Create timezone-aware timestamps in UTC
        start_dt_local = pd.Timestamp(f"{params['start_date']} {params['start_time']}").tz_localize(local_tz)
        end_dt_local = pd.Timestamp(f"{params['end_date']} {params['end_time']}").tz_localize(local_tz)
        start_dt_utc = start_dt_local.astimezone(pytz.UTC)
        end_dt_utc = end_dt_local.astimezone(pytz.UTC)
    except Exception as e:
        logger.error(f"Error creating timestamps: {e}")
        return 0.0

    # Initialize variables to store the earliest and latest timestamps
    earliest_time = None
    latest_time = None

    try:
        # Fetch all measurements (assuming multiple measurements may have data)
        measurements_query = 'SHOW MEASUREMENTS'
        logger.debug(f"Executing query: {measurements_query}")
        measurements_result = client.query(measurements_query)
        measurements = list(measurements_result.get_points())

        if not measurements:
            logger.info("No measurements found in the database.")
            return 0.0

        # Iterate through each measurement to find the earliest and latest timestamps
        for measurement in measurements:
            measurement_name = measurement['name']
            
            # Query for the earliest timestamp
            min_query = f'''
            SELECT MIN(time) as min_time 
            FROM "{measurement_name}"
            WHERE vehicle_id='{vehicle_id}' 
              AND time >= '{start_dt_utc.isoformat()}'
              AND time < '{end_dt_utc.isoformat()}'
            '''
            logger.debug(f"Executing query for earliest time in measurement '{measurement_name}':\n{min_query}")
            min_result = client.query(min_query)
            min_points = list(min_result.get_points())

            # Query for the latest timestamp
            max_query = f'''
            SELECT MAX(time) as max_time 
            FROM "{measurement_name}"
            WHERE vehicle_id='{vehicle_id}' 
              AND time >= '{start_dt_utc.isoformat()}'
              AND time < '{end_dt_utc.isoformat()}'
            '''
            logger.debug(f"Executing query for latest time in measurement '{measurement_name}':\n{max_query}")
            max_result = client.query(max_query)
            max_points = list(max_result.get_points())

            # Update earliest_time
            if min_points and min_points[0]['min_time']:
                # Parse the timestamp as UTC
                min_time_utc = pd.to_datetime(min_points[0]['min_time'], utc=True)
                # Convert to desired timezone
                min_time = min_time_utc.astimezone(local_tz)
                if earliest_time is None or min_time < earliest_time:
                    earliest_time = min_time
                    logger.debug(f"Updated earliest_time to {earliest_time}")

            # Update latest_time
            if max_points and max_points[0]['max_time']:
                # Parse the timestamp as UTC
                max_time_utc = pd.to_datetime(max_points[0]['max_time'], utc=True)
                # Convert to desired timezone
                max_time = max_time_utc.astimezone(local_tz)
                if latest_time is None or max_time > latest_time:
                    latest_time = max_time
                    logger.debug(f"Updated latest_time to {latest_time}")

        if earliest_time and latest_time:
            duration = latest_time - earliest_time
            total_hours = duration.total_seconds() / 3600  # Convert seconds to hours
            # Handle negative durations
            if total_hours < 0:
                logger.warning(f"Calculated duration is negative ({total_hours} hours). Check timezones and data consistency.")
                return 0.0
            logger.info(f"Total data duration: {total_hours:.2f} hours")
            return total_hours
        else:
            logger.info("No data found for the specified parameters.")
            return 0.0

    except Exception as e:
        logger.error(f"Error calculating data hours: {e}")
        return 0.0

# ==================================================
# Main Query Function to Use Either CSV or DB Data
# ==================================================

def run_queries(params, logger):
    """
    Connects to InfluxDB, calculates total data hours, loads measurement fields (from a CSV file or via DB query),
    and then for each measurement-field combination, counts the non-null entries
    based on vehicle and time filters. Finally, saves the results to a CSV file.

    Returns the total_hours, start_dt_utc, and end_dt_utc values.
    """
    # Connect to InfluxDB if needed (for running field count queries)
    client = None
    total_hours = 0.0
    start_dt_utc = None
    end_dt_utc = None
    try:
        client = InfluxDBClient(
            host=params['host'],
            port=int(params['port']),
            username=params['username'],
            password=params['password'],
            database=params['database']
        )
        logger.info("Connected to InfluxDB.")
    except Exception as e:
        logger.error(f"Failed to connect to InfluxDB: {e}")
        return total_hours, start_dt_utc, end_dt_utc

    # Create timezone-aware timestamps using the provided timezone.
    try:
        local_tz = pytz.timezone(params['timezone'])
        start_dt_local = pd.Timestamp(f"{params['start_date']} {params['start_time']}").tz_localize(local_tz)
        end_dt_local = pd.Timestamp(f"{params['end_date']} {params['end_time']}").tz_localize(local_tz)
        start_dt_utc = start_dt_local.astimezone(pytz.UTC)
        end_dt_utc = end_dt_local.astimezone(pytz.UTC)
    except Exception as e:
        logger.error(f"Error creating timestamps: {e}")
        if client:
            client.close()
            logger.info("Closed InfluxDB connection.")
        return total_hours, start_dt_utc, end_dt_utc

    # Calculate and display total data hours
    logger.info("\nCalculating total data hours available for the specified parameters...")
    total_hours = calculate_data_hours(client, params, logger)
    if total_hours > 0:
        logger.info(f"Total Hours of Data Available: {total_hours:.2f} hours")
    else:
        logger.info("No data available for the specified parameters.")
        # Depending on your preference, you might choose to exit here
        # return total_hours, start_dt_utc, end_dt_utc

    # Decide whether to load measurements from CSV or dynamically query the DB.
    if params['use_csv']:
        csv_filename = params['csv_filename']
        if not os.path.exists(csv_filename):
            logger.error(f"CSV file '{csv_filename}' not found.")
            if client:
                client.close()
                logger.info("Closed InfluxDB connection.")
            return total_hours, start_dt_utc, end_dt_utc
        try:
            measurements_fields = load_measurements_fields(csv_filename)
            logger.info("Loaded measurements from CSV file.")
        except Exception as e:
            logger.error(f"Error loading CSV: {e}")
            if client:
                client.close()
                logger.info("Closed InfluxDB connection.")
            return total_hours, start_dt_utc, end_dt_utc
    else:
        try:
            measurements_fields = fetch_measurements_and_fields(client)
            logger.info("Fetched measurements and fields from InfluxDB.")
        except Exception as e:
            logger.error(f"Error fetching measurements/fields: {e}")
            if client:
                client.close()
                logger.info("Closed InfluxDB connection.")
            return total_hours, start_dt_utc, end_dt_utc

    results_list = []

    # Loop through each measurement and its fields and count entries.
    for measurement, fields in measurements_fields.items():
        logger.info(f"\nProcessing Measurement: {measurement}")
        if not fields:
            logger.info("  No fields found for this measurement.")
            continue
        for field in fields:
            count_value = get_count_for_field(client, measurement, field, params['vehicle_id'], start_dt_utc, end_dt_utc, logger)
            combined_key = f"{measurement}.count_{field}"
            results_list.append({
                'Measurement_Field': combined_key,
                'Count': count_value
            })

    # Save results to CSV if any results exist.
    if results_list:
        df = pd.DataFrame(results_list)
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        csv_output = (f"measurements_field_counts_{params['vehicle_id']}_"
                      f"{params['start_date'].replace('-', '')}_"
                      f"{params['start_time'].replace(':', '')}_to_"
                      f"{params['end_time'].replace(':', '')}_{timestamp}.csv")
        try:
            df.to_csv(csv_output, index=False)
            logger.info(f"\nData saved to {csv_output}")
        except Exception as e:
            logger.error(f"Error saving CSV file: {e}")
    else:
        logger.info("No data to save.")

    # Close the InfluxDB connection
    if client:
        client.close()
        logger.info("Closed InfluxDB connection.")

    return total_hours, start_dt_utc, end_dt_utc

# ==================================================
# Tkinter GUI Application
# ==================================================

class InfluxDBGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("InfluxDB Query Tool with CSV Measurements")
        self.geometry("800x850")  # Increased height to accommodate the new label
        self.create_widgets()
        # Setup logging after widgets are created
        self.logger = setup_logging(self.output_text)

    def create_widgets(self):
        # ==================================================
        # InfluxDB Connection Parameters Frame
        # ==================================================
        frame_conn = ttk.LabelFrame(self, text="InfluxDB Connection")
        frame_conn.grid(column=0, row=0, padx=10, pady=10, sticky="W")

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

        # ==================================================
        # Filtering Parameters Frame
        # ==================================================
        frame_filter = ttk.LabelFrame(self, text="Filtering Parameters")
        frame_filter.grid(column=0, row=1, padx=10, pady=10, sticky="W")

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

        # ==================================================
        # CSV / Measurement Source Options Frame
        # ==================================================
        frame_csv = ttk.LabelFrame(self, text="Measurement Source Options")
        frame_csv.grid(column=0, row=2, padx=10, pady=10, sticky="W")

        self.use_csv_var = tk.BooleanVar(value=True)
        self.csv_filename_var = tk.StringVar(value="")  # Initially empty

        ttk.Checkbutton(frame_csv, text="Use CSV for Measurement Fields", variable=self.use_csv_var).grid(column=0, row=0, sticky="W", padx=5, pady=2)
        ttk.Label(frame_csv, text="CSV Filename:").grid(column=0, row=1, sticky="W", padx=5)
        csv_filename_entry = ttk.Entry(frame_csv, width=40, textvariable=self.csv_filename_var)
        csv_filename_entry.grid(column=1, row=1, padx=5, pady=2)

        # Button to select CSV file via dialog.
        self.select_csv_btn = ttk.Button(frame_csv, text="Select File", command=self.select_csv_file)
        self.select_csv_btn.grid(column=2, row=1, padx=5, pady=2)

        # ==================================================
        # Run Button and Output Display
        # ==================================================
        self.run_button = ttk.Button(self, text="Run Query", command=self.on_run_query)
        self.run_button.grid(column=0, row=3, padx=10, pady=10, sticky="W")

        # New Label to Display Total Data Hours
        self.data_hours_var = tk.StringVar(value="Total Data Hours: N/A")
        self.data_hours_label = ttk.Label(self, textvariable=self.data_hours_var, font=("Helvetica", 12, "bold"))
        self.data_hours_label.grid(column=0, row=4, padx=10, pady=5, sticky="W")

        # Adjust the row numbers for the output_text to accommodate the new label
        self.output_text = scrolledtext.ScrolledText(self, width=90, height=25, wrap=tk.WORD, state='disabled')
        self.output_text.grid(column=0, row=5, padx=10, pady=10)

    def select_csv_file(self):
        """ Opens a file dialog for selecting the CSV (or text) file and sets the filename variable. """
        filename = filedialog.askopenfilename(
            title="Select Measurement Fields File",
            filetypes=[("Text Files", "*.txt"), ("CSV Files", "*.csv"), ("All Files", "*.*")]
        )
        if filename:
            self.csv_filename_var.set(filename)

    def on_run_query(self):
        # Clear previous output and data hours
        self.output_text.configure(state='normal')
        self.output_text.delete(1.0, tk.END)
        self.output_text.configure(state='disabled')
        self.data_hours_var.set("Total Data Hours: Calculating...")
        self.run_button.config(state=tk.DISABLED)

        # Gather parameters from the GUI
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

        # Run the query in a separate thread to keep the GUI responsive.
        threading.Thread(target=self.run_query_thread, args=(params,)).start()

    def run_query_thread(self, params):
        try:
            total_hours, start_dt, end_dt = run_queries(params, self.logger)
            # Update the data_hours_var directly
            if total_hours > 0:
                self.data_hours_var.set(f"Total Data Hours: {total_hours:.2f} hours")
            else:
                self.data_hours_var.set("Total Data Hours: 0 hours")
        except Exception as e:
            self.logger.error(f"Error during query execution: {e}")
            self.data_hours_var.set("Total Data Hours: Error")
        finally:
            self.run_button.config(state=tk.NORMAL)

# ==================================================
# Main Execution
# ==================================================

if __name__ == "__main__":
    app = InfluxDBGUI()
    app.mainloop()
