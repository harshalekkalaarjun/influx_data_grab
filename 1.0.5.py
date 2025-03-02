import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from datetime import datetime
from influxdb import InfluxDBClient
import pandas as pd
import pytz
import threading

# ============================
# InfluxDB Query Functions
# ============================

def fetch_measurements_and_fields(client):
    """
    Retrieve all measurements in the database and, for each, retrieve its field keys.
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
    Count non-null entries for a specific field in a measurement given vehicle and time filters.
    Returns the count value (typically as an integer).
    """
    query = f'''
    SELECT COUNT("{field}") 
    FROM "{measurement}"
    WHERE vehicle_id='{vehicle_id}' 
      AND time >= '{start_dt.isoformat()}' 
      AND time < '{end_dt.isoformat()}'
    '''
    output_func(f"Executing query:\n{query}\n")
    result = client.query(query)
    points = list(result.get_points())
    count_column = 'count'
    if points and count_column in points[0]:
        count_val = points[0][count_column]
        output_func(f"Count for {measurement}.{field}: {count_val}\n")
        return count_val
    return 0

def run_queries(params, output_func):
    """
    Connects to InfluxDB, fetches the measurements and fields,
    and gets the count for each field under the provided filter criteria.
    """
    try:
        # Establish connection with InfluxDB
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

    # Create timezone-aware timestamps using the provided timezone
    try:
        local_tz = pytz.timezone(params['timezone'])
        start_dt = pd.Timestamp(f"{params['start_date']} {params['start_time']}").tz_localize(local_tz)
        end_dt = pd.Timestamp(f"{params['end_date']} {params['end_time']}").tz_localize(local_tz)
    except Exception as e:
        output_func(f"Error creating timestamps: {e}\n")
        return

    vehicle_id = params['vehicle_id']

    # Fetch all measurements and their fields.
    try:
        measurements_fields = fetch_measurements_and_fields(client)
        output_func("Fetched measurements and fields.\n")
    except Exception as e:
        output_func(f"Error fetching measurements/fields: {e}\n")
        return

    results_list = []

    # Loop through each measurement and each field and get the count.
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

    # Create a DataFrame with the complete results.
    if results_list:
        df = pd.DataFrame(results_list)
        csv_filename = (f"measurements_field_counts_{vehicle_id}_"
                        f"{params['start_date'].replace('-', '')}_"
                        f"{params['start_time'].replace(':', '')}_to_"
                        f"{params['end_time'].replace(':', '')}.csv")
        try:
            df.to_csv(csv_filename, index=False)
            output_func(f"\nData saved to {csv_filename}\n")
        except Exception as e:
            output_func(f"Error saving CSV file: {e}\n")
    else:
        output_func("No data to save.\n")

# ============================
# Tkinter GUI Application
# ============================

class InfluxDBGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("InfluxDB Query Tool")
        self.geometry("700x600")
        self.create_widgets()

    def create_widgets(self):
        # Create a frame for the connection parameters
        frame_conn = ttk.LabelFrame(self, text="InfluxDB Connection")
        frame_conn.grid(column=0, row=0, padx=10, pady=10, sticky="W")

        # Connection parameters with labels and default values.
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

        # Create a frame for the filtering parameters.
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

        # Button to run the query.
        self.run_button = ttk.Button(self, text="Run Query", command=self.on_run_query)
        self.run_button.grid(column=0, row=2, padx=10, pady=10, sticky="W")

        # Scrolled text widget to show output.
        self.output_text = scrolledtext.ScrolledText(self, width=80, height=20, wrap=tk.WORD)
        self.output_text.grid(column=0, row=3, padx=10, pady=10)

    def append_output(self, message):
        self.output_text.insert(tk.END, message)
        self.output_text.see(tk.END)

    def on_run_query(self):
        # Clear the output field.
        self.output_text.delete(1.0, tk.END)

        # Disable the Run button while processing.
        self.run_button.config(state=tk.DISABLED)

        # Gather all input parameters.
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
            'timezone': self.timezone_var.get()
        }

        # Run the query function in a separate thread to avoid freezing the GUI.
        threading.Thread(target=self.run_query_thread, args=(params,)).start()

    def run_query_thread(self, params):
        try:
            run_queries(params, self.append_output)
        except Exception as e:
            self.append_output(f"Error during query execution: {e}\n")
        finally:
            # Re-enable the Run button when done.
            self.run_button.config(state=tk.NORMAL)

# ============================
# Main execution
# ============================

if __name__ == "__main__":
    app = InfluxDBGUI()
    app.mainloop()
