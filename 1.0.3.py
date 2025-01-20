from influxdb import InfluxDBClient
import pandas as pd
import pytz

def get_measurement_data(client, vehicle_id, start_date, end_date, start_time, end_time, measurement,filed):
    local_tz = pytz.timezone('Asia/Kolkata')

    # Combine date and time, and then localize it
    start_dt = pd.Timestamp(f'{start_date} {start_time}').tz_localize(local_tz)
    end_dt = pd.Timestamp(f'{end_date} {end_time}').tz_localize(local_tz)

    # Create the query for retrieving records in the specified measurement
    query = f"""
    SELECT COUNT("{filed}") 
    FROM "{measurement}"
    WHERE vehicle_id='{vehicle_id}' AND time >= '{start_dt.isoformat()}' AND time < '{end_dt.isoformat()}'
    """
    
    results = client.query(query)
    points = list(results.get_points())
    
    if not points:
        print("No data found for the given parameters. Please check vehicle ID, date range, and measurement name.")
        return pd.DataFrame()  # Return empty DataFrame if no data is found
    
    # Create DataFrame from points
    df = pd.DataFrame(points)
    return df

def main():
    client = InfluxDBClient(host='104.154.190.81', port=15086, username='boson_hmi', password='hmi@boson76$', database='HMI_test')
    vehicle_id = 'VT-Box-T1'
    start_date = '2025-01-08'
    end_date = '2025-01-08'
    start_time = '11:11:59'
    end_time = '12:12:00'
    measurement = 'controller_motor_usage_1_REAR'
    field = 'motorcontroller_1_throttle_input'
    
    data = get_measurement_data(client, vehicle_id, start_date, end_date, start_time, end_time, measurement,field)
    
    if not data.empty:
        # Save the data to a CSV file
        csv_filename = f"{measurement}_{start_date.replace('-', '')}_{start_time.replace(':', '')}_to_{end_time.replace(':', '')}.csv"
        data.to_csv(csv_filename, index=False)
        print(f"Data saved to {csv_filename}")
    else:
        print("No data to save.")

if __name__ == "__main__":
    main()
