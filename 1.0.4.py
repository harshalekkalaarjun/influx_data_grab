
from influxdb import InfluxDBClient
import pandas as pd
import pytz

def fetch_measurements_and_fields(client):
    """
    Retrieve all measurements in the database and, for each, retrieve its field keys.
    Returns a dictionary where keys are measurement names and values are lists of field keys.
    """
    # Retrieve all measurements in the database.
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

def get_count_for_field(client, measurement, field, vehicle_id, start_dt, end_dt):
    """
    Count non-null entries for a specific field in a measurement given vehicle and time filters.
    Returns the count value (typically as an integer).
    """
    # When you use SELECT COUNT("<field>"), InfluxDB returns a column named "count_<field>"
    query = f'''
    SELECT COUNT("{field}") 
    FROM "{measurement}"
    WHERE vehicle_id='{vehicle_id}' 
      AND time >= '{start_dt.isoformat()}' 
      AND time < '{end_dt.isoformat()}'
    '''
    result = client.query(query)
    # print(result)
    points = list(result.get_points())
    print(points)
    count_column = f'count'
    if points and count_column in points[0]:
        # The count value is stored in the 'count_<field>' column.
        print(points[0][count_column])
        return points[0][count_column]
    return 0

def main():
    # Establish connection with InfluxDB.
    client = InfluxDBClient(
        host='104.154.190.81',
        port=15086,
        username='boson_hmi',
        password='hmi@boson76$',
        database='HMI_test'
    )
    
    # Filtering parameters
    vehicle_id = 'VT-Box-T1'
    start_date = '2025-01-08'
    end_date = '2025-01-08'
    start_time = '11:11:59'
    end_time = '12:12:00'
    
    # Create timezone-aware timestamps using the Asia/Kolkata timezone.
    local_tz = pytz.timezone('Asia/Kolkata')
    start_dt = pd.Timestamp(f'{start_date} {start_time}').tz_localize(local_tz)
    end_dt   = pd.Timestamp(f'{end_date} {end_time}').tz_localize(local_tz)
    
    # Fetch all measurements and their fields.
    measurements_fields = fetch_measurements_and_fields(client)
    
    # List to collect all results.
    results_list = []
    
    # Loop through each measurement and each field and get the count.
    for measurement, fields in measurements_fields.items():
        print(f"Processing Measurement: {measurement}")
        
        # Skip if no fields are found for this measurement.

        if not fields:
            print("  No fields found for this measurement.")
            continue
        
        for field in fields:
            count_value = get_count_for_field(client, measurement, field, vehicle_id, start_dt, end_dt)
            # Create a combined key such as "Measurement.count_field"
            combined_key = f"{measurement}.count_{field}"
            # print(f"  {combined_key} -> {count_value}")
            results_list.append({
                'Measurement_Field': combined_key,
                'Count': count_value
            })
    
    # Create one DataFrame with the complete results.
    df = pd.DataFrame(results_list)
    
    if not df.empty:
        csv_filename = f"measurements_field_counts_{vehicle_id}_{start_date.replace('-', '')}_{start_time.replace(':', '')}_to_{end_time.replace(':', '')}.csv"
        df.to_csv(csv_filename, index=False)
        print(f"\nData saved to {csv_filename}")
    else:
        print("No data to save.")

if __name__ == "__main__":
    main()
