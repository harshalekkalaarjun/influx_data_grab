from influxdb import InfluxDBClient

def fetch_measurements_and_fields(client):
    # Fetch measurements
    measurements_query = 'SHOW MEASUREMENTS'
    measurements_result = client.query(measurements_query)
    measurements = list(measurements_result.get_points())

    # Dictionary to hold measurement and their fields
    measurements_fields = {}

    for measurement in measurements:
        measurement_name = measurement['name']
        fields_query = f'SHOW FIELD KEYS FROM "{measurement_name}"'
        fields_result = client.query(fields_query)
        fields = list(fields_result.get_points())

        # Collect all fields for the measurement
        field_names = [field['fieldKey'] for field in fields]
        measurements_fields[measurement_name] = field_names

    return measurements_fields


def main():
    client = InfluxDBClient(host='104.154.190.81', port=15086, username='boson_hmi', password='hmi@boson76$', database='HMI_test')
    
    # Get measurements and fields
    measurements_fields = fetch_measurements_and_fields(client)
    for measurement, fields in measurements_fields.items():
        print(f"Measurement: {measurement}")
        print("Fields:", fields)
        print()

if __name__ == "__main__":
    main()
