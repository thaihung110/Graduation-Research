from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import csv

# Define API URL and Token
API_URL = 'https://engine.hotellook.com/api/v2/lookup.json'
API_TOKEN = 'b7f26609c05d7a284e7433702629d8c2'  # Replace with your actual token

# Define CSV file path
CSV_FILE_PATH = '/data/hotels_data.csv'

# Function to make the API request
def fetch_hotel_data(**kwargs):
    # Define request parameters
    params = {
        'query': 'Tokyo',    # Example search query
        'lookFor': 'hotel',  # Looking for hotels
        'lang': 'en',        # Language for the results
        'limit': 10,         # Maximum number of results
        'token': API_TOKEN   # Your API token
    }

    # Make the GET request to the API
    response = requests.get(API_URL, params=params)
    
    # If successful, return the JSON response
    if response.status_code == 200:
        return response.json()
    else:
        raise ValueError(f"Error: {response.status_code} - {response.text}")

# Function to save hotel data to a CSV file
def save_to_csv(**kwargs):
    # Pull the data fetched from the previous task
    data = kwargs['ti'].xcom_pull(task_ids='fetch_hotel_data')
    
    # Extract location and hotel data from the JSON response
    location_data = data.get('results', {}).get('location', {})
    hotels = data.get('results', {}).get('hotels', [])

    # Define the CSV headers
    csv_headers = [
        'Hotel ID', 'Label', 'Location Name', 'Full Name', 'Location ID',
        'City Name', 'Country Name', 'Country Code', 'IATA', 'Latitude', 
        'Longitude', 'Hotels Count'
    ]

    # Write the hotel data to CSV
    with open(CSV_FILE_PATH, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(csv_headers)  # Write the headers

        # Write location data (assuming one location is returned)
        city_name = location_data.get('cityName', '')
        country_name = location_data.get('countryName', '')
        country_code = location_data.get('countryCode', '')
        iata = location_data.get('iata', '')
        hotels_count = location_data.get('hotelsCount', 0)
        location_lat = location_data.get('lat', '')
        location_lon = location_data.get('lon', '')

        # Write each hotel data
        for hotel in hotels:
            writer.writerow([
                hotel.get('id'),
                hotel.get('label'),
                hotel.get('locationName'),
                hotel.get('fullName'),
                hotel.get('locationId'),
                city_name,
                country_name,
                country_code,
                iata,
                hotel.get('location', {}).get('lat', ''),
                hotel.get('location', {}).get('lon', ''),
                hotels_count
            ])

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1
}

# Define the DAG
with DAG('hotel_data_dag',
         default_args=default_args,
         schedule_interval='@daily',  # Adjust the schedule as needed
         catchup=False) as dag:

    # Task 1: Fetch hotel data
    fetch_hotel_data = PythonOperator(
        task_id='fetch_hotel_data',
        python_callable=fetch_hotel_data,
        provide_context=True
    )

    # Task 2: Save the data to a CSV file
    save_to_csv = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_to_csv,
        provide_context=True
    )

    # Define the task dependencies
    fetch_hotel_data >> save_to_csv
