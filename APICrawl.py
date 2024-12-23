import requests
import json
import csv

# API endpoint for looking up hotels
url = 'https://engine.hotellook.com/api/v2/lookup.json'

# Your API token
token = 'b7f26609c05d7a284e7433702629d8c2'  # Use your actual token here

# Specify CSV file path
csv_file_path = 'hotels_data.csv'

# Define the request parameters
params = {
    'query': 'Tokyo',  # Example search query
    'lookFor': 'hotel',   # Looking for hotels
    'lang': 'en',         # Language for the results
    'limit': 10,           # Maximum number of results
    'token': token        # Your API token
}

# Make the GET request to the API
response = requests.get(url, params=params)

# Check if the request was successful
if response.status_code == 200:
    # Parse the response JSON data
    data = response.json()

    # Extract location and hotel data from the JSON response
    location_data = data.get('results', {}).get('location', {})
    hotels = data.get('results', {}).get('hotels', [])

    # Define the CSV file headers
    csv_headers = [
        'Hotel ID', 'Label', 'Location Name', 'Full Name', 'Location ID',
        'City Name', 'Country Name', 'Country Code', 'IATA', 'Latitude', 
        'Longitude', 'Hotels Count'
    ]

    # Write the hotel data to CSV
    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
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

    print(f"Hotel data successfully saved to {csv_file_path}")
else:
    # Print the error if the request fails
    print(f"Error: {response.status_code} - {response.text}")
