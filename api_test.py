import os
from dotenv import load_dotenv
import requests

load_dotenv()

api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
base_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"  # Or any other API endpoint
params = {
    "query": "test",  # A simple test query
    "location": "25.7743,-80.1937", # Miami
    "radius": 5000,
    "key": api_key
}

try:
    response = requests.get(base_url, params=params)
    response.raise_for_status()  # Check for HTTP errors
    data = response.json()
    print(data)  # Print the API response
except requests.exceptions.RequestException as e:
    print(f"API request error: {e}")
except Exception as e:
    print(f"An error occurred: {e}")