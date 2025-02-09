import os
from dotenv import load_dotenv
import psycopg2
import logging
import requests

# Load environment variables (database credentials from .env)
load_dotenv()

# Configure logging
logging.basicConfig(filename='scraper_errors.log', level=logging.ERROR)

# Database connection
def get_db_connection():
    # ... (no changes to this function)

# Fetch gym data from Google Maps Places API
def search_gyms(location, search_terms, radius=15000):  # 15km radius
    all_results = []

    for term in search_terms:
        base_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        params = {
            "query": term,
            "location": location,
            "radius": radius,
            "key": os.environ.get("GOOGLE_MAPS_API_KEY")  # API key from system env
        }
        url = f"{base_url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"  # Construct the URL
        print(f"API Request URL: {url}")  # Print the URL for debugging

        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            data = response.json()

            status = data.get("status")
            print(f"API Response Status: {status}") # Print status for debugging

            if status == "ZERO_RESULTS":
                logging.warning(f"No results found for '{term}' in the specified area.")
            elif status == "OVER_QUERY_LIMIT":
                logging.error("Google Maps API query limit exceeded.")
                return []  # Or implement a retry mechanism
            elif status == "REQUEST_DENIED":
                logging.error("Google Maps API request denied. Check your API key and billing.")
                return []
            elif status != "OK":
                logging.error(f"Google Maps API returned an unexpected status: {status}")
                return []

            results = data.get("results", [])
            all_results.extend(results)

        except requests.exceptions.RequestException as e:
            print(f"Request Exception: {e}")  # Print exception for debugging
            logging.error(f"Error fetching gym data for '{term}': {e}")
        except (KeyError, TypeError) as e:
            print(f"JSON parsing error: {e}. Data: {data if 'data' in locals() else 'N/A'}") # Print exception and data
            logging.error(f"Error parsing Google Maps API response for '{term}': {e}. Response data: {data if 'data' in locals() else 'N/A'}")

    # Remove duplicate gyms based on place_id
    unique_results = []
    seen_place_ids = set()
    for gym in all_results:
        place_id = gym.get('place_id')
        if place_id and place_id not in seen_place_ids:
            unique_results.append(gym)
            seen_place_ids.add(place_id)

    return unique_results

# Insert gym data into the database
def insert_gym_data(gym_data):
    # ... (no changes to this function)

def main():
    location = "25.7743,-80.1937"  # Miami coordinates
    search_terms = ["Brazilian jiu jitsu", "grappling", "jiu jitsu", "martial arts", "MMA", "judo", "no-gi jiu jitsu"]
    print(f"Location: {location}")  # Print location for debugging
    print(f"Search Terms: {search_terms}")  # Print search terms for debugging
    gyms = search_gyms(location, search_terms)

    if gyms:
        print(f"Found {len(gyms)} gyms.")  # Print number of gyms found
        for gym in gyms:
            insert_gym_data(gym)
        print(f"Successfully inserted {len(gyms)} gyms.")
    else:
        print("No gyms found or inserted.")

if __name__ == "__main__":
    main()