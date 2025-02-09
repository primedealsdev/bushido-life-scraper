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
    try:
        dbname = os.environ.get("DB_NAME")
        user = os.environ.get("DB_USER")
        password = os.environ.get("DB_PASSWORD")
        host = os.environ.get("DB_HOST")
        port = os.environ.get("DB_PORT")

        if not all([dbname, user, password, host, port]):
            raise ValueError("Missing database credentials in environment variables.")

        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        return conn
    except ValueError as e:
        logging.error(f"Database connection error: {e}")
        return None
    except psycopg2.Error as e:
        logging.error(f"PostgreSQL connection error: {e}")
        return None

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
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            data = response.json()

            status = data.get("status")
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
            logging.error(f"Error fetching gym data for '{term}': {e}")
        except (KeyError, TypeError) as e:
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
    conn = get_db_connection()
    if conn is None:
        return

    cursor = conn.cursor()

    try:
        # Robust Data validation
        required_fields = ['place_id', 'name', 'geometry', 'formatted_address']
        if not all(key in gym_data and gym_data[key] for key in required_fields) or not all(key in gym_data['geometry']['location'] and gym_data['geometry']['location'][key] for key in ['lat', 'lng']):
            logging.error(f"Invalid gym data received: {gym_data}")
            conn.rollback()
            return

        address_parts = gym_data.get('formatted_address', '').split(',')
        street_address = address_parts[0].strip() if address_parts else None
        city = address_parts[1].strip() if len(address_parts) > 1 else None
        state = address_parts[2].strip().split()[-2] if len(address_parts) > 2 else None
        zip_code = address_parts[2].strip().split()[-1] if len(address_parts) > 2 else None

        cursor.execute("""
            INSERT INTO bushido_life.MartialArtsGyms (PlaceID, BusinessName, Phone, StreetAddress, City, USState, ZipCode, Website, BusinessCategory, Latitude, Longitude, SourceWebsite)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            gym_data['place_id'], gym_data['name'], gym_data.get('formatted_phone_number'),
            street_address, city, state, zip_code, gym_data.get('website'),
            "Martial Arts School", gym_data['geometry']['location']['lat'],
            gym_data['geometry']['location']['lng'], "Google Maps"
        ))

        conn.commit()
    except psycopg2.Error as e:
        logging.error(f"Error inserting gym data: {e}")
        conn.rollback()
    except Exception as e:
        logging.error(f"An unexpected error occurred during insertion: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def main():
    location = "25.7743,-80.1937"  # Miami coordinates
    search_terms = ["Brazilian jiu jitsu", "grappling", "jiu jitsu", "martial arts gym", "MMA", "no-gi jiu jitsu", "wrestling"]
    gyms = search_gyms(location, search_terms)

    if gyms:
        for gym in gyms:
            insert_gym_data(gym)
        print(f"Successfully inserted {len(gyms)} gyms.")
    else:
        print("No gyms found or inserted.")

if __name__ == "__main__":
    main()