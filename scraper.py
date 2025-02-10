import os
from dotenv import load_dotenv
import psycopg2
import logging
import requests
import json

# Load environment variables
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

# Fetch gym data from Google Maps Places API (Text Search)
def search_gyms(location, search_terms, radius=15000):
    all_results = []

    for term in search_terms:
        base_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        params = {
            "query": term,
            "location": location,
            "radius": radius,
            "key": os.environ.get("GOOGLE_MAPS_API_KEY")
        }
        url = f"{base_url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"
        print(f"API Request URL: {url}")

        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()

            status = data.get("status")
            print(f"API Response Status: {status}")

            if status == "ZERO_RESULTS":
                logging.warning(f"No results found for '{term}' in the specified area.")
            elif status == "OVER_QUERY_LIMIT":
                logging.error("Google Maps API query limit exceeded.")
                return []
            elif status == "REQUEST_DENIED":
                logging.error("Google Maps API request denied. Check your API key and billing.")
                return []
            elif status != "OK":
                logging.error(f"Google Maps API returned an unexpected status: {status}")
                return []

            results = data.get("results", [])
            all_results.extend(results)

        except requests.exceptions.RequestException as e:
            print(f"Request Exception: {e}")
            logging.error(f"Error fetching gym data for '{term}': {e}")
        except (KeyError, TypeError) as e:
            print(f"JSON parsing error: {e}. Data: {data if 'data' in locals() else 'N/A'}")
            logging.error(f"Error parsing Google Maps API response for '{term}': {e}. Response data: {data if 'data' in locals() else 'N/A'}")

    unique_results = []
    seen_place_ids = set()
    for gym in all_results:
        place_id = gym.get('place_id')
        if place_id and place_id not in seen_place_ids:
            unique_results.append(gym)
            seen_place_ids.add(place_id)

    return unique_results

# Fetch gym details from Google Maps Places API (Details API)
def get_gym_details(place_id):
    base_url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "formatted_phone_number,website,formatted_address,geometry,name",
        "key": os.environ.get("GOOGLE_MAPS_API_KEY")
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        details_data = response.json()
        print(json.dumps(details_data, indent=4))
        status = details_data.get("status")

        if status == "OK":
            result = details_data.get("result", {})
            phone = result.get('formatted_phone_number')
            website = result.get('website')
            address = result.get('formatted_address')
            geometry = result.get('geometry')
            name = result.get('name')

            return {
                "phone": phone,
                "website": website,
                "address": address,
                "geometry": geometry,
                "name": name
            }
        else:
            logging.error(f"Places Details API error: {status}")
            return {}
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching gym details: {e}")
        return {}
    except (KeyError, TypeError) as e:
        logging.error(f"Error parsing Places Details API response: {e}. Data: {details_data if 'details_data' in locals() else 'N/A'}")
        return {}


# Insert gym data into the database
def insert_gym_data(gym_data):
    conn = get_db_connection()
    if conn is None:
        return

    cursor = conn.cursor()

    try:
        required_fields = ['place_id', 'name', 'geometry', 'address']
        if not all(key in gym_data and gym_data[key] for key in required_fields) or not all(key in gym_data['geometry']['location'] and gym_data['geometry']['location'][key] for key in ['lat', 'lng']):
            logging.error(f"Invalid gym data received: {gym_data}")
            conn.rollback()
            return

        address_parts = gym_data.get('address', '').split(',')
        street_address = address_parts[0].strip() if address_parts else None
        city = address_parts[1].strip() if len(address_parts) > 1 else None
        state = address_parts[2].strip().split()[-2] if len(address_parts) > 2 else None
        zip_code = address_parts[2].strip().split()[-1] if len(address_parts) > 2 else None

        cursor.execute("""
            INSERT INTO bushido_life.MartialArtsGyms (PlaceID, BusinessName, Phone, StreetAddress, City, USState, ZipCode, Website, BusinessCategory, Latitude, Longitude, SourceWebsite)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            gym_data['place_id'], gym_data['name'], gym_data.get('phone'),
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
    search_terms = ["Brazilian jiu jitsu", "grappling", "jiu jitsu", "martial arts", "MMA", "judo", "no-gi jiu jitsu"]
    print(f"Location: {location}")
    print(f"Search Terms: {search_terms}")
    gyms = search_gyms(location, search_terms)

    if gyms:
        print(f"Found {len(gyms)} gyms.")
        for gym in gyms:
            place_id = gym.get('place_id')
            if place_id:
                gym_details = get_gym_details(place_id)
                if gym_details:
                    gym.update(gym_details)
                    insert_gym_data(gym)
                else:
                    logging.error(f"Failed to retrieve details for place_id: {place_id