import os
from dotenv import load_dotenv
load_dotenv()  # Load env vars from .env file
import psycopg2
import logging
import requests

# Configure logging
logging.basicConfig(filename='scraper_errors.log', level=logging.ERROR)

# Database connection
def get_db_connection():
    return psycopg2.connect(
        dbname="your_database",
        user="your_username",
        password="your_password",
        host="localhost",
        port="5432"
    )

# Fetch gym data from Google Maps Places API
def search_gyms(location, radius=5000):
    base_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {
        "query": "martial arts gym",
        "location": location,
        "radius": radius,
        "key": "YOUR_API_KEY"  # Replace with your Google Maps API key
    }
    response = requests.get(base_url, params=params)
    return response.json().get("results", [])

# Insert gym data into the database
def insert_gym_data(gym_data):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Insert into MartialArtsGyms
        cursor.execute("""
            INSERT INTO bushido_life.MartialArtsGyms (PlaceID, BusinessName, Phone, StreetAddress, City, USState, ZipCode, Website, BusinessCategory, Latitude, Longitude, SourceWebsite)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            gym_data['place_id'], gym_data['name'], gym_data.get('formatted_phone_number'),
            gym_data.get('formatted_address'), gym_data.get('city'), gym_data.get('state'),
            gym_data.get('zip_code'), gym_data.get('website'), "Martial Arts School",
            gym_data['geometry']['location']['lat'], gym_data['geometry']['location']['lng'],
            "Google Maps"
        ))

        conn.commit()
    except Exception as e:
        logging.error(f"Error inserting gym data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Main function
def main():
    location = "40.7128,-74.0060"  # Example: New York City coordinates
    gyms = search_gyms(location)

    for gym in gyms:
        insert_gym_data(gym)

if __name__ == "__main__":
    main()
