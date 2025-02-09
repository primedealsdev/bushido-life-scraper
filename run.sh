#!/bin/bash

# Activate the virtual environment
source venv/bin/activate

# Run the connection test script
python test_connection.py

# Run the main scraper script
python scraper.py
