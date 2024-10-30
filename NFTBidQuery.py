import os
from dotenv import load_dotenv
import requests
import psycopg2
from datetime import datetime

# Load the .env file
load_dotenv()

def connect_db():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        print("Connection to PostgreSQL database successful.")

        # Create a cursor and run a test query
        cur = conn.cursor()
        cur.execute("SELECT NOW();")
        result = cur.fetchone()
        print(f"Current Timestamp from Database: {result[0]}")

        # Close the cursor (keep connection open for use)
        cur.close()
        return conn

    except Exception as e:
        print(f"[ERROR] Could not connect to the database: {e}")
        exit(1)


# Create the table if it doesn't exist
def create_table(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS nft_data (
                    id SERIAL PRIMARY KEY,
                    collection_name VARCHAR(255),
                    highest_single_bid NUMERIC,
                    floor_price NUMERIC,
                    last_updated TIMESTAMP,
                    CONSTRAINT unique_collection_time UNIQUE (collection_name, last_updated)
                );
            """)
            conn.commit()
            print("[INFO] Table created or already exists.")
    except Exception as e:
        print(f"[ERROR] Could not create table: {e}")


from decimal import Decimal

# Define a small tolerance for float comparison
EPSILON = 1e-6

# Update save_data() to include change details and current price
def save_data(conn, collection, highest_bid, floor_price):
    data_changed = False
    change_details = []
    try:
        with conn.cursor() as cur:
            # Fetch the most recent data for the collection from the database
            cur.execute("""
                SELECT highest_single_bid, floor_price 
                FROM nft_data 
                WHERE collection_name = %s 
                ORDER BY last_updated DESC 
                LIMIT 1;
            """, (collection,))
            result = cur.fetchone()

            # Unpack the existing data and convert to float for comparison
            existing_bid, existing_floor = None, None
            if result:
                existing_bid = float(result[0]) if result[0] is not None else None
                existing_floor = float(result[1]) if result[1] is not None else None
                print(f"[DEBUG] Existing data for {collection}: Highest Bid: {existing_bid}, Floor Price: {existing_floor}")
            else:
                print(f"[DEBUG] No existing data for {collection}, adding initial data.")

            # Determine if a change has occurred and calculate the difference
            if existing_bid is None and existing_floor is None:
                data_changed = True  # First snapshot, always insert
                change_details.append("Initial snapshot added.")
                print(f"[DEBUG] No existing snapshot for {collection}, adding initial data.")
            else:
                # Use tolerance for floating-point comparisons
                bid_changed = (highest_bid is not None and abs(highest_bid - existing_bid) > EPSILON)
                floor_changed = (floor_price is not None and abs(floor_price - existing_floor) > EPSILON)

                if bid_changed:
                    bid_diff = highest_bid - existing_bid
                    change_details.append(f"Highest Bid changed by {bid_diff:.6f} WETH (New: {highest_bid:.6f} WETH)")
                    data_changed = True

                if floor_changed:
                    floor_diff = floor_price - existing_floor
                    change_details.append(f"Floor Price changed by {floor_diff:.6f} WETH (New: {floor_price:.6f} WETH)")
                    data_changed = True

            # Insert new data if there's a change
            if data_changed:
                cur.execute("""
                    INSERT INTO nft_data (collection_name, highest_single_bid, floor_price, last_updated)
                    VALUES (%s, %s, %s, %s);
                """, (collection, highest_bid, floor_price, datetime.now()))
                conn.commit()
                print(f"[INFO] New data point added for {collection}.")
            else:
                print(f"[INFO] No change for {collection}, not adding new data.")

    except Exception as e:
        print(f"[ERROR] Could not save data for {collection}: {e}")

    return data_changed, ", ".join(change_details)


# Get the API key from the environment variable
api_key = os.getenv("OPENSEA_API_KEY")

# Check if the API key is loaded correctly
if not api_key:
    print("Error: API key not found in environment variables.")
    exit(1)

# Define the headers with the API key
headers = {
    "accept": "application/json",
    "X-API-KEY": api_key
}

# Define the OpenSea API URL templates
offer_url_template = "https://api.opensea.io/api/v2/offers/collection/{}"
floor_url_template = "https://api.opensea.io/api/v2/listings/collection/{}/best"


# Function to convert Wei to WETH
def wei_to_weth(wei_value, decimals=18):
    return int(wei_value) / (10 ** decimals)


# Function to truncate collection names to a maximum of 30 characters
def truncate_name(name, max_length=30):
    return name[:max_length] + "..." if len(name) > max_length else name


# Function to get the highest single bid for a specific collection
def get_highest_single_bid(collection_slug):
    url = offer_url_template.format(collection_slug)
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"[ERROR] {collection_slug} - Failed to fetch offers: {response.status_code} - {response.text}")
            return None

        offers_data = response.json().get("offers", [])
        if not offers_data:
            print(f"[INFO] No offers found for {collection_slug}")
            return None

        highest_single_bid_value = 0
        # Iterate through the offers to find the highest single bid
        for offer in offers_data:
            start_amount_wei = int(offer["protocol_data"]["parameters"]["offer"][0]["startAmount"])
            num_bids = int(offer["protocol_data"]["parameters"]["consideration"][0]["startAmount"])

            total_weth_value = wei_to_weth(start_amount_wei)
            single_bid_value = total_weth_value / num_bids if num_bids > 0 else total_weth_value

            # Update the highest single bid if a higher value is found
            if single_bid_value > highest_single_bid_value:
                highest_single_bid_value = single_bid_value

        return highest_single_bid_value if highest_single_bid_value > 0 else None

    except Exception as e:
        print(f"[ERROR] An error occurred while fetching the highest single bid for {collection_slug}: {e}")
        return None


# Function to get the floor price for a specific collection
def get_floor_price(collection_slug):
    url = floor_url_template.format(collection_slug)
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"[ERROR] {collection_slug} - Failed to fetch floor price: {response.status_code} - {response.text}")
            return None

        listings_data = response.json().get("listings", [])
        if not listings_data:
            print(f"[INFO] No listings found for {collection_slug}")
            return None

        floor_price = None
        # Iterate through the listings to find the floor price
        for listing in listings_data:
            price_info = listing.get('price', {}).get('current', {})
            price_wei = price_info.get('value')
            if price_wei:
                price_weth = wei_to_weth(int(price_wei))
                if floor_price is None or price_weth < floor_price:
                    floor_price = price_weth

        return floor_price if floor_price is not None else None

    except Exception as e:
        print(f"[ERROR] An error occurred while fetching the floor price for {collection_slug}: {e}")
        return None


# Modified main function to group output
def main():
    conn = connect_db()
    create_table(conn)

    # Lists to hold collections with changes and no changes
    changed_collections = []
    unchanged_collections = []

    collections = [
        "pridepunks2018", "meebits", "official-v1-punks", "the-blocks-of-art-by-shvembldr",
        "watercolor-dreams-by-numbersinmotion", "rituals-venice-by-aaron-penne-x-boreta",
        "aerial-view-by-dalenz", "blockbob-rorschach-by-eboy", "timepiece-by-wawaa",
        "mirage-gallery-dreamers", "friendship-bracelets-by-alexis-andre", "meekicks",
        "easy-peasy", "spiromorphs-by-sab", "dot-matrix-gradient-study-by-jake-rockland",
        "patterns-of-life-by-vamoss", "time-squared-by-steen-x-n-e-o",
        "spiroflakes-by-alexander-reben", "flowers-by-rvig",
        "transitions-by-jason-ting-x-matt-bilfield", "talking-blocks-by-remo-x-dcsan",
        "gizmobotz-by-mark-cotton", "octo-garden-by-rich-lord",
        "spaghettification-by-owen-moore", "panelscape-a-b-by-paolo-tonon",
        "inspirals-by-radix", "chromie-squiggle-by-snowfro", "color-study-by-jeff-davis",
        "bitgans", "cryptoblots-by-daim-aggott-honsch", "apparitions-by-aaron-penne",
        "unigrids-by-zeblocks", "algobots-by-stina-jones"
    ]

    for collection in collections:
        highest_bid = get_highest_single_bid(collection)
        floor_price = get_floor_price(collection)

        # Check if there's a change and save accordingly
        change_detected, change_details = save_data(conn, collection, highest_bid, floor_price)

        # Sort collections based on whether there was a change or not
        if change_detected:
            changed_collections.append((collection, change_details))
        else:
            unchanged_collections.append(collection)

    conn.close()

    # Print grouped results with change details
    print("\nNFTs with Changes:")
    print("=" * 20)
    for collection, details in changed_collections:
        print(f"{collection}: {details}")

    print("\nNFTs without Changes:")
    print("=" * 23)
    for collection in unchanged_collections:
        print(f"{collection}")

if __name__ == "__main__":
    main()
