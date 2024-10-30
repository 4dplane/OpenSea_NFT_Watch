import os
from dotenv import load_dotenv
import requests
import psycopg2
from datetime import datetime
import schedule
import time

# Load the .env file
load_dotenv()

# Connect to the PostgreSQL database
def connect_db():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        print("Connection to PostgreSQL database successful.")
        cur = conn.cursor()
        cur.execute("SELECT NOW();")
        result = cur.fetchone()
        print(f"Current Timestamp from Database: {result[0]}")
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
                    num_bids INT,
                    last_updated TIMESTAMP,
                    CONSTRAINT unique_collection_time UNIQUE (collection_name, last_updated)
                );
            """)
            conn.commit()
            print("[INFO] Table created or already exists.")
    except Exception as e:
        print(f"[ERROR] Could not create table: {e}")

# Define a small tolerance for float comparison
EPSILON = 1e-6

# Save data to the database
def save_data(conn, collection, highest_bid, floor_price, num_bids):
    data_changed = False
    change_details = []
    try:
        with conn.cursor() as cur:
            # Fetch the most recent data for the collection from the database
            cur.execute("""
                SELECT highest_single_bid, floor_price, num_bids 
                FROM nft_data 
                WHERE collection_name = %s 
                ORDER BY last_updated DESC 
                LIMIT 1;
            """, (collection,))
            result = cur.fetchone()

            # Unpack the existing data and convert to float for comparison
            existing_bid, existing_floor, existing_num_bids = None, None, 0
            if result:
                existing_bid = float(result[0]) if result[0] is not None else None
                existing_floor = float(result[1]) if result[1] is not None else None
                existing_num_bids = int(result[2]) if result[2] is not None else 0
                print(f"[DEBUG] Existing data for {collection}: Highest Bid: {existing_bid}, Floor Price: {existing_floor}, Num Bids: {existing_num_bids}")
            else:
                print(f"[DEBUG] No existing data for {collection}, adding initial data.")

            # Set default for num_bids if it is None
            if num_bids is None:
                num_bids = 0

            # Determine if a change has occurred
            if existing_bid is None and existing_floor is None and existing_num_bids == 0:
                data_changed = True  # First snapshot, always insert
                change_details.append("Initial snapshot added.")
            else:
                # Use tolerance for floating-point comparisons
                bid_changed = (highest_bid is not None and abs(highest_bid - existing_bid) > EPSILON)
                floor_changed = (floor_price is not None and abs(floor_price - existing_floor) > EPSILON)
                num_bids_changed = (num_bids != existing_num_bids)

                if bid_changed:
                    bid_diff = highest_bid - existing_bid
                    change_details.append(f"Highest Bid changed by {bid_diff:.6f} WETH (New: {highest_bid:.6f} WETH)")
                    data_changed = True

                if floor_changed:
                    floor_diff = floor_price - existing_floor
                    change_details.append(f"Floor Price changed by {floor_diff:.6f} WETH (New: {floor_price:.6f} WETH)")
                    data_changed = True

                if num_bids_changed:
                    bids_diff = num_bids - existing_num_bids
                    change_details.append(f"Number of Bids changed by {bids_diff} (New: {num_bids})")
                    data_changed = True

            # Insert new data if there's a change
            if data_changed:
                cur.execute("""
                    INSERT INTO nft_data (collection_name, highest_single_bid, floor_price, num_bids, last_updated)
                    VALUES (%s, %s, %s, %s, %s);
                """, (collection, highest_bid, floor_price, num_bids, datetime.now()))
                conn.commit()
                print(f"[INFO] New data point added for {collection}.")
            else:
                print(f"[INFO] No change for {collection}, not adding new data.")

    except Exception as e:
        print(f"[ERROR] Could not save data for {collection}: {e}")

    return data_changed, ", ".join(change_details)

# Convert Wei to WETH
def wei_to_weth(wei_value, decimals=18):
    return int(wei_value) / (10 ** decimals)

# Truncate collection names to a maximum of 30 characters
def truncate_name(name, max_length=30):
    return name[:max_length] + "..." if len(name) > max_length else name

# Get the highest single bid and count
def get_highest_single_bid_and_count(collection_slug):
    url = f"https://api.opensea.io/api/v2/offers/collection/{collection_slug}"
    try:
        response = requests.get(url, headers={"accept": "application/json", "X-API-KEY": os.getenv("OPENSEA_API_KEY")})
        if response.status_code != 200:
            print(f"[ERROR] {collection_slug} - Failed to fetch offers: {response.status_code} - {response.text}")
            return None, 0

        offers_data = response.json().get("offers", [])
        if not offers_data:
            print(f"[INFO] No offers found for {collection_slug}")
            return None, 0

        highest_single_bid_value = 0
        num_bids = 0

        # Iterate through the offers to find the highest single bid and number of bids
        for offer in offers_data:
            start_amount_wei = int(offer["protocol_data"]["parameters"]["offer"][0]["startAmount"])
            bid_count = int(offer["protocol_data"]["parameters"]["consideration"][0]["startAmount"])

            total_weth_value = wei_to_weth(start_amount_wei)
            single_bid_value = total_weth_value / bid_count if bid_count > 0 else total_weth_value

            if single_bid_value > highest_single_bid_value:
                highest_single_bid_value = single_bid_value
                num_bids = bid_count

        return highest_single_bid_value, num_bids

    except Exception as e:
        print(f"[ERROR] An error occurred while fetching the highest single bid for {collection_slug}: {e}")
        return None, 0

# Get the floor price
def get_floor_price(collection_slug):
    url = f"https://api.opensea.io/api/v2/listings/collection/{collection_slug}/best"
    try:
        response = requests.get(url, headers={"accept": "application/json", "X-API-KEY": os.getenv("OPENSEA_API_KEY")})
        if response.status_code != 200:
            print(f"[ERROR] {collection_slug} - Failed to fetch floor price: {response.status_code} - {response.text}")
            return None

        listings_data = response.json().get("listings", [])
        if not listings_data:
            print(f"[INFO] No listings found for {collection_slug}")
            return None

        floor_price = None
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

# Global variables to track the number of updates and scheduler runs
update_counter = 0
scheduler_run_counter = 0

# Main function that contains the current script logic
def main_job():
    global update_counter, scheduler_run_counter

    # Increment the scheduler run counter
    scheduler_run_counter += 1
    print(f"\n[INFO] Running scheduled job... (Run #{scheduler_run_counter})")

    # Counter for updates in the current run
    current_run_updates = 0

    # Connect to the database
    conn = connect_db()
    create_table(conn)

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
        "unigrids-by-zeblocks"
    ]

    changed_collections = []
    unchanged_collections = []

    for collection in collections:
        highest_bid, num_bids = get_highest_single_bid_and_count(collection)
        floor_price = get_floor_price(collection)

        # Check if there's a change and save accordingly
        change_detected, change_details = save_data(conn, collection, highest_bid, floor_price, num_bids)

        if change_detected:
            changed_collections.append((collection, change_details))
            current_run_updates += 1
        else:
            unchanged_collections.append(collection)

    conn.close()

    # Update the global update counter
    update_counter += current_run_updates

    # Print grouped results with change details
    print("\nNFTs with Changes:")
    print("=" * 20)
    for collection, details in changed_collections:
        print(f"{collection}: {details}")

    print("\nNFTs without Changes:")
    print("=" * 23)
    for collection in unchanged_collections:
        print(f"{collection}")

    # Display the update counters
    print(f"\n[INFO] Updates in current run: {current_run_updates}")
    print(f"[INFO] Total updates since start: {update_counter}")
    print(f"[INFO] Total scheduler runs: {scheduler_run_counter}")

# Schedule the script to run every hour
schedule.every(1).hours.do(main_job)

# Keep the script running
print("[INFO] Scheduler started. Script will run every hour.")
while True:
    schedule.run_pending()
    time.sleep(1)
