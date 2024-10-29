import os
from dotenv import load_dotenv
import requests

# Load the .env file
load_dotenv()

# Get the API key from the environment variable
api_key = os.getenv("OPENSEA_API_KEY")

# Define the headers with the API key
headers = {
    "accept": "application/json",
    "X-API-KEY": api_key
}

# Define the OpenSea API URL for the specific collection
url = "https://api.opensea.io/api/v2/offers/collection/pridepunks2018"

# Function to convert Wei to WETH
def wei_to_weth(wei_value, decimals=18):
    return int(wei_value) / (10 ** decimals)


# Function to get the highest bid
def get_highest_bid():
    try:
        # Make the API request
        response = requests.get(url, headers=headers)

        # Check if the request was successful
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            return

        # Parse the JSON response
        api_response = response.json()

        # Initialize variables for the highest bid
        highest_bid_value = 0
        highest_bid_count = 0
        single_bid_value = 0

        # Iterate through the offers to find the highest bid
        for offer in api_response.get("offers", []):
            start_amount = int(offer["protocol_data"]["parameters"]["offer"][0]["startAmount"])
            num_bids = int(offer["protocol_data"]["parameters"]["consideration"][0]["startAmount"])

            weth_value = wei_to_weth(start_amount)
            single_bid_value = weth_value / num_bids if num_bids > 0 else weth_value

            # Check if this is the highest bid
            if weth_value > highest_bid_value or (weth_value == highest_bid_value and num_bids > highest_bid_count):
                highest_bid_value = weth_value
                highest_bid_count = num_bids

            # Print each bid detail, including total bids and the value of a single bid
            print(
                f"Total Bids: {num_bids}, Total WETH Value: {weth_value:.6f} WETH, Single Bid Value: {single_bid_value:.6f} WETH")

    except Exception as e:
        print(f"An error occurred: {e}")


# Call the function to get the highest bid
get_highest_bid()
