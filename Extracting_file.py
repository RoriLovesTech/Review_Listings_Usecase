import pandas as pd
import gzip
import io
import requests
import json
import os

# Create a folder to store downloaded files
os.makedirs("airbnb_data", exist_ok=True)

# Define the URLs and their types
datasets = {
    "calendar": {
        "url": "https://data.insideairbnb.com/south-africa/wc/cape-town/2024-09-25/data/calendar.csv.gz",
        "compressed": True,
        "filename": "calendar.csv.gz"
    },
     "listing": {
        "url": "https://data.insideairbnb.com/south-africa/wc/cape-town/2024-12-28/data/listings.csv.gz",
        "compressed": True,
        "filename": "listing.csv.gz"
    },
    "reviews": {
        "url": "https://data.insideairbnb.com/south-africa/wc/cape-town/2024-09-25/data/reviews.csv.gz",
        "compressed": True,
        "filename": "reviews.csv.gz"
    },
    "listings_visual": {
        "url": "https://data.insideairbnb.com/south-africa/wc/cape-town/2024-09-25/visualisations/listings.csv",
        "compressed": False,
        "filename": "listings_visual.csv"
    },
    "reviews_visual": {
        "url": "https://data.insideairbnb.com/south-africa/wc/cape-town/2024-09-25/visualisations/reviews.csv",
        "compressed": False,
        "filename": "reviews_visual.csv"
    },
    "neighbourhoods_csv": {
        "url": "https://data.insideairbnb.com/south-africa/wc/cape-town/2024-12-28/visualisations/neighbourhoods.csv",
        "compressed": False,
        "filename": "neighbourhoods.csv"
    },
    "neighbourhoods": {
        "url": "https://data.insideairbnb.com/south-africa/wc/cape-town/2024-09-25/visualisations/neighbourhoods.geojson",
        "geojson": True,
        "filename": "neighbourhoods.geojson"
    }
}

# Dictionary to store loaded data
loaded_data = {}

# Loop through each dataset
for name, info in datasets.items():
    print(f"\nLoading: {name}")
    response = requests.get(info["url"])
    
    if response.status_code == 200:
        file_path = os.path.join("airbnb_data", info["filename"])
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded and saved: {file_path}")
        
        if info.get("geojson"):
            geojson_data = json.loads(response.text)
            loaded_data[name] = geojson_data
            print(f"Loaded GeoJSON with {len(geojson_data['features'])} features.")
        
        elif info.get("compressed"):
            compressed_file = io.BytesIO(response.content)
            with gzip.open(compressed_file, mode='rt', encoding='utf-8') as f:
                df = pd.read_csv(f)
            loaded_data[name] = df
            print(df.info())
        
        else:
            df = pd.read_csv(io.StringIO(response.text))
            loaded_data[name] = df
            print(df.info())
    else:
        print(f"Failed to load {name}. Status code: {response.status_code}")

# Example: Access calendar DataFrame
# calendar_df = loaded_data["calendar"]
