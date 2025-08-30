import pandas as pd
import gzip
import io
import requests
import os

# Create a folder to store downloaded files
os.makedirs("airbnb_data", exist_ok=True)

# List of calendar URLs (you can add more here)
calendar_urls = [
    "https://data.insideairbnb.com/south-africa/wc/cape-town/2024-09-25/data/calendar.csv.gz",
    "https://data.insideairbnb.com/south-africa/wc/cape-town/2024-12-28/data/calendar.csv.gz"
]

# List to store DataFrames
calendar_dfs = []

# Loop through each calendar URL
for url in calendar_urls:
    print(f"\n📥 Processing: {url}")
    response = requests.get(url)
    
    if response.status_code == 200:
        compressed_file = io.BytesIO(response.content)
        with gzip.open(compressed_file, mode='rt', encoding='utf-8') as f:
            df = pd.read_csv(f)
            calendar_dfs.append(df)
            print(f"✅ Loaded {len(df)} rows.")
    else:
        print(f"❌ Failed to load calendar. Status code: {response.status_code}")

# Combine all calendar DataFrames
if calendar_dfs:
    combined_calendar = pd.concat(calendar_dfs, ignore_index=True)
    output_path = os.path.join("airbnb_data", "calendar_combined.csv")
    combined_calendar.to_csv(output_path, index=False)
    print(f"\n🧾 Combined calendar saved to: {output_path}")
else:
    print("⚠️ No calendar data loaded.")
