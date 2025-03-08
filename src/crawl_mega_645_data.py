import os
import re
import yaml
import requests
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup

# Constants
data_dir = "data"
csv_path = os.path.join(data_dir, "mega_645_history.csv")
yaml_path = os.path.join(data_dir, "missing_prize.yaml")
start_date = '01-01-2016'
end_date = '09-03-2025'
url = f'https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html?datef={start_date}&datet={end_date}'

# Ensure data directory exists
os.makedirs(data_dir, exist_ok=True)

def fetch_lottery_data(url):
    """Fetches and parses lottery data from the given URL."""
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch data. HTTP Status: {response.status_code}")
        return []
    
    soup = BeautifulSoup(response.text, 'html.parser')
    data = []
    rows = soup.find_all('tr')
    
    for row in tqdm(rows, desc="Processing rows"):
        date_td = row.find('td')
        if not date_td:
            continue
        
        raw_date = date_td.text.strip()
        date_match = re.search(r'(\d{2}/\d{2}/\d{4})', raw_date)
        date = date_match.group(1) if date_match else 'N/A'

        numbers = [span.text for span in row.find_all('span', class_='home-mini-whiteball')]
        if len(numbers) != 6:
            continue
        
        prize_td = row.find_all('td')[-1]
        prize_span = prize_td.find('span', class_='hidden-xs')
        prize = prize_span.text.strip() if prize_span else 'N/A'
        
        data.append([date] + numbers + [prize])
    
    return data

def save_to_csv(data, path):
    """Saves lottery data to a CSV file."""
    df = pd.DataFrame(data, columns=['date', 'number-1', 'number-2', 'number-3', 'number-4', 'number-5', 'number-6', 'prize'])
    df.to_csv(path, index=False, encoding='utf-8-sig')
    print(f"Data has been saved to {path}")

def load_yaml(path):
    """Loads YAML data from a file."""
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)

def update_missing_prizes(csv_path, yaml_path):
    """Updates missing prize values in the dataset using YAML file."""
    if not os.path.exists(csv_path):
        print("CSV file not found. Skipping update.")
        return
    
    df = pd.read_csv(csv_path, dtype=str)
    missing_prizes = load_yaml(yaml_path)
    df["prize"] = df.apply(lambda row: missing_prizes.get(row["date"], row["prize"]), axis=1)
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print("Missing prize values have been updated and saved successfully!")

# Execute data fetching and saving
data = fetch_lottery_data(url)
if data:
    save_to_csv(data, csv_path)
    update_missing_prizes(csv_path, yaml_path)
