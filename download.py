import os
import requests

URLS = [
    "https://data.insideairbnb.com/united-states/ny/new-york-city/2025-03-01/data/listings.csv.gz",
    "https://data.insideairbnb.com/united-states/ca/los-angeles/2025-03-01/data/listings.csv.gz",
    "https://data.insideairbnb.com/united-states/ca/san-francisco/2025-03-01/data/listings.csv.gz",
    "https://data.insideairbnb.com/united-states/il/chicago/2025-09-22/data/listings.csv.gz",
    "https://data.insideairbnb.com/united-states/tx/austin/2025-09-16/data/listings.csv.gz",

    "https://data.insideairbnb.com/united-kingdom/england/london/2025-09-14/data/listings.csv.gz",
    "https://data.insideairbnb.com/france/ile-de-france/paris/2025-03-03/data/listings.csv.gz",
    "https://data.insideairbnb.com/spain/catalonia/barcelona/2025-09-14/data/listings.csv.gz",
    "https://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2025-09-11/data/listings.csv.gz",
    "https://data.insideairbnb.com/germany/be/berlin/2025-09-23/data/listings.csv.gz",

    "https://data.insideairbnb.com/japan/kant%C5%8D/tokyo/2025-09-29/data/listings.csv.gz",
    "https://data.insideairbnb.com/australia/nsw/sydney/2025-03-03/data/listings.csv.gz",
    "https://data.insideairbnb.com/thailand/central-thailand/bangkok/2025-09-26/data/listings.csv.gz",

    "https://data.insideairbnb.com/canada/on/toronto/2025-11-11/data/listings.csv.gz",
    "https://data.insideairbnb.com/mexico/df/mexico-city/2025-09-27/data/listings.csv.gz"
]

URL2 = [
"https://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2025-09-11/data/listings.csv.gz",
"https://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2025-06-09/data/listings.csv.gz",
"https://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2025-03-02/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/tx/austin/2025-09-16/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/tx/austin/2025-06-13/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/tx/austin/2025-03-06/data/listings.csv.gz",
"https://data.insideairbnb.com/thailand/central-thailand/bangkok/2025-09-26/data/listings.csv.gz",
"https://data.insideairbnb.com/thailand/central-thailand/bangkok/2025-06-24/data/listings.csv.gz",
"https://data.insideairbnb.com/thailand/central-thailand/bangkok/2025-03-19/data/listings.csv.gz",
"https://data.insideairbnb.com/thailand/central-thailand/bangkok/2024-12-25/data/listings.csv.gz",
"https://data.insideairbnb.com/spain/catalonia/barcelona/2025-09-14/data/listings.csv.gz",
"https://data.insideairbnb.com/spain/catalonia/barcelona/2025-06-12/data/listings.csv.gz",
"https://data.insideairbnb.com/spain/catalonia/barcelona/2025-03-05/data/listings.csv.gz",
"https://data.insideairbnb.com/germany/be/berlin/2025-09-23/data/listings.csv.gz",
"https://data.insideairbnb.com/germany/be/berlin/2025-06-20/data/listings.csv.gz",
"https://data.insideairbnb.com/germany/be/berlin/2025-03-15/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/il/chicago/2025-09-22/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/il/chicago/2025-06-17/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/il/chicago/2025-03-11/data/listings.csv.gz",
"https://data.insideairbnb.com/united-kingdom/england/london/2025-09-14/data/listings.csv.gz",
"https://data.insideairbnb.com/united-kingdom/england/london/2025-06-10/data/listings.csv.gz",
"https://data.insideairbnb.com/united-kingdom/england/london/2025-03-04/data/listings.csv.gz",
"https://data.insideairbnb.com/france/ile-de-france/paris/2025-09-12/data/listings.csv.gz",
"https://data.insideairbnb.com/france/ile-de-france/paris/2025-06-06/data/listings.csv.gz",
"https://data.insideairbnb.com/france/ile-de-france/paris/2025-03-03/data/listings.csv.gz",
"https://data.insideairbnb.com/australia/nsw/sydney/2025-09-12/data/listings.csv.gz",
"https://data.insideairbnb.com/australia/nsw/sydney/2025-06-10/data/listings.csv.gz",
"https://data.insideairbnb.com/australia/nsw/sydney/2025-03-03/data/listings.csv.gz",
"https://data.insideairbnb.com/japan/kant%C5%8D/tokyo/2025-09-29/data/listings.csv.gz",
"https://data.insideairbnb.com/japan/kant%C5%8D/tokyo/2025-06-27/data/listings.csv.gz",
"https://data.insideairbnb.com/japan/kant%C5%8D/tokyo/2025-03-23/data/listings.csv.gz",
"https://data.insideairbnb.com/japan/kant%C5%8D/tokyo/2024-12-30/data/listings.csv.gz",
"https://data.insideairbnb.com/canada/on/toronto/2025-11-11/data/listings.csv.gz",
"https://data.insideairbnb.com/canada/on/toronto/2025-10-07/data/listings.csv.gz",
"https://data.insideairbnb.com/canada/on/toronto/2025-09-11/data/listings.csv.gz",
"https://data.insideairbnb.com/canada/on/toronto/2025-08-05/data/listings.csv.gz",
"https://data.insideairbnb.com/canada/on/toronto/2025-07-05/data/listings.csv.gz",
"https://data.insideairbnb.com/canada/on/toronto/2025-06-09/data/listings.csv.gz",
"https://data.insideairbnb.com/canada/on/toronto/2025-05-03/data/listings.csv.gz",
"https://data.insideairbnb.com/canada/on/toronto/2025-04-07/data/listings.csv.gz",
"https://data.insideairbnb.com/canada/on/toronto/2025-03-02/data/listings.csv.gz",
"https://data.insideairbnb.com/canada/on/toronto/2025-02-09/data/listings.csv.gz",
"https://data.insideairbnb.com/canada/on/toronto/2025-01-09/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ca/los-angeles/2025-12-04/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ca/los-angeles/2025-09-01/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ca/los-angeles/2025-06-17/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ca/los-angeles/2025-03-01/data/listings.csv.gz",
"https://data.insideairbnb.com/mexico/df/mexico-city/2025-09-27/data/listings.csv.gz",
"https://data.insideairbnb.com/mexico/df/mexico-city/2025-06-25/data/listings.csv.gz",
"https://data.insideairbnb.com/mexico/df/mexico-city/2025-03-19/data/listings.csv.gz",
"https://data.insideairbnb.com/mexico/df/mexico-city/2024-12-27/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ny/new-york-city/2025-12-04/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ny/new-york-city/2025-11-01/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ny/new-york-city/2025-10-01/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ny/new-york-city/2025-09-01/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ny/new-york-city/2025-08-01/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ny/new-york-city/2025-07-01/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ny/new-york-city/2025-06-17/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ny/new-york-city/2025-05-01/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ny/new-york-city/2025-04-01/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ny/new-york-city/2025-03-01/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ny/new-york-city/2025-02-01/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ny/new-york-city/2025-01-03/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ca/san-francisco/2025-12-04/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ca/san-francisco/2025-09-01/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ca/san-francisco/2025-06-17/data/listings.csv.gz",
"https://data.insideairbnb.com/united-states/ca/san-francisco/2025-03-01/data/listings.csv.gz",
]

DEST_DIR = "./data/listings"

def download_files():
    os.makedirs(DEST_DIR, exist_ok=True)
    
    for url in URL2:
        parts = url.split('/')
        date = parts[6]
        city = parts[5] if len(parts) > 5 else "file"
        
        filename = f"{date}_{city}_{os.path.basename(url)}"
        save_path = os.path.join(DEST_DIR, filename)
        
        if os.path.exists(save_path):
            print(f"Skipping {filename}, already exists.")
            continue

        print(f"Downloading {filename}...")
        try:
            with requests.get(url, stream=True, allow_redirects=True) as r:
                r.raise_for_status()
                with open(save_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=16384):
                        f.write(chunk)
            print(f"Downloaded {filename}.")
        except Exception as e:
            print(f"Failed to download {url}: {e}")
            continue


URLS_REVIEW = [
    "https://data.insideairbnb.com/united-states/ny/new-york-city/2025-12-04/data/reviews.csv.gz",
    "https://data.insideairbnb.com/united-states/ca/los-angeles/2025-12-04/data/reviews.csv.gz",
    "https://data.insideairbnb.com/united-states/ca/san-francisco/2025-12-04/data/reviews.csv.gz",
    "https://data.insideairbnb.com/united-states/il/chicago/2025-09-22/data/reviews.csv.gz",
    "https://data.insideairbnb.com/united-states/tx/austin/2025-09-16/data/reviews.csv.gz",

    "https://data.insideairbnb.com/united-kingdom/england/london/2025-09-14/data/reviews.csv.gz",
    "https://data.insideairbnb.com/france/ile-de-france/paris/2025-09-12/data/reviews.csv.gz",
    "https://data.insideairbnb.com/spain/catalonia/barcelona/2025-09-14/data/reviews.csv.gz",
    "https://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/2025-09-11/data/reviews.csv.gz",
    "https://data.insideairbnb.com/germany/be/berlin/2025-09-23/data/reviews.csv.gz",

    "https://data.insideairbnb.com/japan/kant%C5%8D/tokyo/2025-09-29/data/reviews.csv.gz",
    "https://data.insideairbnb.com/australia/nsw/sydney/2025-09-12/data/reviews.csv.gz",
    "https://data.insideairbnb.com/thailand/central-thailand/bangkok/2025-09-26/data/reviews.csv.gz",

    "https://data.insideairbnb.com/canada/on/toronto/2025-11-11/data/reviews.csv.gz",
    "https://data.insideairbnb.com/mexico/df/mexico-city/2025-09-27/data/reviews.csv.gz"
]

def download_reviews():
    os.makedirs("./data/reviews", exist_ok=True)

    for url in URLS_REVIEW:
        parts = url.split('/')
        city = parts[5] if len(parts) > 5 else "file"

        filename = f"{city}_{os.path.basename(url)}"
        save_path = os.path.join("./data/reviews", filename)

        if os.path.exists(save_path):
            print(f"Skipping {filename}, already exists.")
            continue
            
        print(f"Downloading {filename}...")
        try:
            with requests.get(url, stream=True, allow_redirects=True) as r:
                r.raise_for_status()
                with open(save_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=16384):
                        f.write(chunk)
            print(f"Downloaded {filename}.")
        except Exception as e:
            print(f"Failed to download {url}: {e}")
            continue

if __name__ == "__main__":
    download_files()
    download_reviews()
