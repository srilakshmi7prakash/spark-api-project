import requests

def fetch_api(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() 
        return response.json()
    except Exception as e:
        print(f"Error fetching API: {e}")
        return None  # Or return []