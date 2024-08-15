import pandas as pd
import requests
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


# Load the dataset
movie_df = pd.read_csv('movie_fact_table.csv')


def requests_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504), session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_movie_plot(api_key, movie_title, delay=0.25, retries=3):
    base_url = "https://api.themoviedb.org/3"
    search_url = f"{base_url}/search/movie"

    params = {
        'api_key': api_key,
        'query': movie_title
    }

    for attempt in range(retries):
        try:
            response = requests_retry_session().get(search_url, params=params, timeout=10)
            time.sleep(delay)

            if response.status_code != 200:
                return f"Error: {response.status_code}, message: {response.json().get('status_message', 'No message')}"

            data = response.json()

            if data['results']:
                movie = data['results'][0]
                movie_id = movie['id']

                details_url = f"{base_url}/movie/{movie_id}"
                details_params = {'api_key': api_key}
                details_response = requests_retry_session().get(details_url, params=details_params, timeout=10)
                time.sleep(delay)

                if details_response.status_code != 200:
                    return f"Error: {details_response.status_code}, message: {details_response.json().get('status_message', 'No message')}"

                details_data = details_response.json()
                return details_data.get('overview', 'Plot not found')
            else:
                return 'Movie not found'
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(delay * (attempt + 1))
    
    return "Failed to retrieve data after multiple attempts"

def fetch_and_update_plot(row):
    plot = get_movie_plot(api_key, row['movie_name'])
    return plot

# Apply the fetch_and_update_plot function to each row and update the plot column
movie_df['plot'] = movie_df.apply(fetch_and_update_plot, axis=1)

# Save the updated dataframe to a new CSV file
movie_df.to_csv('movie_fact_table_with_plots.csv', index=False)
print(f"Total movies updated with plots: {len(movie_df)}")
