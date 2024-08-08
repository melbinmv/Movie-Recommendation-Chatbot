import pandas as pd
import requests
import time

# Load the movie data
movie_df = pd.read_csv('movie_fact_table.csv')
api_key = "#"

def get_movie_plot(api_key, movie_title, delay=0.25):
    base_url = "https://api.themoviedb.org/3"
    search_url = f"{base_url}/search/movie"

    params = {
        'api_key': api_key,
        'query': movie_title
    }

    response = requests.get(search_url, params=params)
    time.sleep(delay)

    if response.status_code != 200:
        return f"Error: {response.status_code}, message: {response.json().get('status_message', 'No message')}"

    data = response.json()

    if data['results']:
        movie = data['results'][0]
        movie_id = movie['id']

        details_url = f"{base_url}/movie/{movie_id}"
        details_params = {'api_key': api_key}
        details_response = requests.get(details_url, params=details_params)
        time.sleep(delay)

        if details_response.status_code != 200:
            return f"Error: {details_response.status_code}, message: {details_response.json().get('status_message', 'No message')}"

        details_data = details_response.json()
        return details_data.get('overview', 'Plot not found')
    else:
        return 'Movie not found'

updated_count = 0

for idx, title in enumerate(movie_df['movie_name']):
    plot = get_movie_plot(api_key, title)
    movie_df.loc[movie_df['movie_name'] == title, 'plot'] = plot
    updated_count += 1
    print(f"Updated {updated_count}/{len(movie_df)}: {title}")

# Save the updated dataframe to a new CSV file
movie_df.to_csv('movie_fact_table_with_plots.csv', index=False)
print(f"Total movies updated with plots: {updated_count}")