import os
import requests
import s3fs
import pandas as pd
from dotenv import load_dotenv

# Define the URL
playlist_url = "https://api.spotify.com/v1/playlists/2UmSkliK7pZzohnDjtbo9k?si=371214b33a4b4877"

load_dotenv()

client_id = os.getenv("client_id") or ""
client_secret = os.getenv("client_secret") or ""

def get_token(_client_id: str,
              _client_secret: str
              ):
    # Define the URL
    url = "https://accounts.spotify.com/api/token"

    # Define the payload data
    data = {
        "grant_type": "client_credentials",
        "client_id": _client_id,
        "client_secret": _client_secret,
    }

    # Define the headers
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    # Send the POST request
    response = requests.post(url, data=data, headers=headers)

    # Check the response
    if response.status_code == 200:
        # The request was successful, and the access token is in JSON format
        access_token = response.json().get("access_token")
        print("Access Token:", access_token)
    else:
        print("POST request failed with status code:", response.status_code)
    return access_token


def get_playlist_data(_access_token: str,
                      _playlist_url: str
                      ):
    # Define the authorization header with your Bearer token
    headers = {
        "Authorization": "Bearer " + _access_token  # Replace with your actual token
    }

    # Send the GET request
    response = requests.get(_playlist_url, headers=headers)

    # Check the response
    if response.status_code == 200:
        # The request was successful, and the response content is in JSON format
        playlist_data = response.json()
    else:
        print("GET request failed with status code:", response.status_code)
        print("Error Message: ", response.json()['error']['message'])
    return playlist_data

#get data for a page
def collect_data_from_page(_track_items: list,
                           _songs_dict: dict
                           ):
    for i in range(len(_track_items)):
        added_time = _track_items[i]['added_at']
        artist = _track_items[i]['track']['artists'][0]['name']
        track_name = _track_items[i]['track']['name']
        popularity = _track_items[i]['track']['popularity']
        _songs_dict['added_at'].append(added_time)
        _songs_dict['artist'].append(artist)
        _songs_dict['track_name'].append(track_name)
        _songs_dict['popularity'].append(popularity)

#Check next page data
def check_next_page_data(_next_page_url: str,
                         _access_token: str,
                         _songs_dict: dict
                                ):
    while _next_page_url:
        next_page_data = get_playlist_data(_access_token, _next_page_url)
        track_items = next_page_data.get("items")
        collect_data_from_page(track_items, _songs_dict)
        _next_page_url = next_page_data.get("next")

def run_spotify_etl():
    access_token = get_token(client_id, client_secret)

    playlist_data = get_playlist_data(access_token, playlist_url)

    playlist_name = playlist_data.get("name")
    playlist_owner = playlist_data.get("owner")['display_name']
    next_page_url = playlist_data.get("tracks").get("next")
    track_items = playlist_data.get("tracks").get("items")
    print(type(track_items))
    songs = {
        "added_at": [],
        "artist": [],
        "track_name": [],
        "popularity": []
    }

    collect_data_from_page(track_items, songs)

    check_next_page_data(next_page_url, access_token, songs)

    songs_df = pd.DataFrame.from_dict(songs)
    songs_df['playlist_name'] = playlist_name
    songs_df['playlist_owner'] = playlist_owner
    songs_df.to_csv("s3://henry-airflow/songs.csv")









