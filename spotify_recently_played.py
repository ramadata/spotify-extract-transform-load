import os
from dotenv import load_dotenv
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import sqlalchemy
import sqlite3


load_dotenv()

client_id = os.getenv("SPOTIPY_CLIENT_ID")
client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")
redirect_uri = os.getenv("REDIRECT_URI")
scope = 'user-library-read user-read-recently-played'
DATABASE_LOCATION = "sqlite:///my_recently_played_tracks.sqlite"

auth_manager = SpotifyOAuth(
    client_id=client_id,
    client_secret=client_secret,
    redirect_uri=redirect_uri,
    scope=scope
)

sp = Spotify(auth_manager=auth_manager)

def get_recently_played_tracks():
    print("Accessing Spotify to get recently played tracks...")
    recently_played = sp.current_user_recently_played(limit=50) # when coding change limit to 1 to see output
    # print(recently_played) # use this line to retrieve keys and the see the layout of the json
    return recently_played

def json_to_df(data):
    print("Data retrieved...")
    df = pd.json_normalize(data, "items")
    # print(df.columns) # pull the columns that you will use to construct your dataframe
    return df

def transform_df(data: pd.DataFrame):
    transformed = data.copy()
    transformed = transformed[['track.id', 'track.album.release_date', 'played_at', 'context', 'track.duration_ms', 'track.artists','track.name', 'track.album.name']]
    transformed.columns = ['track_id', 'track_album_release_date', 'played_at', 'context', 'track_duration_ms', 'track_artists','track_name', 'track_album_name']
    transformed['track_artists'] = transformed['track_artists'].apply(lambda x: x[0]['name'])
    # print(transformed['track_artists']) # check your output
    # print(transformed['context']) # check if the column is indeed null 
    return transformed

def validate(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No songs downloaded...")
        return False
    else:
        print("Downloaded songs. Now beginning transform process...")
        return df

def load_df(df):
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_recently_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_recently_played_tracks(
        track_id VARCHAR(200),
        track_album_release_date VARCHAR(200),
        played_at VARCHAR(200),
        context VARCHAR(200),
        track_duration_ms VARCHAR(200),
        track_artists VARCHAR(200), 
        track_name VARCHAR(200), 
        track_album_name VARCHAR(200),
     
           CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    try:
        cursor.execute(sql_query)
        print("Database opened sucessfully...")
    except Exception as error:
        print("Could not open database...", error)
    

    try:
        df.to_sql('my_recently_played_tracks', engine, index=False, if_exists='append')
    except sqlite3.IntegrityError as error:
        print("These tracks already exist in the database. Nothing new to update... ", error)
    except Exception as error:
        print("Problem inserting data into database...", error)

    conn.close()
    print("Database closed successfully...")

if __name__ == '__main__':
    recent_tracks = get_recently_played_tracks()
    df = json_to_df(recent_tracks)
    validated = validate(df)
    transformed = transform_df(validated)
    load_df(transformed)

