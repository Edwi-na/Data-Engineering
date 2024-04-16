import os
import glob
import psycopg2
import pandas as pd
import json
import numpy as np
from sql_queries import *

# Database connection
conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor()

# Retrieve all JSON files from a directory
def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    
    return all_files

#-----processing song data
# Use the `get_files` function to get a list of all song JSON files in `data/song_data`
song_files = get_files('data/song_data')

# Read each song file and append its data to song_list
song_list = []
for file in song_files:
    with open(file, 'r') as f:
        data = json.load(f)
        song_list.append(data)

# Convert song_list to a DataFrame
df = pd.DataFrame(song_list)

#1 - generating records into table song
# Select columns for the songs table: song ID, title, artist ID, year, and duration
songs_clean = df[['song_id', 'title', 'artist_id', 'year', 'duration']]

# Print the initial shape of the songs data
print("Initial shape of songs data:", songs_clean.shape)

# Drop rows with missing values
songs_clean = songs_clean.dropna()

# Print the shape of the cleaned songs data
print("Shape of cleaned songs data after dropping NA values:", songs_clean.shape)

# Convert the cleaned songs data to a list of lists
songs_data = songs_clean.values.tolist()

# Insert song records into the database
for song in songs_data:
    cur.execute(song_table_insert, song)
    conn.commit()



#2 - generating records into table artist
# Select columns for the songs table: artist ID, name, location, latitude, and longitude
artist_clean = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_latitude']]

# Print the initial shape of the songs data
print("Initial shape of songs data:", artist_clean.shape)

# Print the initial shape of the artist data
print("Initial shape of artist data:", artist_clean.shape)

# Replace blank values with NaN in selected columns
artist_clean[['artist_id', 'artist_name', 'artist_location']] = artist_clean[['artist_id', 'artist_name', 'artist_location']].replace('', np.nan)

# Drop rows where artist_id, artist_name, or artist_location is NaN
artist_clean = artist_clean.dropna(subset=['artist_id', 'artist_name', 'artist_location'])

# Print the shape of the cleaned artist data
print("Shape of cleaned artist data after dropping NA values:", artist_clean.shape)

# Convert cleaned artist data to a list of lists
artist_data = artist_clean.values.tolist()

# Insert artists record into the database
for artist in artist_data:
    cur.execute(artist_table_insert, artist)
    conn.commit()





#-----processing log data
#Use the `get_files` function provided above to get a list of all log JSON files in `data/log_data`
log_files = get_files('data/log_data')

# Read the log file line by line and process each line as a separate JSON object
log_data_list = []
for file in log_files:
    with open(file, 'r') as f:
        for line in f:
            data = json.loads(line)
            log_data_list.append(data)

# Create a DataFrame from the processed log data
log_df = pd.DataFrame(log_data_list)

#3 - generating records into table time
# Filter records by NextSong action
log_df_filter = log_df.loc[log_df["page"] == "NextSong"] 

#Convert the ts timestamp column to datetime
log_df_filter["ts"] = pd.to_datetime(log_df_filter['ts'], unit='ms') 

# Extract the required time-related data from the 'ts' column
time_data = [
    log_df_filter["ts"],                       
    log_df_filter["ts"].dt.hour,               
    log_df_filter["ts"].dt.day,                
    log_df_filter["ts"].dt.isocalendar().week, 
    log_df_filter["ts"].dt.month,              
    log_df_filter["ts"].dt.year,               
    log_df_filter["ts"].dt.weekday             
]

# Define the column labels
column_labels = ["start_time", "hour", "day", "week", "month", "year", "weekday"]

# Create the time_df DataFrame
time_df = pd.concat(time_data, axis=1)
time_df.columns = column_labels

# Convert time_df to list of lists
time_df = time_df.values.tolist()

# Insert artists records into the database
for time in time_df:
    cur.execute(time_table_insert, time)
    conn.commit()



#4 - generating records into table users
# Select columns for the users table: user ID, first name, last name, gender, and level
user_d = log_df[["userId","firstName", "lastName", "gender", "level"]]
# Drop rows with missing value
print("Before dropping missing values:", user_d.shape)
user_d = user_d.dropna()
print("After dropping missing values:", user_d.shape)


# Drop duplicates to avoid inserting duplicate user records
user_df = user_d.drop_duplicates().values.tolist()  

# Insert artists records into the database
for user in user_df:
    cur.execute(user_table_insert, user)
    conn.commit()



#5 - generating records into table songplays 
# Extract necessary columns from log_df
log_df['ts'] = pd.to_datetime(log_df['ts'], unit='ms')  # Convert timestamp to datetime format

# Drop rows with missing values
a = log_df.dropna()

# Initialize empty list to store songplay data
songplay_data = []

# Iterate through each row in a
for index, row in a.iterrows():
    # Get song ID and artist ID using song_select query
    params = (row.song, row.artist, row.length)
    #print(f"Parameters: {params}")  # Debugging line
    cur.execute(song_select, params)
    results = cur.fetchone()
    #print(f"Query Result: {results}")  # Debugging line

    if results:
        song_id, artist_id = results
        print(results)
    else:
        song_id, artist_id = None, None

    # Append songplay data to songplay_data list
    songplay_data.append([
        row.ts,         # timestamp
        row.userId,     # user ID
        row.level,      # level
        song_id,        # song ID
        artist_id,      # artist ID
        row.sessionId,  # session ID
        row.location,   # location
        row.userAgent   # user agent
    ])

# Insert songplays records into the database
for data in songplay_data:
    cur.execute(songplay_table_insert, data)
    conn.commit()  # Commit the transaction

# Close the cursor and database connection
cur.close()
conn.close()

print("Cursor and database connection closed.")
