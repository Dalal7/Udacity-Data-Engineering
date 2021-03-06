import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """ 
    Read song data from the given song file and insert the records into songs and artists tables 
  
    Parameters: 
    cur (psycopg2.cursor): psycopg2 cursor connected to the database
    filepath (str): path of the song file
    
    """
    
    df = pd.read_json(filepath, lines=True)

    song_data = df[['song_id', 'title', 'artist_id', 'year','duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    artist_data = df[['artist_id', 'artist_name', 'artist_location','artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ 
    Read log data from the given log file and insert the records into time, users and songplays tables 
  
    Parameters: 
    cur (psycopg2.cursor): psycopg2 cursor connected to the database
    filepath (str): path of the log file
    
    """
    
    df = pd.read_json(filepath, lines=True)

    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    ts = df['ts']
    t = pd.to_datetime(ts,unit='ms')
    
    time_data = [(dt, dt.hour, dt.day, dt.week, dt.month, dt.year, dt.weekday()) for dt in t]
    column_labels = ('ts', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates(subset=['userId'])

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = [pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ 
    Read all the files in a given filepath of a directory and execute functions to know the number of files has processed
  
    Parameters: 
    cur (psycopg2.cursor): psycopg2 cursor connected to the database
    conn (psycopg2.connection): the database connection
    filepath (str): path of the directory
    func (function): function to be executed, can be process_song_data function or process_log_data function
    
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """ 
   `Create a conncetion to the Sparkify database, call process data to procese the song and log data and close the database resources.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()