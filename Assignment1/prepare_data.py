import mysql.connector
import random
import datetime
import string
import hashlib
from faker import Faker
import time

fake = Faker()  # Initialize Faker

conn_params = { # Database connection parameters
    "database": "",  
    "user": "",  
    "password": "",  
    "host": "localhost",
    "port": 3306  
}

# Connect to the database
conn = mysql.connector.connect(**conn_params)
cursor = conn.cursor()

def generate_password_hash(password):
    return hashlib.sha256(password.encode()).hexdigest()

def insert_users(num_users):
    print(f"Inserting {num_users} users...")
    
    your_srn = ""
    your_name = ""
    
    insert_query = """
    INSERT INTO Users 
    (username, email, password, reg_date, is_premium, last_login) 
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    cursor.execute(insert_query, (
        your_name,
        f"{your_srn.lower()}@pesu.pes.edu",
        generate_password_hash("password123"),
        fake.date_time_between(start_date="-2y", end_date="now"),
        random.choice([True, False]),
        fake.date_time_between(start_date="-1m", end_date="now")
    ))
    
    batch_size = 1000       # Batch insert for better performance
    users = []
    
    for i in range(num_users - 1):
        username = fake.user_name()
        email = fake.email()
        password = generate_password_hash(fake.password())
        reg_date = fake.date_time_between(start_date="-2y", end_date="now")
        is_premium = random.choice([True, False])
        last_login = fake.date_time_between(start_date="-1m", end_date="now")
        
        users.append((username, email, password, reg_date, is_premium, last_login))
        
        if len(users) >= batch_size:
            cursor.executemany(insert_query, users)
            conn.commit()
            users = []
    
    # Insert any remaining users
    if users:
        cursor.executemany(insert_query, users)
        conn.commit()
    
    cursor.execute("SELECT MAX(user_id) FROM Users")
    return cursor.fetchone()[0]

def insert_artists(num_artists):
    print(f"Inserting {num_artists} artists...")
    
    insert_query = """
    INSERT INTO Artists 
    (name, country, bio, formed_year) 
    VALUES (%s, %s, %s, %s)
    """
    
    batch_size = 1000
    artists = []
    
    for i in range(num_artists):
        name = fake.name() if random.random() < 0.7 else fake.company()
        country = fake.country()
        bio = fake.text(max_nb_chars=500)
        formed_year = random.randint(1950, 2023)
        
        artists.append((name, country, bio, formed_year))
        
        if len(artists) >= batch_size:
            cursor.executemany(insert_query, artists)
            conn.commit()
            artists = []
    
    # Insert any remaining artists
    if artists:
        cursor.executemany(insert_query, artists)
        conn.commit()
    
    cursor.execute("SELECT MAX(artist_id) FROM Artists")
    return cursor.fetchone()[0]

def insert_albums(num_albums, max_artist_id):
    print(f"Inserting {num_albums} albums...")
    
    insert_query = """
    INSERT INTO Albums 
    (title, artist_id, release_date, genre, total_tracks) 
    VALUES (%s, %s, %s, %s, %s)
    """
    
    genres = ["Rock", "Pop", "Hip Hop", "Jazz", "Classical", "Electronic", "R&B", "Country", 
              "Reggae", "Metal", "Folk", "Blues", "Indie", "Alternative", "Dance", "Latin"]
    
    batch_size = 1000
    albums = []
    
    for i in range(num_albums):
        title = " ".join(fake.words(nb=random.randint(1, 5))).title()
        artist_id = random.randint(1, max_artist_id)
        release_date = fake.date_between(start_date="-70y", end_date="now")
        genre = random.choice(genres)
        total_tracks = random.randint(1, 20)
        
        albums.append((title, artist_id, release_date, genre, total_tracks))
        
        if len(albums) >= batch_size:
            cursor.executemany(insert_query, albums)
            conn.commit()
            albums = []
    
    # Insert any remaining albums
    if albums:
        cursor.executemany(insert_query, albums)
        conn.commit()
    
    cursor.execute("SELECT MAX(album_id) FROM Albums")
    return cursor.fetchone()[0]

def insert_songs(num_songs, max_album_id, max_artist_id):
    print(f"Inserting {num_songs} songs...")
    
    insert_query = """
    INSERT INTO Songs 
    (title, album_id, artist_id, duration, track_number, genre, plays, release_date) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    genres = ["Rock", "Pop", "Hip Hop", "Jazz", "Classical", "Electronic", "R&B", "Country", 
              "Reggae", "Metal", "Folk", "Blues", "Indie", "Alternative", "Dance", "Latin"]
    
    batch_size = 1000
    songs = []
    
    for i in range(num_songs):
        title = " ".join(fake.words(nb=random.randint(1, 6))).title()
        album_id = random.randint(1, max_album_id)
        artist_id = random.randint(1, max_artist_id)
        duration = random.randint(30, 600)  # 30 seconds to 10 minutes
        track_number = random.randint(1, 20)
        genre = random.choice(genres)
        plays = random.randint(0, 1000000)
        release_date = fake.date_between(start_date="-70y", end_date="now")
        
        songs.append((title, album_id, artist_id, duration, track_number, genre, plays, release_date))
        
        if len(songs) >= batch_size:
            cursor.executemany(insert_query, songs)
            conn.commit()
            songs = []
    
    # Insert any remaining songs
    if songs:
        cursor.executemany(insert_query, songs)
        conn.commit()
    
    cursor.execute("SELECT MAX(song_id) FROM Songs")
    return cursor.fetchone()[0]

def insert_playlists(num_playlists, max_user_id):
    print(f"Inserting {num_playlists} playlists...")
    
    insert_query = """
    INSERT INTO Playlists 
    (user_id, name, description, created_date, is_public, last_updated) 
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    batch_size = 1000
    playlists = []
    
    for i in range(num_playlists):
        user_id = random.randint(1, max_user_id)
        name = " ".join(fake.words(nb=random.randint(1, 4))).title()
        description = fake.text(max_nb_chars=200) if random.random() > 0.3 else None
        created_date = fake.date_time_between(start_date="-2y", end_date="now")
        is_public = random.random() > 0.2  # 80% are public
        last_updated = fake.date_time_between(start_date=created_date, end_date="now")
        
        playlists.append((user_id, name, description, created_date, is_public, last_updated))
        
        if len(playlists) >= batch_size:
            cursor.executemany(insert_query, playlists)
            conn.commit()
            playlists = []
    
    # Insert any remaining playlists
    if playlists:
        cursor.executemany(insert_query, playlists)
        conn.commit()
    
    cursor.execute("SELECT MAX(playlist_id) FROM Playlists")
    return cursor.fetchone()[0]

def insert_playlist_songs(num_playlist_songs, max_playlist_id, max_song_id, max_user_id):
    print(f"Inserting {num_playlist_songs} playlist-song associations...")
    
    insert_query = """
    INSERT INTO PlaylistSongs 
    (playlist_id, song_id, added_date, added_by) 
    VALUES (%s, %s, %s, %s)
    """
    
    # To avoid duplicates, we'll use a set to track combinations we've already inserted
    already_inserted = set()
    
    batch_size = 1000
    playlist_songs = []
    
    inserted_count = 0
    attempts = 0
    max_attempts = num_playlist_songs * 2  # Limit attempts to avoid infinite loop
    
    while inserted_count < num_playlist_songs and attempts < max_attempts:
        attempts += 1
        playlist_id = random.randint(1, max_playlist_id)
        song_id = random.randint(1, max_song_id)
        
        # Skip if this combination already exists
        if (playlist_id, song_id) in already_inserted:
            continue
        
        already_inserted.add((playlist_id, song_id))
        
        added_date = fake.date_time_between(start_date="-2y", end_date="now")
        added_by = random.randint(1, max_user_id)
        
        playlist_songs.append((playlist_id, song_id, added_date, added_by))
        inserted_count += 1
        
        if len(playlist_songs) >= batch_size:
            try:
                cursor.executemany(insert_query, playlist_songs)
                conn.commit()
            except mysql.connector.Error as err:
                print(f"Error inserting playlist songs batch: {err}")
                # Try inserting one by one for problematic batches
                for entry in playlist_songs:
                    try:
                        cursor.execute(insert_query, entry)
                        conn.commit()
                    except mysql.connector.Error as inner_err:
                        print(f"Error inserting individual entry: {inner_err}")
                        conn.rollback()
            playlist_songs = []
    
    # Insert any remaining playlist songs
    if playlist_songs:
        try:
            cursor.executemany(insert_query, playlist_songs)
            conn.commit()
        except mysql.connector.Error as err:
            print(f"Error inserting final playlist songs batch: {err}")
            # Try inserting one by one
            for entry in playlist_songs:
                try:
                    cursor.execute(insert_query, entry)
                    conn.commit()
                except mysql.connector.Error as inner_err:
                    print(f"Error inserting individual entry: {inner_err}")
                    conn.rollback()
    
    print(f"Successfully inserted {inserted_count} playlist-song associations after {attempts} attempts")

def main():
    start_time = time.time()
    
    try:
        num_users = 1000
        num_artists = 500
        num_albums = 2000
        num_songs = 12000  # One of our large tables (>10,000 rows)
        num_playlists = 3000
        num_playlist_songs = 15000  # Another large table (>10,000 rows)
        
        # Insert data into each table
        max_user_id = insert_users(num_users)
        print(f"Users inserted. Max user_id: {max_user_id}")
        
        max_artist_id = insert_artists(num_artists)
        print(f"Artists inserted. Max artist_id: {max_artist_id}")
        
        max_album_id = insert_albums(num_albums, max_artist_id)
        print(f"Albums inserted. Max album_id: {max_album_id}")
        
        max_song_id = insert_songs(num_songs, max_album_id, max_artist_id)
        print(f"Songs inserted. Max song_id: {max_song_id}")
        
        max_playlist_id = insert_playlists(num_playlists, max_user_id)
        print(f"Playlists inserted. Max playlist_id: {max_playlist_id}")
        
        insert_playlist_songs(num_playlist_songs, max_playlist_id, max_song_id, max_user_id)
        print("Playlist songs inserted.")
        
        end_time = time.time()
        print(f"Data generation completed in {(end_time - start_time):.2f} seconds")
        
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    except Exception as err:
        print(f"Error: {err}")
    finally:
        # Close the database connection
        cursor.close()
        conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()
