-- Load the dataset from HDFS
songs = LOAD 'hdfs://localhost:9000/spotify_data/synthetic_songs.csv' USING PigStorage(',') 
    AS (song_id:int, title:chararray, artist:chararray, album:chararray, genre:chararray, 
        duration:int, popularity:int, tempo:double, danceability:double, energy:double);

-- Classify songs into playlists based on tempo, energy, and danceability audio faetures
playlist_classification = FOREACH songs GENERATE song_id, title, artist, album, genre, 
    (tempo > 100 ? 'Workout' : 
        (danceability > 0.5 ? 'Party' : 
            (tempo < 100 ? 'Relax' : 'Other'))) AS playlist;

-- Separate the songs into different playlists
workout_songs = FILTER playlist_classification BY playlist == 'Workout';
party_songs = FILTER playlist_classification BY playlist == 'Party';
relax_songs = FILTER playlist_classification BY playlist == 'Relax';
other_songs = FILTER playlist_classification BY playlist == 'Other';

-- Store the classified songs into separate directories for each playlist
STORE workout_songs INTO 'hdfs://localhost:9000/spotify_data/output/workout_playlist' USING PigStorage(',');
STORE party_songs INTO 'hdfs://localhost:9000/spotify_data/output/party_playlist' USING PigStorage(',');
STORE relax_songs INTO 'hdfs://localhost:9000/spotify_data/output/relax_playlist' USING PigStorage(',');
STORE other_songs INTO 'hdfs://localhost:9000/spotify_data/output/other_playlist' USING PigStorage(',');
