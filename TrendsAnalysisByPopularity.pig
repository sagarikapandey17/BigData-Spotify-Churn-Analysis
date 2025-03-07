-- Load the dataset from HDFS
songs = LOAD 'hdfs://localhost:9000/spotify_data/synthetic_songs.csv' USING PigStorage(',') 
    AS (song_id:chararray, title:chararray, artist:chararray, album:chararray, genre:chararray, 
        duration:int, popularity:int, tempo:double, danceability:double, energy:double);

-- 1. Analyze the relationship between popularity and energy/tempo/danceability
popularity_trends = FOREACH songs GENERATE title, genre, popularity, energy, tempo, danceability;
sorted_by_popularity = ORDER popularity_trends BY popularity DESC;

-- 2. Track popularity by genre to find trends over time
popularity_by_genre = GROUP songs BY genre;
avg_popularity_per_genre = FOREACH popularity_by_genre GENERATE group AS genre, AVG(songs.popularity) AS avg_popularity;
sorted_popularity_genre = ORDER avg_popularity_per_genre BY avg_popularity DESC;

-- 3. Filter songs that are energetic and have high danceability (likely for fitness/playlists)
high_energy_danceable_songs = FILTER songs BY energy > 0.75 AND danceability > 0.75;

-- 4. Songs with increasing popularity based on tempo and energy trends
songs_with_increasing_popularity = FILTER songs BY popularity > 50 AND energy > 0.7 AND tempo > 120;

-- Store the results in HDFS
STORE sorted_by_popularity INTO 'hdfs://localhost:9000/spotify_data/output/popularity_trends' USING PigStorage(',');
STORE sorted_popularity_genre INTO 'hdfs://localhost:9000/spotify_data/output/popularity_by_genre' USING PigStorage(',');
STORE high_energy_danceable_songs INTO 'hdfs://localhost:9000/spotify_data/output/high_energy_danceable_songs' USING PigStorage(',');
STORE songs_with_increasing_popularity INTO 'hdfs://localhost:9000/spotify_data/output/songs_with_increasing_popularity' USING PigStorage(',');
