-- Load the data
user_data = LOAD 'hdfs://localhost:9000/spotify_data/synthetic_users_interactions.csv' 
    USING PigStorage(',')
    AS (
        interaction_id:chararray, 
        user_id:chararray, 
        name:chararray, 
        country:chararray, 
        age:int, 
        subscription:chararray, 
        song_id:int, 
        title:chararray, 
        artist:chararray, 
        genre:chararray, 
        action:chararray, 
        device:chararray, 
        timestamp:chararray
    );

-- Group data by subscription type
grouped_data = GROUP user_data BY subscription;

-- Calculate the average age for each subscription type
avg_age_data = FOREACH grouped_data GENERATE 
    group AS subscription, 
    AVG(user_data.age) AS avg_age;

-- Store the result in HDFS
STORE avg_age_data INTO 'hdfs://localhost:9000/spotify_data/output/avg_age_by_subscription' USING PigStorage(',');
