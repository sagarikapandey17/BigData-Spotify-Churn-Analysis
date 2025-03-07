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

-- Group data by subscription type and device
grouped_devices = GROUP user_data BY (subscription, device);

-- Count the number of interactions for each device within each subscription type
device_count = FOREACH grouped_devices GENERATE 
    group.subscription AS subscription, 
    group.device AS device, 
    COUNT(user_data) AS interaction_count;

-- Find the most common device for each subscription type
most_common_device = FOREACH (GROUP device_count BY subscription) {
    top_device = LIMIT device_count 1;
    GENERATE FLATTEN(top_device);
}

-- Store the result in HDFS
STORE most_common_device INTO 'hdfs://localhost:9000/spotify_data/output/common_device_by_subscription' USING PigStorage(',');
