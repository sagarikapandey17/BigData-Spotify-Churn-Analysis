package Reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class SongReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Use a HashMap to count the frequency of each song for the given country
        Map<String, Integer> songFrequencyMap = new HashMap<>();
        
        // Count the frequency of each song for the  country
        for (Text song : values) {
            String songId = song.toString();
            songFrequencyMap.put(songId, songFrequencyMap.getOrDefault(songId, 0) + 1);
        }

        // Sort the songs by frequency in descending order 
        List<Map.Entry<String, Integer>> songList = new ArrayList<>(songFrequencyMap.entrySet());
        songList.sort((a, b) -> b.getValue().compareTo(a.getValue())); // Sorting in descending order

        // Prepare a list to store the top K songs
        StringBuilder topKSongs = new StringBuilder();
        int topK = 10;  // For example, Top 10 songs

        // Iterate through the sorted list and append the top K songs to the result
        int count = 0;
        for (Map.Entry<String, Integer> entry : songList) {
            if (count >= topK) {
                break;
            }
            String song = entry.getKey();
            topKSongs.append(song).append(", ");
            count++;
        }

        // Remove the trailing comma and space
        if (topKSongs.length() > 0) {
            topKSongs.setLength(topKSongs.length() - 2);
        }

        result.set(topKSongs.toString());
        context.write(key, result); // Write country and its top K songs
    }
}
