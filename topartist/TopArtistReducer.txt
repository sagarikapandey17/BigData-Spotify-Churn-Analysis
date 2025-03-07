package com.mycompany.topartist;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopArtistReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable totalCount = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int playCount = 0;
        int skipCount = 0;

        // Aggregate the counts for play and skip actions
        for (IntWritable value : values) {
            if (key.toString().contains("skip")) {
                skipCount += value.get();  // Count skips
            } else {
                playCount += value.get();  // Count plays
            }
        }

        // Compute the composite ranking score: (e.g., Play Count - Skip Count)
        int rankScore = playCount - skipCount;

      //apply weights to the metrics
        int w_play = 2; // Play weight
        int w_skip = 1; // Skip weight
        rankScore = (w_play * playCount) - (w_skip * skipCount);

        totalCount.set(rankScore); // Set the rank score as the output value
        context.write(key, totalCount); // Output the artist with their rank score
    }
}
