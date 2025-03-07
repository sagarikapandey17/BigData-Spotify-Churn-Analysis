package com.mycompany.highvalueusers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class HighValueUserGenreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Set<String> processedInteractions = new HashSet<>();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String userId = fields[1]; // User ID
        String genre = fields[9]; // Genre

        // Create a unique interaction key (User-Genre combination)
        String interactionKey = userId + "-" + genre;

        // Only process if the interaction hasn't been seen before
        if (!processedInteractions.contains(interactionKey)) {
            processedInteractions.add(interactionKey);
            context.write(new Text(userId), new IntWritable(1)); // Emit user ID with count of 1 for each genre
        }
    }
}
